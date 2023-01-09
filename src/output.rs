use crate::error::{Error, ErrorKind};
use crate::util::send_error_to_replier;
use cooplan_lapin_wrapper::config::api_consumer::ApiConsumer;
use futures_util::StreamExt;
use lapin::Channel;
use serde::Deserialize;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::oneshot::Sender;
use tokio::time::{timeout, Instant};

/// Utility for reading values from a RabbitMQ stream.
///
/// # Returns
/// `Ok` if the read value has been sent correctly through the replier.
/// `Err` if an error occurred during the reading process.
/// The replier is also informed of the error.
pub async fn read_from_stream<ResponseType: for<'de> Deserialize<'de>>(
    api_consumer: Arc<ApiConsumer>,
    output_id: &str,
    channel: Arc<Channel>,
    replier: Sender<Result<ResponseType, Error>>,
    timeout_after_seconds: u64,
) -> Result<(), Error> {
    let output = match api_consumer
        .output()
        .iter()
        .find(|element| element.id() == output_id)
    {
        Some(output) => output,
        None => {
            return send_error_to_replier(
                replier,
                Error::new(
                    ErrorKind::AutoConfigFailure,
                    format!("missing output api consumer with id: '{}'", output_id),
                ),
            )
        }
    };

    let qos = output.qos();
    match channel
        .basic_qos(qos.prefetch_count(), *qos.options())
        .await
    {
        Ok(_) => (),
        Err(error) => {
            return send_error_to_replier(
                replier,
                Error::new(
                    ErrorKind::AmqpFailure,
                    format!("failed to set qos: {}", error),
                ),
            )
        }
    }

    let consume = output.consume();
    let mut consumer = match channel
        .basic_consume(
            output.queue_name(),
            "",
            *consume.options(),
            consume.arguments().clone(),
        )
        .await
    {
        Ok(consumer) => consumer,
        Err(error) => {
            return send_error_to_replier(
                replier,
                Error::new(
                    ErrorKind::AmqpFailure,
                    format!("failed to consume: {}", error),
                ),
            )
        }
    };

    let ack_options = output.acknowledge();

    let start = Instant::now();
    let mut start_elapsed_seconds = 0u64;

    loop {
        start_elapsed_seconds = start.elapsed().as_secs();
        if start_elapsed_seconds >= timeout_after_seconds {
            return send_error_to_replier(
                replier,
                Error::new(ErrorKind::ApiFailure, "timed out waiting for response"),
            );
        }

        let delivery = match timeout(
            Duration::from_secs(timeout_after_seconds - start_elapsed_seconds),
            consumer.next(),
        )
        .await
        {
            Ok(result) => match result {
                Some(result) => match result {
                    Ok(delivery) => delivery,
                    Err(error) => {
                        return send_error_to_replier(
                            replier,
                            Error::new(
                                ErrorKind::AmqpFailure,
                                format!("failed to get delivery: {}", error),
                            ),
                        )
                    }
                },
                None => continue,
            },
            Err(error) => {
                return send_error_to_replier(
                    replier,
                    Error::new(
                        ErrorKind::AmqpFailure,
                        format!("response timed out: {}", error),
                    ),
                )
            }
        };

        match delivery.ack(*ack_options).await {
            Ok(_) => (),
            Err(error) => log::warn!("failed to acknowledge reply from api: {}", error),
        }

        let response = match serde_json::from_slice::<ResponseType>(&delivery.data) {
            Ok(response) => response,
            Err(error) => {
                return send_error_to_replier(
                    replier,
                    Error::new(
                        ErrorKind::ApiFailure,
                        format!("failed to deserialize response: {}", error),
                    ),
                );
            }
        };

        match replier.send(Ok(response)) {
            Ok(_) => (),
            Err(_) => {
                return Err(Error::new(
                    ErrorKind::InternalFailure,
                    "failed to send response",
                ))
            }
        }

        break;
    }

    Ok(())
}
