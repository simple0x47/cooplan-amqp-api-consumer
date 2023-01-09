use crate::error::{Error, ErrorKind};
use crate::util::send_error_to_replier;
use cooplan_amqp_api_shared::api::input::request_result::RequestResult;
use cooplan_lapin_wrapper::config::api_consumer::ApiConsumer;
use futures_util::StreamExt;
use lapin::options::BasicAckOptions;
use lapin::types::ShortString;
use lapin::Channel;
use serde::Deserialize;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::oneshot::Sender;
use tokio::time::{timeout, Instant};

/// Utility for sending RPC like requests to an AMQP Api and, afterwards, waiting for the response.
///
/// # Returns
/// * `Ok` if the request was successfully sent and the response was send to the replier.
/// * `Err` if an error occurred while sending the request or while waiting for the response.
/// The replier is also informed of the error.
pub async fn send_request_and_wait_for_response<ResponseType: for<'de> Deserialize<'de>>(
    api_consumer: Arc<ApiConsumer>,
    input_id: &str,
    channel: Arc<Channel>,
    request: Vec<u8>,
    replier: Sender<Result<ResponseType, Error>>,
    response_timeout_in_seconds: u64,
) -> Result<(), Error> {
    let input = match api_consumer
        .input()
        .iter()
        .find(|element| element.id() == input_id)
    {
        Some(input) => input,
        None => {
            return send_error_to_replier(
                replier,
                Error::new(
                    ErrorKind::AutoConfigFailure,
                    format!("missing input api consumer with id: '{}'", input_id),
                ),
            );
        }
    };

    let response_queue_config = input.response().queue();
    let response_queue = match channel
        .queue_declare(
            response_queue_config.name(),
            *response_queue_config.declare().options(),
            response_queue_config.declare().arguments().clone(),
        )
        .await
    {
        Ok(queue) => queue,
        Err(error) => {
            return send_error_to_replier(
                replier,
                Error::new(
                    ErrorKind::AmqpFailure,
                    format!("failed to declare response queue: {}", error),
                ),
            );
        }
    };

    let publish = input.request().publish();
    let correlation_id = ShortString::from(uuid::Uuid::new_v4().to_string());

    let properties = publish
        .properties()
        .clone()
        .with_reply_to(response_queue.name().clone())
        .with_correlation_id(correlation_id.clone());

    match channel
        .basic_publish(
            publish.exchange(),
            input.request().queue_name(),
            *publish.options(),
            &request,
            properties,
        )
        .await
    {
        Ok(_) => (),
        Err(error) => {
            return send_error_to_replier(
                replier,
                Error::new(
                    ErrorKind::AmqpFailure,
                    format!("failed to publish request: {}", error),
                ),
            )
        }
    }

    let qos = input.response().qos();
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

    let consume = input.response().consume();
    let mut consumer = match channel
        .basic_consume(
            response_queue.name().as_str(),
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

    let ack_options = input.response().acknowledge();

    return wait_for_response(
        response_timeout_in_seconds,
        replier,
        correlation_id,
        &mut consumer,
        ack_options,
    )
    .await;
}

async fn wait_for_response<ResponseType: for<'de> Deserialize<'de>>(
    response_timeout_in_seconds: u64,
    replier: Sender<Result<ResponseType, Error>>,
    correlation_id: ShortString,
    consumer: &mut lapin::Consumer,
    ack_options: &BasicAckOptions,
) -> Result<(), Error> {
    let start = Instant::now();

    loop {
        if start.elapsed().as_secs() > response_timeout_in_seconds {
            return send_error_to_replier(
                replier,
                Error::new(
                    ErrorKind::AmqpFailure,
                    format!(
                        "timed out waiting for response with correlation id '{}'",
                        correlation_id
                    ),
                ),
            );
        }

        let delivery = match timeout(
            Duration::from_secs(response_timeout_in_seconds),
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
                        );
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
                );
            }
        };

        match delivery.properties.correlation_id() {
            Some(delivery_correlation_id) => {
                if *delivery_correlation_id != correlation_id {
                    continue;
                }
            }
            None => continue,
        }

        match delivery.ack(*ack_options).await {
            Ok(_) => (),
            Err(error) => log::warn!("failed to acknowledge reply from api: {}", error),
        }

        let response = match serde_json::from_slice::<RequestResult>(&delivery.data) {
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

        let definition = match response {
            RequestResult::Ok(definition) => {
                match serde_json::from_value::<ResponseType>(definition) {
                    Ok(definition) => definition,
                    Err(error) => {
                        return send_error_to_replier(
                            replier,
                            Error::new(
                                ErrorKind::ApiFailure,
                                format!("failed to deserialize definition: {}", error),
                            ),
                        );
                    }
                }
            }
            RequestResult::Err(error) => {
                return send_error_to_replier(
                    replier,
                    Error::new(
                        ErrorKind::ApiFailure,
                        format!("api returned error: {}", error),
                    ),
                )
            }
        };

        match replier.send(Ok(definition)) {
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
