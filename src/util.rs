use crate::error::Error;
use serde::Deserialize;
use tokio::sync::oneshot::Sender;

/// Send the error to the replier and ALWAYS return an instance of the error through the Err.
/// The Ok variant is never used.
pub fn send_error_to_replier<ResponseType: for<'de> Deserialize<'de>>(
    replier: Sender<Result<ResponseType, Error>>,
    error: Error,
) -> Result<(), Error> {
    match replier.send(Err(error.clone())) {
        Ok(_) => (),
        Err(_) => log::error!("failed to send erroneous reply"),
    }

    Err(error)
}
