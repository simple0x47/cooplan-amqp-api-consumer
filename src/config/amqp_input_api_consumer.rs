use crate::config::amqp_queue_consumer::AmqpQueueConsumer;
use crate::config::amqp_request::AmqpRequest;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize)]
pub struct AmqpInputApiConsumer {
    id: String,
    request: AmqpRequest,
    response: AmqpQueueConsumer,
}

impl AmqpInputApiConsumer {
    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn request(&self) -> &AmqpRequest {
        &self.request
    }

    pub fn response(&self) -> &AmqpQueueConsumer {
        &self.response
    }
}
