use crate::config::amqp_input_api_consumer::AmqpInputApiConsumer;
use crate::config::amqp_output_api_consumer::AmqpOutputApiConsumer;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize)]
pub struct ApiConsumer {
    input: Vec<AmqpInputApiConsumer>,
    output: Vec<AmqpOutputApiConsumer>,
}

impl ApiConsumer {
    pub fn input(&self) -> &[AmqpInputApiConsumer] {
        &self.input
    }

    pub fn output(&self) -> &[AmqpOutputApiConsumer] {
        &self.output
    }
}
