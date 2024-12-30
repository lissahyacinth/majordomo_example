use crate::error::{TitanicError, TitanicResult};
use crate::{MDP_CLIENT, ZMQ_POLL_MSEC};
use tracing::debug;
use zmq::{Context, Socket};

struct MajordomoClient {
    context: Context,
    broker: String,
    broker_socket: Socket,
    timeout: usize,
    retries: usize,
}

impl Drop for MajordomoClient {
    fn drop(&mut self) {
        if let Err(e) = self.broker_socket.disconnect(&self.broker) {
            debug!("Error disconnecting from broker: {}", e);
        }
    }
}

impl MajordomoClient {
    pub fn new(broker: String) -> TitanicResult<Self> {
        let context = Context::new();
        let broker_socket = MajordomoClient::connect_to_broker(&context, broker.as_str())?;
        debug!("Connected to broker");

        Ok(Self {
            context,
            broker,
            broker_socket,
            timeout: 2500,
            retries: 3,
        })
    }

    fn connect_to_broker(context: &Context, broker: &str) -> TitanicResult<Socket> {
        let socket = context.socket(zmq::REQ).map_err(TitanicError::from)?;
        socket.connect(broker).map_err(TitanicError::from)?;
        Ok(socket)
    }

    fn validate_reply(reply: &[Vec<u8>], service: &str) -> TitanicResult<()> {
        if reply.len() < 3 {
            return Err(TitanicError::InvalidReply("Too few frames"));
        }
        if reply[0] != MDP_CLIENT.as_bytes() {
            return Err(TitanicError::InvalidReply(
                "Invalid protocol header",
            ));
        }
        if reply[1] != service.as_bytes() {
            return Err(TitanicError::InvalidReply("Service mismatch"));
        }
        Ok(())
    }

    /// Send a request to the broker and gets a reply - even if it has to retry multiple times.
    pub fn send(
        &mut self,
        service: &str,
        messages: Vec<Vec<u8>>,
    ) -> TitanicResult<Vec<Vec<u8>>> {
        // Frames
        // Frame 1: "MPDCxy" MDP/Client x.y
        // Frame 2: Service name (Printable String)
        // Frame 3+: Request Body
        let mut message_parts: Vec<Vec<u8>> = vec![Vec::from(MDP_CLIENT.as_bytes()), Vec::from(service.as_bytes())];
        message_parts.extend(messages.iter().cloned());
        debug!("Sending message to {} service", service);
        let mut retries_left: usize = self.retries;
        while retries_left > 0 && !crate::is_interrupted() {
            self.broker_socket
                .send_multipart(message_parts.clone(), 0)
                .map_err(TitanicError::from)?;

            match self
                .broker_socket
                .poll(zmq::POLLIN, (self.timeout * ZMQ_POLL_MSEC) as i64)
                .map_err(TitanicError::from)?
            {
                1 => {
                    // Some event was signalled
                    let reply = self
                        .broker_socket
                        .recv_multipart(0)
                        .map_err(TitanicError::from)?;
                    debug!("Received reply");
                    MajordomoClient::validate_reply(&reply, &self.broker)?;
                    return Ok(reply[2..].to_vec());
                }
                0 => {
                    retries_left -= 1;
                    dbg!("No reply, reconnecting");
                    self.broker_socket =
                        MajordomoClient::connect_to_broker(&self.context, self.broker.as_str())?;
                }
                // Disallowed by API
                _ => unreachable!(),
            }
        }

        if crate::is_interrupted() {
            return Err(TitanicError::Interrupted);
        }
        Err(TitanicError::NoResponseFromBroker(self.retries))
    }

    pub fn set_timeout(&mut self, timeout: usize) {
        self.timeout = timeout;
    }

    pub fn set_retries(&mut self, retries: usize) {
        self.retries = retries;
    }
}

fn example_app() -> TitanicResult<()> {
    let mut majordomo_client = MajordomoClient::new("tcp://localhost:5555".to_string())?;
    let mut count: usize = 0;
    while count < 100_000 {
        let request = b"Hello World";
        let reply = majordomo_client.send("echo", vec![request.to_vec()])?;
        if reply.is_empty() {
            break;
        }
        count += 1;
    }
    println!("{} requests/replies processed", count);
    Ok(())
}
