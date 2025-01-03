use crate::consts::{MDP_CLIENT, ZMQ_POLL_MSEC};
use crate::majordomo::error::{MajordomoError, MajordomoResult};
use crate::util::zmq_unwrap;
use log::warn;
use tracing::debug;
use zmq::{Context, Socket};

struct AsyncMajordomoClient {
    context: Context,
    broker: String,
    broker_socket: Socket,
    timeout: usize,
}

impl Drop for AsyncMajordomoClient {
    fn drop(&mut self) {
        if let Err(e) = self.broker_socket.disconnect(&self.broker) {
            debug!("Error disconnecting from broker: {}", e);
        }
    }
}

impl AsyncMajordomoClient {
    pub fn new(broker: String) -> MajordomoResult<Self> {
        let context = Context::new();
        let broker_socket = AsyncMajordomoClient::connect_to_broker(&context, broker.as_str())?;
        debug!("Connected to broker");

        Ok(Self {
            context,
            broker,
            broker_socket,
            timeout: 2500,
        })
    }

    fn connect_to_broker(context: &Context, broker: &str) -> MajordomoResult<Socket> {
        let socket = context.socket(zmq::DEALER).map_err(MajordomoError::from)?;
        socket.set_rcvhwm(0)?;
        socket.connect(broker).map_err(MajordomoError::from)?;
        Ok(socket)
    }

    fn validate_reply(reply: &[Vec<u8>]) -> MajordomoResult<()> {
        if reply.len() < 4 {
            return Err(MajordomoError::InvalidReply("Too few frames"));
        }
        if reply[0] != MDP_CLIENT.as_bytes() {
            return Err(MajordomoError::InvalidReply("Invalid protocol header"));
        }
        Ok(())
    }

    pub fn send(&mut self, service: &str, messages: Vec<Vec<u8>>) -> MajordomoResult<()> {
        // Frames
        // Frame 0: Empty (Req Emulation)
        // Frame 1: "MPDCxy" MDP/Client x.y
        // Frame 2: Service name (Printable String)
        // Frame 3+: Request Body
        let mut message_parts: Vec<Vec<u8>> = vec![
            Vec::new(),
            Vec::from(MDP_CLIENT.as_bytes()),
            Vec::from(service.as_bytes()),
        ];
        message_parts.extend(messages.iter().cloned());
        self.broker_socket
            .send_multipart(message_parts, 0)
            .map_err(MajordomoError::from)?;
        Ok(())
    }

    pub fn receive(&mut self) -> MajordomoResult<Option<Vec<Vec<u8>>>> {
        match self
            .broker_socket
            .poll(zmq::POLLIN, (self.timeout * ZMQ_POLL_MSEC) as i64)
            .map_err(MajordomoError::from)?
        {
            1 => {
                // Some event was signalled
                let mut reply = self
                    .broker_socket
                    .recv_multipart(0)
                    .map_err(MajordomoError::from)?;
                _ = reply.remove(0);
                let (_routing_envelope, core_message) = zmq_unwrap(reply);
                debug!(
                    "Received reply. Envelope: {:?}, Message: {:?}",
                    _routing_envelope, core_message
                );
                AsyncMajordomoClient::validate_reply(&core_message)?;
                return Ok(Some(core_message));
            }
            0 => {}
            // Disallowed by API
            _ => unreachable!(),
        }
        if crate::is_interrupted() {
            return Err(MajordomoError::Interrupted);
        }
        Ok(None)
    }
}

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
    pub fn new(broker: String) -> MajordomoResult<Self> {
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

    fn connect_to_broker(context: &Context, broker: &str) -> MajordomoResult<Socket> {
        let socket = context.socket(zmq::REQ).map_err(MajordomoError::from)?;
        socket.connect(broker).map_err(MajordomoError::from)?;
        Ok(socket)
    }

    fn validate_reply(reply: &[Vec<u8>], service: &str) -> MajordomoResult<()> {
        if reply.len() < 3 {
            return Err(MajordomoError::InvalidReply("Too few frames"));
        }
        if reply[0] != MDP_CLIENT.as_bytes() {
            return Err(MajordomoError::InvalidReply("Invalid protocol header"));
        }
        if String::from_utf8(reply[1].to_vec()).unwrap() != service {
            return Err(MajordomoError::InvalidReply("Service mismatch"));
        }
        Ok(())
    }

    /// Send a request to the broker and gets a reply - even if it has to retry multiple times.
    pub fn send(&mut self, service: &str, messages: Vec<Vec<u8>>) -> MajordomoResult<Vec<Vec<u8>>> {
        // Frames
        // Frame 1: "MPDCxy" MDP/Client x.y
        // Frame 2: Service name (Printable String)
        // Frame 3+: Request Body
        let mut message_parts: Vec<Vec<u8>> = vec![
            Vec::from(MDP_CLIENT.as_bytes()),
            Vec::from(service.as_bytes()),
        ];
        message_parts.extend(messages.iter().cloned());
        debug!("Sending message {:?} to {} service", message_parts, service);
        let mut retries_left: usize = self.retries;
        while retries_left > 0 && !crate::is_interrupted() {
            self.broker_socket
                .send_multipart(message_parts.clone(), 0)
                .map_err(MajordomoError::from)?;

            match self
                .broker_socket
                .poll(zmq::POLLIN, (self.timeout * ZMQ_POLL_MSEC) as i64)
                .map_err(MajordomoError::from)?
            {
                1 => {
                    // Some event was signalled
                    let reply = self
                        .broker_socket
                        .recv_multipart(0)
                        .map_err(MajordomoError::from)?;
                    let (_routing_envelope, core_message) = zmq_unwrap(reply);
                    debug!(
                        "Received reply. Envelope: {:?}, Message: {:?}",
                        _routing_envelope, core_message
                    );
                    MajordomoClient::validate_reply(&core_message, service)?;
                    return Ok(core_message);
                }
                0 => {
                    retries_left -= 1;
                    if retries_left > 0 {
                        warn!("No reply, reconnecting");
                        self.broker_socket = MajordomoClient::connect_to_broker(
                            &self.context,
                            self.broker.as_str(),
                        )?;
                        self.retries = 3;
                    } else {
                        // No more retries, give up;
                        return Err(MajordomoError::Generic("No more retries".to_string()));
                    }
                }
                // Disallowed by API
                _ => unreachable!(),
            }
        }

        if crate::is_interrupted() {
            return Err(MajordomoError::Interrupted);
        }
        Err(MajordomoError::NoResponseFromBroker(self.retries))
    }
}

enum MClient {
    Sync(MajordomoClient),
    Async(AsyncMajordomoClient),
}

fn example_e2e(mut client: MClient) -> MajordomoResult<()> {
    let start_time = std::time::Instant::now();
    let mut count: usize = 0;

    match client {
        MClient::Sync(ref mut client) => {
            while count < 100_000 {
                let request = b"Hello World";
                let reply = client.send("echo", vec![request.to_vec()])?;
                if reply.is_empty() {
                    break;
                }
                count += 1;
            }
        }
        MClient::Async(ref mut client) => {
            for _ in 0..100_000 {
                let request = b"Hello World";
                client.send("echo", vec![request.to_vec()])?;
            }
            println!("Sent all messages");
            for _ in 0..100_000 {
                let reply = client.receive()?;
                if reply.is_some() {
                    count += 1;
                }
            }
        }
    }

    let duration = start_time.elapsed();
    let requests_per_second = count as f64 / duration.as_secs_f64();

    println!("{} requests/replies processed", count);
    println!("Total time: {:.2?}", duration);
    println!("Requests per second: {:.2}", requests_per_second);
    Ok(())
}

pub(crate) fn example_async_client() -> MajordomoResult<()> {
    example_e2e(MClient::Async(AsyncMajordomoClient::new(
        "tcp://localhost:5555".to_string(),
    )?))
}

pub(crate) fn example_client() -> MajordomoResult<()> {
    example_e2e(MClient::Sync(MajordomoClient::new(
        "tcp://localhost:5555".to_string(),
    )?))
}
