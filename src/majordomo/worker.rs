use crate::commands::Command;
use crate::error::{TitanicError, TitanicResult};
use crate::ZMQ_POLL_MSEC;
use std::thread::sleep;
use std::time::{Duration, Instant};
use tracing::{debug, warn};
use zmq::{Context, Socket};

const HEARTBEAT_LIVENESS: usize = 3;
const MDPW_WORKER: &str = "MDPW01";
const DEFAULT_HEARTBEAT_DELAY_MS: usize = 2500;
const DEFAULT_RECONNECT_DELAY_MS: usize = 2500;
const MINIMUM_MESSAGE_FRAMES: usize = 3;

struct MajordomoWorker {
    context: Context,
    broker: String,
    broker_socket: Socket,

    service_name: String,

    // Heartbeat Management
    /// How many heartbeat attempts left
    liveness: usize,
    /// When to send HEARTBEAT, millis
    heartbeat_at: Instant,
    /// Heartbeat delay, millis
    heartbeat_delay: usize,
    /// Reconnection delay, millis
    reconnect_delay: usize,

    /// Tracks whether we're expecting a reply from the broker.
    /// This is set to true after sending a message that requires
    /// acknowledgment.
    expect_reply: bool,

    /// List of return addresses for the current request.
    /// These addresses are used to route replies back through
    /// the broker to the original client.
    reply_to: Vec<String>,
}

impl Drop for MajordomoWorker {
    fn drop(&mut self) {
        self.broker_socket.disconnect(&self.broker).unwrap();
        self.context.destroy().unwrap();
    }
}

impl MajordomoWorker {
    fn new(broker: String, service_name: String) -> TitanicResult<Self> {
        let context = Context::new();
        let broker_socket = context.socket(zmq::DEALER)?;
        let heartbeat_delay = DEFAULT_HEARTBEAT_DELAY_MS;

        let mut worker = Self {
            context,
            broker,
            broker_socket,
            service_name,
            liveness: HEARTBEAT_LIVENESS,
            heartbeat_at: Instant::now()
                .checked_add(Duration::from_millis(heartbeat_delay as u64))
                .unwrap(),
            heartbeat_delay,
            reconnect_delay: DEFAULT_RECONNECT_DELAY_MS,
            expect_reply: false,
            reply_to: vec![],
        };

        worker.connect_to_broker()?;
        Ok(worker)
    }

    fn connect_to_broker(&mut self) -> TitanicResult<Socket> {
        let socket = self
            .context
            .socket(zmq::DEALER)
            .map_err(TitanicError::from)?;
        socket
            .connect(self.broker.as_str())
            .map_err(TitanicError::from)?;
        self.send_to_broker(Command::Ready, Some(self.service_name.as_str()), None)?;
        self.liveness = HEARTBEAT_LIVENESS;
        self.heartbeat_at = Instant::now()
            .checked_add(Duration::from_millis(self.heartbeat_delay as u64))
            .unwrap();
        Ok(socket)
    }

    fn reconnect(&mut self) -> TitanicResult<()> {
        self.broker_socket = self.connect_to_broker()?;
        Ok(())
    }

    fn validate_message(reply: &[Vec<u8>]) -> TitanicResult<()> {
        if reply.len() < MINIMUM_MESSAGE_FRAMES {
            return Err(TitanicError::ProtocolError("Invalid message format"));
        }
        if reply[0] != b"" || reply[1] != MDPW_WORKER.as_bytes() {
            return Err(TitanicError::ProtocolError("Invalid protocol header"));
        }
        Ok(())
    }

    fn send_to_broker(
        &self,
        command: Command,
        option: Option<&str>,
        msg: Option<Vec<Vec<u8>>>,
    ) -> TitanicResult<()> {
        let command_bytes = vec![command.as_byte()];

        // Protocol Order is;
        //  1. Empty Frame
        //  2. Identifier
        //  3. Command
        //  4. Existing Message
        let mut message_parts: Vec<Vec<u8>> = vec![
            Vec::from("".as_bytes()),
            Vec::from(MDPW_WORKER.as_bytes()),
            Vec::from(command_bytes.as_slice()),
        ];

        if let Some(opt) = option {
            message_parts.push(Vec::from(opt.as_bytes()));
        }
        if let Some(existing_msg) = msg {
            message_parts.extend(existing_msg.iter().cloned());
        }
        debug!("Sending {} to broker", command);
        self.broker_socket
            .send_multipart(message_parts.as_slice(), 0)?;
        Ok(())
    }

    fn handle_request(&mut self, reply: &[Vec<u8>]) -> TitanicResult<Option<Vec<Vec<u8>>>> {
        let mut reply_idx = 3;
        let mut addresses = Vec::with_capacity(1);

        while !reply[reply_idx].is_empty() {
            addresses.push(String::from_utf8_lossy(&reply[reply_idx]).to_string());
            reply_idx += 1;
        }

        self.reply_to = addresses;
        // Skip empty delimiter frame.
        Ok(Some(reply[reply_idx + 1..].to_vec()))
    }

    fn handle_message(&mut self) -> TitanicResult<Option<Vec<Vec<u8>>>> {
        let reply = self
            .broker_socket
            .recv_multipart(0)
            .map_err(TitanicError::from)?;
        debug!("Received message from broker");
        self.liveness = HEARTBEAT_LIVENESS;
        MajordomoWorker::validate_message(&reply)?;
        let command = Command::from_byte(reply[2][0]).unwrap();
        match command {
            Command::Request => {
                self.handle_request(&reply[1..].to_vec())
            }
            Command::Heartbeat => {
                // Do nothing for heartbeats.
                Ok(None)
            }
            Command::Disconnect => {
                self.reconnect()?;
                Ok(None)
            }
            Command::Reply | Command::Ready => unimplemented!(),
        }
    }

    /// .split recv method
    /// This first sends any reply, and then waits for a new request.
    /// Send reply, if any, to broker, and wait for next request.
    fn receive(&mut self, reply: Option<Vec<Vec<u8>>>) -> TitanicResult<Option<Vec<Vec<u8>>>> {
        if let Some(reply) = reply {
            if self.reply_to.is_empty() {
                return Err(TitanicError::ConfigurationError("No reply address set"));
            } else {
                // Push reply_to into message body
                let mut reply = reply.to_vec();
                // Push empty frame to delimit.
                reply.insert(0, b"".to_vec());
                for address in self.reply_to.iter() {
                    reply.insert(0, Vec::from(address.as_bytes()));
                }
                self.send_to_broker(Command::Reply, None, Some(reply))?;
            }
            self.expect_reply = true;
            loop {
                match self
                    .broker_socket
                    .poll(zmq::POLLIN, (self.heartbeat_delay * ZMQ_POLL_MSEC) as i64)
                    .map_err(TitanicError::from)?
                {
                    // Handle Incoming Message
                    1 => {
                        if let Some(reply) = self.handle_message()? {
                            return Ok(Some(reply));
                        }
                    }
                    // Interruption
                    -1 => break,
                    // Timeout, handle Reconnection
                    0 => {
                        self.liveness -= 1;
                        warn!("Disconnected from broker - retrying");
                        sleep(Duration::from_millis(self.reconnect_delay as u64));
                        self.reconnect()?;
                    },
                    _ => unreachable!("zmq_poll only returns -1, 0, or positive values")
                }
                if Instant::now() > self.heartbeat_at {
                    self.send_to_broker(Command::Heartbeat, None, None)?;
                    self.heartbeat_at = Instant::now()
                        .checked_add(Duration::from_millis(self.heartbeat_delay as u64))
                        .unwrap();
                }
                if crate::is_interrupted() {
                    return Err(TitanicError::Interrupted);
                }
            }
        }
        Ok(None)
    }
}


fn example_worker() -> TitanicResult<()> {
    let mut worker = MajordomoWorker::new("tcp://localhost:5555".to_string(), "echo".to_string())?;
    let mut stored_reply: Option<Vec<Vec<u8>>> = None;
    loop {
        let request = worker.receive(stored_reply)?;
        match request {
            None => break,
            Some(reply) => {
                stored_reply = Some(reply);
            }
        };
    }
    Ok(())
}