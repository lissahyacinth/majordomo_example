use crate::consts::{
    DEFAULT_RECONNECT_DELAY_MS, HEARTBEAT_INTERVAL, HEARTBEAT_LIVENESS, MDPW_WORKER,
    MINIMUM_MESSAGE_FRAMES, ZMQ_POLL_MSEC,
};
use crate::majordomo::commands::Command;
use crate::majordomo::error::{MajordomoError, MajordomoResult};
use crate::util::{zmq_unwrap, zmq_wrap};
use std::thread::sleep;
use std::time::{Duration, Instant};
use tracing::{debug, warn};
use zmq::{Context, Socket};

struct MajordomoWorker {
    context: Context,
    broker: String,
    broker_socket: Option<Socket>,

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
    reply_to: Vec<Vec<u8>>,
}

impl Drop for MajordomoWorker {
    fn drop(&mut self) {
        let broker = &self.broker;
        if let Some(broker_socket) = self.broker_socket.as_ref() {
            broker_socket.disconnect(broker).unwrap();
        }
        self.context.destroy().unwrap();
    }
}

impl MajordomoWorker {
    fn new(broker: String, service_name: String) -> MajordomoResult<Self> {
        let context = Context::new();
        let heartbeat_delay = HEARTBEAT_INTERVAL;

        let mut worker = Self {
            context,
            broker,
            broker_socket: None,
            service_name,
            liveness: HEARTBEAT_LIVENESS,
            heartbeat_at: Instant::now()
                .checked_add(Duration::from_millis(HEARTBEAT_INTERVAL as u64))
                .unwrap(),
            heartbeat_delay,
            reconnect_delay: DEFAULT_RECONNECT_DELAY_MS,
            expect_reply: false,
            reply_to: vec![],
        };

        worker.heartbeat_at = Instant::now()
            .checked_add(Duration::from_millis(HEARTBEAT_INTERVAL as u64))
            .unwrap();

        worker.connect_to_broker()?;
        Ok(worker)
    }

    fn connect_to_broker(&mut self) -> MajordomoResult<()> {
        self.broker_socket = Some(
            self.context
                .socket(zmq::DEALER)
                .map_err(MajordomoError::from)?,
        );
        self.broker_socket
            .as_ref()
            .unwrap()
            .connect(self.broker.as_str())
            .map_err(MajordomoError::from)?;
        self.liveness = HEARTBEAT_LIVENESS;
        self.heartbeat_at = Instant::now()
            .checked_add(Duration::from_millis(self.heartbeat_delay as u64))
            .unwrap();
        self.send_to_broker(
            Command::Ready,
            None,
            Some(vec![self.service_name.as_bytes().to_vec()]),
        )?;
        Ok(())
    }

    fn reconnect(&mut self) -> MajordomoResult<()> {
        self.connect_to_broker()?;
        Ok(())
    }

    fn validate_message(reply: &[Vec<u8>]) -> MajordomoResult<()> {
        if reply.len() < MINIMUM_MESSAGE_FRAMES {
            return Err(MajordomoError::ProtocolError("Invalid message format"));
        }
        if reply[0] != b"" || reply[1] != MDPW_WORKER.as_bytes() {
            return Err(MajordomoError::ProtocolError("Invalid protocol header"));
        }
        Ok(())
    }

    fn send_to_broker(
        &self,
        command: Command,
        option: Option<&str>,
        msg: Option<Vec<Vec<u8>>>,
    ) -> MajordomoResult<()> {
        let command_bytes = vec![command.as_byte()];

        // Protocol Order is;
        //  1. Empty Frame
        //  2. Identifier
        //  3. Command
        //  4. Existing Message
        let mut message_parts: Vec<Vec<u8>> = vec![
            Vec::new(),
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
        debug!("Sending message parts: {:?}", message_parts);
        self.broker_socket
            .as_ref()
            .unwrap()
            .send_multipart(message_parts.as_slice(), 0)?;
        debug!("Sent multipart message");
        Ok(())
    }

    fn handle_message(&mut self) -> MajordomoResult<Option<Vec<Vec<u8>>>> {
        let mut reply = self
            .broker_socket
            .as_ref()
            .unwrap()
            .recv_multipart(0)
            .map_err(MajordomoError::from)?;
        self.liveness = HEARTBEAT_LIVENESS;
        MajordomoWorker::validate_message(&reply)?;
        // Remove empty frame.
        reply.remove(0);
        let _header_frame = reply.remove(0);
        let command = Command::from_byte(reply.remove(0)[0]).unwrap();
        debug!("Received {command} message from broker");
        match command {
            Command::Request => {
                let (routing_addresses, message) = zmq_unwrap(reply);
                self.reply_to = routing_addresses;
                Ok(Some(message))
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
    fn receive(&mut self, reply: Option<Vec<Vec<u8>>>) -> MajordomoResult<Option<Vec<Vec<u8>>>> {
        if let Some(reply) = reply {
            if self.reply_to.is_empty() {
                return Err(MajordomoError::ConfigurationError("No reply address set"));
            } else {
                let reply = zmq_wrap(self.reply_to.clone(), reply.to_vec());
                self.send_to_broker(Command::Reply, None, Some(reply))?;
            }
        }
        self.expect_reply = true;
        loop {
            match self
                .broker_socket
                .as_ref()
                .unwrap()
                .poll(zmq::POLLIN, (self.heartbeat_delay * ZMQ_POLL_MSEC) as i64)
                .map_err(MajordomoError::from)?
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
                    if self.liveness == 0 {
                        warn!("Disconnected from broker - retrying");
                        sleep(Duration::from_millis(self.reconnect_delay as u64));
                        self.reconnect()?;
                    }
                }
                _ => unreachable!("zmq_poll only returns -1, 0, or positive values"),
            }
            if Instant::now() > self.heartbeat_at {
                self.send_to_broker(Command::Heartbeat, None, None)?;
                self.heartbeat_at =
                    Instant::now() + Duration::from_millis(self.heartbeat_delay as u64);
            }
            if crate::is_interrupted() {
                return Err(MajordomoError::Interrupted);
            }
        }
        Ok(None)
    }
}

pub(crate) fn example_worker() -> MajordomoResult<()> {
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
