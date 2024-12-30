use crate::consts::{HEARTBEAT_EXPIRY, HEARTBEAT_INTERVAL, MDPW_WORKER, MDP_CLIENT, ZMQ_POLL_MSEC};
use crate::is_interrupted;
use crate::majordomo::commands::Command;
use crate::majordomo::error::{MajordomoError, MajordomoResult};
use crate::util::{from_hex_string, to_hex_string, zmq_unwrap, zmq_wrap};
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tracing::{debug, error, info};
use zmq::{Context, Socket};

struct MajordomoBroker {
    context: Context,
    socket: Socket,   // Socket for Clients & Workers
    endpoint: String, // Bind to this endpoint
    services: HashMap<String, Service>,
    workers_to_service: HashMap<String, String>,
    heartbeat_at: Instant,
}

impl Drop for MajordomoBroker {
    fn drop(&mut self) {
        if let Err(e) = self.socket.disconnect(self.endpoint.as_str()) {
            eprintln!("{}", e);
        }
        if let Err(e) = self.context.destroy() {
            eprintln!("{}", e);
        }
    }
}

#[derive(Debug)]
struct Service {
    requests: Vec<Vec<Vec<u8>>>,
    waiting: Vec<Worker>, // List of waiting worker identifiers, ordered by expiry time,
    workers: usize,
}

impl Service {
    pub fn new() -> Self {
        Self {
            requests: Default::default(),
            waiting: Default::default(),
            workers: 0,
        }
    }

    pub(crate) fn update_expiry(&mut self, worker_name: String) {
        if let Some(worker_idx) = self.waiting.iter().position(|w| w.identity == worker_name) {
            self.waiting[worker_idx].expiry =
                Instant::now() + Duration::from_millis(HEARTBEAT_EXPIRY as u64);
        }
    }

    fn purge(&mut self) -> MajordomoResult<Vec<Worker>> {
        if self.waiting.is_empty() || !self.waiting[0].has_expired() {
            return Ok(vec![]);
        }
        let mut expired_workers: Vec<Worker> = vec![];
        // Find the first non-expired worker
        if let Some(split_idx) = self.waiting.iter().position(|worker| !worker.has_expired()) {
            // Remove all expired workers before this index
            expired_workers.extend(self.waiting.drain(0..split_idx));
        } else {
            // All workers have expired, clear the entire vector
            expired_workers.append(&mut self.waiting);
        }
        Ok(expired_workers)
    }
}

#[derive(Debug, Clone)]
struct Worker {
    // Hex String
    identity: String,
    // When worker expires if heartbeat does not occur
    expiry: Instant,
}

impl Worker {
    pub(crate) fn has_expired(&self) -> bool {
        self.expiry < Instant::now()
    }
}

impl Worker {
    pub fn new(identity: String) -> Self {
        info!("Creating worker with identity: {}", identity);
        Self {
            identity,
            expiry: Instant::now() + Duration::from_millis(HEARTBEAT_EXPIRY as u64),
        }
    }
}

impl MajordomoBroker {
    pub fn new(endpoint: String) -> MajordomoResult<Self> {
        let context = Context::new();
        let socket = context.socket(zmq::ROUTER)?;

        let mut broker = MajordomoBroker {
            context: Default::default(),
            socket,
            endpoint,
            services: Default::default(),
            heartbeat_at: Instant::now()
                .checked_add(Duration::from_millis(HEARTBEAT_INTERVAL as u64))
                .unwrap(),
            workers_to_service: Default::default(),
        };

        broker.bind()?;

        Ok(broker)
    }

    /// Bind to a socket
    /// Same socket used for workers and clients.
    fn bind(&mut self) -> MajordomoResult<()> {
        self.socket.bind(self.endpoint.as_str())?;
        info!("MDP broker/0.2.0 is active at {}", self.endpoint);
        Ok(())
    }

    fn purge(&mut self) -> MajordomoResult<()> {
        let mut expired_workers: Vec<Worker> = vec![];
        for service in self.services.values_mut() {
            expired_workers.extend(service.purge()?);
        }
        for worker in expired_workers {
            self.workers_to_service.remove(&worker.identity);
        }
        Ok(())
    }

    /// Remove worker from broker and service
    fn delete_worker(
        &mut self,
        worker_identity: &str,
        service_name: Option<String>,
        disconnect: bool,
    ) -> MajordomoResult<()> {
        if disconnect {
            MajordomoBroker::send_to_worker(
                &self.socket,
                worker_identity,
                Command::Disconnect,
                None,
                None,
            )?;
        }
        self.workers_to_service.remove(worker_identity);
        debug!("Deleting worker {}", worker_identity);
        if let Some(service_name) = service_name {
            debug!("Deleting worker from service {}", service_name);
            let relevant_service = self
                .services
                .get_mut(service_name.as_str())
                .ok_or(MajordomoError::Generic("Service not found".to_owned()))?;
            relevant_service.workers -= 1;
            relevant_service.waiting.remove(
                relevant_service
                    .waiting
                    .iter()
                    .position(|w| w.identity.as_str() == worker_identity)
                    .ok_or(MajordomoError::Generic("Worker not found".to_owned()))?,
            );
        }

        Ok(())
    }

    fn service_dispatch(
        &mut self,
        service_name: &str,
        msg: Option<Vec<Vec<u8>>>,
    ) -> MajordomoResult<()> {
        if let Some(msg) = msg {
            self.services
                .get_mut(service_name)
                .ok_or(MajordomoError::Generic(
                    "Could not find service".to_string(),
                ))?
                .requests
                .push(msg);
        }

        self.purge()?;
        let service = self
            .services
            .get_mut(service_name)
            .ok_or(MajordomoError::Generic(
                "Could not find service".to_string(),
            ))?;
        // When there's a request and an available worker for this service.
        while !service.waiting.is_empty() && !service.requests.is_empty() {
            // Get available worker.
            let worker = service.waiting.pop().unwrap();
            let message = service.requests.pop().unwrap();
            MajordomoBroker::send_to_worker(
                &self.socket,
                worker.identity.as_str(),
                Command::Request,
                None,
                Some(message),
            )?;
        }
        Ok(())
    }

    fn send_to_worker(
        socket: &Socket,
        worker_identity: &str,
        command: Command,
        option: Option<&str>,
        msg: Option<Vec<Vec<u8>>>,
    ) -> MajordomoResult<()> {
        let command_bytes = vec![command.as_byte()];
        // Protocol Order is;
        //  1. Address;
        //  2. Identifier
        //  3. Command
        //  4. Existing Message

        info!("Sending Command: {} to worker {}", command, worker_identity);
        let message_parts: Vec<Vec<u8>> = vec![
            Vec::from(MDPW_WORKER.as_bytes()),
            Vec::from(command_bytes.as_slice()),
        ];
        let mut message_parts = zmq_wrap(
            vec![from_hex_string(worker_identity).unwrap()],
            message_parts,
        );
        if let Some(opt) = option {
            message_parts.push(Vec::from(opt.as_bytes()));
        }
        if let Some(existing_msg) = msg {
            message_parts.extend(existing_msg.iter().cloned());
        }
        debug!(
            "Sending message {:?} to worker {}",
            message_parts, worker_identity
        );
        socket.send_multipart(&message_parts, 0)?;
        Ok(())
    }

    fn add_waiting_worker(
        &mut self,
        worker_identity: &str,
        service_name: &str,
        new_worker: bool
    ) -> MajordomoResult<()> {
        let relevant_service = self.services.get_mut(service_name).ok_or(MajordomoError::Generic("Service not found".to_owned()))?;
        relevant_service.waiting.push(Worker::new(worker_identity.to_string()));
        if new_worker {
            relevant_service.workers += 1;
        }
        self.workers_to_service
            .insert(worker_identity.to_string(), service_name.to_string());
        Ok(())
    }

    fn handle_worker_ready(
        &mut self,
        sender: Vec<u8>,
        worker_ready: bool,
        worker_identity: &str,
        mut message: Vec<Vec<u8>>,
    ) -> MajordomoResult<()> {
        if worker_ready {
            let worker_service = self.workers_to_service.get(worker_identity).cloned();
            // Worker is in an invalid configuration - it cannot READY twice. Remove.
            self.delete_worker(worker_identity, worker_service, true)
        } else if sender.len() >= 4 // Reserved service name
                    && sender.starts_with(b"mmi.")
        {
            // Worker is invalid - it's using reserved naming. Remove.
            self.delete_worker(worker_identity, None, true)
        } else {
            let service_name = String::from_utf8(message.remove(0)).unwrap();
            // If the service doesn't exist, create it.
            self.services
                .entry(service_name.clone())
                .or_insert(Service::new());
            // Attach worker to service and mark as idle.
            self.add_waiting_worker(worker_identity, service_name.as_str(), true)?;
            Ok(())
        }
    }

    fn handle_worker_reply(
        &mut self,
        worker_identity: &str,
        message: Vec<Vec<u8>>,
    ) -> MajordomoResult<()> {
        let maybe_service_name = self.workers_to_service.get(worker_identity).cloned();
        if let Some(service_name) = maybe_service_name {
            let (routing_addresses, message) = zmq_unwrap(message);
            let mut core_message = vec![
                MDP_CLIENT.as_bytes().to_vec(),
                service_name.clone().into_bytes(),
            ];
            core_message.extend(message);
            let message = zmq_wrap(routing_addresses, core_message);
            self.socket.send_multipart(&message, 0)?;
            self.add_waiting_worker(worker_identity, service_name.as_str(), false)?;
        } else {
            // Worker is invalid - it has no attached service. Remove.
            self.delete_worker(worker_identity, None, true)?;
        }
        Ok(())
    }

    fn handle_worker_message(
        &mut self,
        sender: Vec<u8>,
        mut message: Vec<Vec<u8>>,
    ) -> MajordomoResult<()> {
        if message.is_empty() {
            return Err(MajordomoError::ProtocolError(
                "At least a command is required in message",
            ));
        }
        let worker_key = to_hex_string(sender.as_slice());
        let worker_ready = self.workers_to_service.contains_key(worker_key.as_str());
        let command = Command::from_byte(message.remove(0)[0])
            .ok_or(MajordomoError::ProtocolError("Invalid Command received"))?;
        info!(
            "Received worker message - {} - regarding {} - from sender {:?}",
            command, worker_key, sender
        );
        match command {
            Command::Ready => {
                self.handle_worker_ready(sender, worker_ready, worker_key.as_str(), message)?
            }
            Command::Reply => self.handle_worker_reply(worker_key.as_str(), message)?,
            Command::Heartbeat => {
                let maybe_service_name = self.workers_to_service.get(worker_key.as_str());
                match maybe_service_name {
                    None => {
                        // Worker is invalid as it has no attached service. Remove.
                        self.delete_worker(worker_key.as_str(), None, true)?;
                    }
                    Some(service_name) => {
                        self.services
                            .get_mut(service_name.as_str())
                            .expect("Could not find service")
                            .update_expiry(worker_key);
                    }
                }
            }
            Command::Disconnect => {
                // Worker requested disconnect. Remove.
                self.delete_worker(
                    worker_key.as_str(),
                    self.workers_to_service.get(worker_key.as_str()).cloned(),
                    true,
                )?;
            }
            Command::Request => unreachable!("Not supported by Majordomo Broker, only workers"),
        }
        Ok(())
    }

    fn handle_heartbeat(&mut self) -> MajordomoResult<()> {
        let now = Instant::now();
        if now > self.heartbeat_at {
            self.purge()?;

            // Send heartbeat to all waiting workers
            let mut workers_to_heartbeat = Vec::new();
            for service in self.services.values() {
                workers_to_heartbeat.extend(service.waiting.iter().cloned());
            }

            for worker in workers_to_heartbeat {
                MajordomoBroker::send_to_worker(
                    &self.socket,
                    &worker.identity,
                    Command::Heartbeat,
                    None,
                    None,
                )?;
            }

            self.heartbeat_at = now + Duration::from_millis(HEARTBEAT_INTERVAL as u64);
        }
        Ok(())
    }

    /// Process a request coming from a client. We implement MMI requests directly here.
    fn handle_client_message(
        &mut self,
        sender: Vec<u8>,
        mut message: Vec<Vec<u8>>,
    ) -> MajordomoResult<()> {
        if message.len() < 2 {
            return Err(MajordomoError::ProtocolError(
                "Message requires Service Name + Body",
            ));
        }
        let service_name = String::from_utf8(message.remove(0)).unwrap();
        self.services
            .entry(service_name.clone())
            .or_insert(Service::new());
        // Set reply return identity
        let mut message = zmq_wrap(vec![sender], message);
        info!("Handling client message to {}", service_name);
        if service_name.starts_with("mmi.") {
            // If internal
            let return_code: String = if service_name == "mmi.service" {
                let requested_service = String::from_utf8(message.last().unwrap().clone()).unwrap();
                let service = self.services.get(&requested_service);
                match service {
                    None => "501".to_string(),
                    Some(service) => {
                        if service.workers > 0 {
                            "200".to_string()
                        } else {
                            "404".to_string()
                        }
                    }
                }
            } else {
                "501".to_string()
            };
            // Replace the last frame with the new return code
            if let Some(last_frame) = message.last_mut() {
                last_frame.clear();
                last_frame.extend_from_slice(return_code.as_bytes());
            }
            let (routing_frames, mut message) = zmq_unwrap(message);
            message.insert(0, service_name.into_bytes());
            message.insert(0, MDP_CLIENT.as_bytes().to_vec());
            let message = zmq_wrap(routing_frames, message);
            self.socket.send_multipart(&message, 0)?;
        } else {
            // Dispatch to requested service
            info!("Dispatching client message to {}", service_name);
            self.service_dispatch(service_name.as_str(), Some(message))?;
        }
        Ok(())
    }
}

pub(crate) fn example_broker() -> MajordomoResult<()> {
    let mut broker = MajordomoBroker::new("tcp://*:5555".to_string())?;
    loop {
        match broker
            .socket
            .poll(zmq::POLLIN, (HEARTBEAT_INTERVAL * ZMQ_POLL_MSEC) as i64)
            .map_err(MajordomoError::from)?
        {
            // Message
            1 => {
                let mut message = broker
                    .socket
                    .recv_multipart(0)
                    .map_err(MajordomoError::from)?;
                let sender = message.remove(0);
                // Empty.
                _ = message.remove(0);
                let header = message.remove(0);

                if header == MDP_CLIENT.as_bytes() {
                    broker.handle_client_message(sender, message)?;
                } else if header == MDPW_WORKER.as_bytes() {
                    broker.handle_worker_message(sender, message)?
                } else {
                    error!("Invalid message")
                }
            }
            // No Event
            0 => {}
            // Disallowed by API
            _ => unreachable!(),
        }
        broker.handle_heartbeat()?;
        if is_interrupted() {
            return Err(MajordomoError::Interrupted);
        }
    }
}
