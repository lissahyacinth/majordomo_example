// Naming
pub(crate) const MDPW_WORKER: &str = "MDPW01";
pub(crate) const MDP_CLIENT: &str = "MDPC01";

// Heartbeat
pub(crate) const HEARTBEAT_LIVENESS: usize = 3;
pub(crate) const HEARTBEAT_INTERVAL: usize = 2500;

pub(crate) const HEARTBEAT_EXPIRY: usize = HEARTBEAT_LIVENESS * HEARTBEAT_INTERVAL;

// Polling
pub(crate) const ZMQ_POLL_MSEC: usize = 1; // Defined by suggested shim macro in  https://zguide.zeromq.org/docs/chapter1/

// Reconnection
pub(crate) const DEFAULT_RECONNECT_DELAY_MS: usize = 2500;
// Message Validity
pub(crate) const MINIMUM_MESSAGE_FRAMES: usize = 3;
