use hex::FromHexError;

pub(crate) fn to_hex_string(bytes: &[u8]) -> String {
    hex::encode(bytes)
}

pub(crate) fn from_hex_string(hex: &str) -> Result<Vec<u8>, FromHexError> {
    hex::decode(hex)
}

/// Unwrap a message by removing all routing frames
/// Returns (Routing Frames, Remaining Message)
pub fn zmq_unwrap(mut message: Vec<Vec<u8>>) -> (Vec<Vec<u8>>, Vec<Vec<u8>>) {
    for i in 0..message.len() {
        if message[i].is_empty() {
            let remaining = message.split_off(i + 1);
            return (message, remaining);
        }
    }
    // No delimiter found - treat whole message as content
    (Vec::new(), message)
}

/// Wrap a message with a routing envelope
///
/// Takes a routing address and inserts it plus delimiter at start of message
pub fn zmq_wrap(mut routing_addresses: Vec<Vec<u8>>, mut message: Vec<Vec<u8>>) -> Vec<Vec<u8>> {
    routing_addresses.reserve(1 + message.len());
    routing_addresses.push(Vec::new());
    routing_addresses.append(&mut message);
    routing_addresses
}
