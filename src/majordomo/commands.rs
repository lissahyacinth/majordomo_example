use std::fmt::{Display, Formatter};

#[repr(u8)]
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum Command {
    Ready = 0x01,
    Request = 0x02,
    Reply = 0x03,
    Heartbeat = 0x04,
    Disconnect = 0x05,
}

impl Display for Command {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Command::Ready => write!(f, "Ready"),
            Command::Request => write!(f, "Request"),
            Command::Reply => write!(f, "Reply"),
            Command::Heartbeat => write!(f, "Heartbeat"),
            Command::Disconnect => write!(f, "Disconnect"),
        }
    }
}

impl Command {
    pub fn as_byte(&self) -> u8 {
        match self {
            Command::Ready => 0x01,
            Command::Request => 0x02,
            Command::Reply => 0x03,
            Command::Heartbeat => 0x04,
            Command::Disconnect => 0x05,
        }
    }

    pub fn from_byte(byte: u8) -> Option<Self> {
        match byte {
            0x01 => Some(Command::Ready),
            0x02 => Some(Command::Request),
            0x03 => Some(Command::Reply),
            0x04 => Some(Command::Heartbeat),
            0x05 => Some(Command::Disconnect),
            _ => None,
        }
    }
}
