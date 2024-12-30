use thiserror::Error;

pub type MajordomoResult<T> = Result<T, MajordomoError>;

#[derive(Error, Debug, Clone, PartialEq)]
pub enum MajordomoError {
    #[error("Generic: {0}")]
    Generic(String),

    #[error("No response from broker after {0} attempts")]
    NoResponseFromBroker(usize),

    #[error("Interrupted by CTRL-C Event")]
    Interrupted,

    #[error("Invalid Reply due to {0}")]
    InvalidReply(&'static str),

    #[error("Could not reply due to {0}")]
    ConfigurationError(&'static str),

    #[error("Message does not conform to protocol due to {0}")]
    ProtocolError(&'static str),
}

macro_rules! error_from {
    ($err:ty, $majordomo_error:ident, $func:expr) => {
        impl From<$err> for MajordomoError {
            fn from(value: $err) -> Self {
                MajordomoError::$majordomo_error($func(value))
            }
        }
    };
    ($err:ty, $majordomo_error:ident) => {
        impl From<$err> for MajordomoError {
            fn from(value: $err) -> Self {
                MajordomoError::$majordomo_error(value.to_string())
            }
        }
    };
}

error_from!(zmq::Error, Generic);
