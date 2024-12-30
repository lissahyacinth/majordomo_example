pub(crate) mod consts;

pub mod majordomo;
pub(crate) mod util;

use crate::majordomo::broker::example_broker;
use crate::majordomo::client::example_client;
use crate::majordomo::worker::example_worker;
use lazy_static::lazy_static;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tracing_subscriber::EnvFilter;

lazy_static! {
    static ref RUNNING: Arc<AtomicBool> = Arc::new(AtomicBool::new(true));
}

fn is_interrupted() -> bool {
    !RUNNING.load(Ordering::SeqCst)
}

fn init_interrupt_handler() {
    ctrlc::set_handler(move || {
        RUNNING.store(false, Ordering::SeqCst);
    })
    .expect("Error setting Ctrl-C handler");
}

fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();
    init_interrupt_handler();
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 2 {
        eprintln!("Usage: {} [worker|broker|client]", args[0]);
        std::process::exit(1);
    }
    let result = match args[1].as_str() {
        "worker" => example_worker(),
        "broker" => example_broker(),
        "client" => example_client(),
        _ => {
            eprintln!("Invalid argument. Use: worker, broker, or client");
            std::process::exit(1);
        }
    };

    result.unwrap_or_else(|e| {
        eprintln!("Error: {}", e);
        std::process::exit(1);
    });
}
