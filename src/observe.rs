use std::error::Error;
use std::sync::mpsc::{Sender, channel};
use std::thread::{self, JoinHandle};

use crate::{LimitOrder, Price};
#[derive(Debug, Copy, Clone)]
pub enum Event {
    New(LimitOrder),
    Cancel {
        order_id: u64,
    },
    Amend {
        order_id: u64,
        new_price: Option<Price>,
        new_quantity: Option<u64>,
    },
}

pub trait Observer {
    fn log(&mut self, event: &Event) -> Result<(), Box<dyn Error>>;
}

pub struct ConsoleLogger {
    logging_thread: JoinHandle<()>,
    sending_channel: Sender<Event>,
}

impl ConsoleLogger {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Default for ConsoleLogger {
    fn default() -> Self {
        let (sender, receiver) = channel();
        Self {
            logging_thread: thread::spawn(move || {
                while let Some(event) = receiver.iter().next() {
                    println!("[ConsoleLogger] {event:?}");
                }
            }),
            sending_channel: sender,
        }
    }
}

impl Observer for ConsoleLogger {
    fn log(&mut self, event: &Event) -> Result<(), Box<dyn Error>> {
        self.sending_channel
            .send(event.clone())
            .map_err(|err| Box::new(err) as Box<dyn Error>)
    }
}
