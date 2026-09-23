use std::error::Error;
use std::fmt::Display;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::worker::Worker;
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
impl Into<TimedEvent> for Event {
    fn into(self) -> TimedEvent {
        TimedEvent {
            event: self,
            time: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("Failed to get current time")
                .as_nanos(),
        }
    }
}
#[derive(Clone, Copy)]
pub struct TimedEvent {
    event: Event,
    time: u128,
}

impl Display for TimedEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {:?}", self.time, self.event).expect("Failed to display TimedEvent");
        Ok(())
    }
}

pub trait Observer {
    fn log(&mut self, event: &TimedEvent) -> Result<(), Box<dyn Error>>;
}

pub struct ConsoleLogger {
    worker: Worker<TimedEvent>,
}

impl ConsoleLogger {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Default for ConsoleLogger {
    fn default() -> Self {
        Self {
            worker: Worker::new(move |event| println!("[ConsoleLogger] {event}")),
        }
    }
}

impl Observer for ConsoleLogger {
    fn log(&mut self, event: &TimedEvent) -> Result<(), Box<dyn Error>> {
        self.worker
            .sender()
            .send(event.clone())
            .map_err(|err| Box::new(err) as Box<dyn Error>)
    }
}

// TODO maybe upgrade to multicast eventually for more complicated observers?
pub struct MultiObserver {
    observers: Vec<Box<dyn Observer>>,
}

impl Observer for MultiObserver {
    fn log(&mut self, event: &TimedEvent) -> Result<(), Box<dyn Error>> {
        for observer in &mut self.observers {
            if let Some(e) = observer.log(event).err() {
                return Err(e);
            }
        }
        Ok(())
    }
}

pub trait FileObserver: Observer {}

pub struct BufferedFileObserver {
    worker: Worker<TimedEvent>,
}

impl BufferedFileObserver {
    pub const DEFAULT_BUFFER_SIZE: usize = 8 * 1024;
    pub fn with_capacity(file: File, buffer_size: usize) -> Self {
        let mut buf_writer = BufWriter::with_capacity(buffer_size, file);
        let worker = Worker::new(move |event: TimedEvent| {
            if let Err(err) = write!(buf_writer, "{} {:?}", event.time, event.event) {
                eprintln!("Failed to write {event}: {err}");
            }
        });

        Self { worker }
    }

    pub fn new(file: File) -> Self {
        Self::with_capacity(file, Self::DEFAULT_BUFFER_SIZE)
    }
}

impl Observer for BufferedFileObserver {
    fn log(&mut self, event: &TimedEvent) -> Result<(), Box<dyn Error>> {
        self.worker
            .sender()
            .send(event.clone())
            .map_err(|err| Box::new(err) as Box<dyn Error>)
    }
}

impl FileObserver for BufferedFileObserver {}
