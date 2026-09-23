use std::env::consts::DLL_PREFIX;
use std::error::Error;
use std::fmt::Display;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::marker::PhantomData;
use std::sync::mpsc::{Sender, channel};
use std::thread::{self, JoinHandle, spawn};
use std::time::{SystemTime, UNIX_EPOCH};

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
    logging_thread: JoinHandle<()>,
    sending_channel: Sender<TimedEvent>,
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
                    println!("[ConsoleLogger] {event}");
                }
            }),
            sending_channel: sender,
        }
    }
}

impl Observer for ConsoleLogger {
    fn log(&mut self, event: &TimedEvent) -> Result<(), Box<dyn Error>> {
        self.sending_channel
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

struct Worker<TParam> {
    sender: Sender<TParam>,
    thread: JoinHandle<()>,
}

impl<TParam: Send + 'static> Worker<TParam> {
    pub fn new<TFunc>(mut func: TFunc) -> Self
    where
        TFunc: FnMut(TParam) -> () + Send + 'static,
    {
        let (sender, receiver) = channel::<TParam>();
        let thread = spawn(move || {
            for item in receiver.iter() {
                func(item);
            }
        });
        Self { sender, thread }
    }

    pub fn sender(&self) -> &Sender<TParam> {
        &self.sender
    }

    pub fn join(self) -> std::thread::Result<()> {
        drop(self.sender);
        self.thread.join()
    }
}
