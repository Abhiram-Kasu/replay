use std::error::Error;
use std::fmt::Display;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::os::fd::{AsFd, AsRawFd, IntoRawFd, RawFd};
use std::time::{SystemTime, UNIX_EPOCH};

use memmap2::{Mmap, MmapMut, MmapOptions};

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
pub struct MultiObserver<const NUM_OBSERVERS: usize> {
    observers: [Box<dyn Observer>; NUM_OBSERVERS],
}

impl<const NUM: usize> MultiObserver<NUM> {
    pub fn new(observers: [Box<dyn Observer>; NUM]) -> Self {
        Self { observers }
    }
}

impl<'a, const NUM: usize> Observer for MultiObserver<NUM> {
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
            if let Err(err) = write!(buf_writer, "[BufferedFileObserver] {event}\n") {
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
//Will be append only, so we can memory map chunk by chunk
struct MemoryMappedFileObserver {
    worker: Worker<TimedEvent>,
}

struct MemoryMappedFileObserverState {
    file_fd: RawFd,
    mmap_handle: MmapMut,
    offset: usize,
    default_size: usize,
}

impl MemoryMappedFileObserverState {
    fn remap(&mut self) -> Result<(), Box<dyn Error>> {
        self.mmap_handle = unsafe {
            MmapOptions::new()
                .offset(self.offset as u64)
                .len(self.default_size)
                .map_mut(self.file_fd)?
        };

        self.offset = 0;

        Ok(())
    }
}

impl MemoryMappedFileObserver {
    // we will just overwrite anything in the file, maybe in the future have a pointer into the file from where we can start writing
    pub fn new(file: File, initial_size: usize) -> Result<Self, Box<dyn Error>> {
        let mmap = unsafe {
            MmapOptions::default()
                .len(initial_size)
                .map_mut(file.as_raw_fd())
        }?;

        let mut state = MemoryMappedFileObserverState {
            file_fd: file.into_raw_fd(),
            mmap_handle: mmap,
            offset: 0,
            default_size: initial_size,
        };

        let worker = Worker::new(move |event: TimedEvent| {
            let st = &mut state;
            let output = format!("[MMapObserver] {event}");
            let len = output.len();

            if len + st.offset >= st.mmap_handle.len() {
                if let Some(err) = st.remap().err() {
                    eprintln!("Failed to remap: {err}");
                }
            }

            let bytes = output.bytes().collect::<Vec<_>>();

            st.mmap_handle[st.offset..st.offset + bytes.len()].copy_from_slice(bytes.as_slice());
        });
        Ok(Self { worker })
    }
}
