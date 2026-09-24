use std::error::Error;
use std::fmt::Display;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

use memmap2::{MmapMut, MmapOptions};

use crate::worker::Worker;
use crate::{LimitOrder, Price, Trade};
#[derive(Debug, Copy, Clone)]
pub enum Event {
    New(LimitOrder),
    Trade(Trade),
    Cancel {
        order_id: u64,
    },
    Amend {
        order_id: u64,
        new_price: Option<Price>,
        new_quantity: Option<u64>,
    },
}
impl From<Event> for TimedEvent {
    fn from(event: Event) -> Self {
        TimedEvent {
            event,
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

impl TimedEvent {
    pub fn event(&self) -> Event {
        self.event
    }
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
            .send(*event)
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

impl<const NUM: usize> Observer for MultiObserver<NUM> {
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
            if let Err(err) = writeln!(buf_writer, "[BufferedFileObserver] {event}") {
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
            .send(*event)
            .map_err(|err| Box::new(err) as Box<dyn Error>)
    }
}

impl FileObserver for BufferedFileObserver {}
//Will be append only, so we can memory map chunk by chunk
pub struct MemoryMappedFileObserver {
    worker: Worker<TimedEvent>,
    mmap: Arc<RwLock<MmapMut>>,
    file: File,
    // needed to truncate the file correctly at the end
    total_file_size: Arc<AtomicUsize>,
}

struct MemoryMappedFileObserverState {
    mmap_handle: Arc<RwLock<MmapMut>>,
    file: File,
    offset: usize,
}

impl Observer for MemoryMappedFileObserver {
    fn log(&mut self, event: &TimedEvent) -> Result<(), Box<dyn Error>> {
        self.worker.sender().send(*event)?;
        Ok(())
    }
}

impl FileObserver for MemoryMappedFileObserver {}
impl MemoryMappedFileObserverState {
    fn ensure_capacity(&mut self, required_size: usize) -> Result<(), Box<dyn Error>> {
        let mut mmap = self
            .mmap_handle
            .write()
            .expect("Failed to lock mmap for growth");
        if required_size <= mmap.len() {
            return Ok(());
        }

        mmap.flush()?;
        let new_size = required_size.max(mmap.len().checked_mul(2).ok_or_else(|| {
            std::io::Error::other("memory-mapped log grew beyond addressable capacity")
        })?);
        self.file.set_len(new_size as u64)?;
        *mmap = unsafe { MmapOptions::new().len(new_size).map_mut(&self.file)? };
        Ok(())
    }
}

impl Drop for MemoryMappedFileObserver {
    fn drop(&mut self) {
        self.worker
            .shutdown()
            .expect("Failed to stop memory-mapped observer worker");
        // truncate the file
        self.file
            .set_len(
                self.total_file_size
                    .load(std::sync::atomic::Ordering::Acquire) as u64,
            )
            .expect("Failed to truncate file");
        self.mmap
            .read()
            .expect("Failed to lock mmap for Read")
            .flush()
            .expect("Failed to flush mmap changes on drop");
    }
}

impl MemoryMappedFileObserver {
    // we will just overwrite anything in the file, maybe in the future have a pointer into the file from where we can start writing
    pub fn new(file: File, initial_size: usize) -> Result<Self, Box<dyn Error>> {
        if initial_size == 0 {
            return Err("memory-mapped file observer requires a non-zero initial size".into());
        }
        file.set_len(initial_size as u64)?;
        let mmap = Arc::new(RwLock::new(unsafe {
            MmapOptions::default().len(initial_size).map_mut(&file)
        }?));

        let mut state = MemoryMappedFileObserverState {
            mmap_handle: mmap.clone(),
            file: file.try_clone()?,
            offset: 0,
        };

        let total_file_size = Arc::new(AtomicUsize::new(0));
        let worker_copy = total_file_size.clone();

        let worker = Worker::new(move |event: TimedEvent| {
            let st = &mut state;
            let output = format!("[MMapObserver] {event}\n");
            let len = output.len();
            let required_size = st
                .offset
                .checked_add(len)
                .expect("memory-mapped log size overflow");
            st.ensure_capacity(required_size)
                .expect("Failed to grow memory-mapped log");

            let mut mmap_write = st.mmap_handle.write().expect("Failed to get write handle");
            mmap_write[st.offset..required_size].copy_from_slice(output.as_bytes());
            st.offset = required_size;
            worker_copy.store(st.offset, std::sync::atomic::Ordering::Release);
        });
        Ok(Self {
            worker,
            file,
            mmap,
            total_file_size,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Side;
    use std::fs::{self, OpenOptions};
    use std::num::NonZero;

    #[test]
    fn memory_mapped_observer_grows_the_backing_file() {
        let path = std::env::temp_dir().join(format!(
            "replay-mmap-observer-{}-{}.log",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&path)
            .unwrap();

        {
            let mut observer = MemoryMappedFileObserver::new(file, 16).unwrap();
            observer
                .log(
                    &Event::New(LimitOrder {
                        id: 1,
                        side: Side::Buy,
                        price: NonZero::new(100).unwrap(),
                        quantity: 1,
                    })
                    .into(),
                )
                .unwrap();
        }

        let contents = fs::read_to_string(&path).unwrap();
        assert!(contents.contains("New(LimitOrder"));
        assert!(fs::metadata(&path).unwrap().len() > 16);
        fs::remove_file(path).unwrap();
    }
}
