use std::error::Error;
use std::fmt::Display;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
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
    const MAX_BINARY_LEN: usize = 49;

    pub fn event(&self) -> Event {
        self.event
    }

    fn binary_len(&self) -> usize {
        match self.event {
            Event::New(_) => 42,
            Event::Trade(_) => 49,
            Event::Cancel { .. } => 25,
            Event::Amend { .. } => 43,
        }
    }

    /// Encodes one event in the observer binary format.
    ///
    /// Every record starts with a one-byte event tag followed by a little-endian
    /// `u128` timestamp. New-order, trade, and cancel payloads then contain their
    /// fields in declaration order. Amend records use presence bytes followed by
    /// fixed-width price and quantity fields, so their records are fixed-size too.
    fn encode_into(&self, output: &mut [u8]) -> usize {
        let mut writer = BinaryWriter::new(output);
        match self.event {
            Event::New(order) => {
                writer.u8(1);
                writer.u128(self.time);
                writer.u64(order.id);
                writer.u8(match order.side {
                    crate::Side::Buy => 0,
                    crate::Side::Sell => 1,
                });
                writer.i64(order.price.get());
                writer.u64(order.quantity);
            }
            Event::Trade(trade) => {
                writer.u8(2);
                writer.u128(self.time);
                writer.u64(trade.maker_id);
                writer.u64(trade.taker_id);
                writer.i64(trade.price.get());
                writer.u64(trade.quantity);
            }
            Event::Cancel { order_id } => {
                writer.u8(3);
                writer.u128(self.time);
                writer.u64(order_id);
            }
            Event::Amend {
                order_id,
                new_price,
                new_quantity,
            } => {
                writer.u8(4);
                writer.u128(self.time);
                writer.u64(order_id);
                writer.u8(u8::from(new_price.is_some()));
                writer.i64(new_price.map_or(0, Price::get));
                writer.u8(u8::from(new_quantity.is_some()));
                writer.u64(new_quantity.unwrap_or(0));
            }
        }
        writer.len()
    }
}

struct BinaryWriter<'a> {
    output: &'a mut [u8],
    offset: usize,
}

impl<'a> BinaryWriter<'a> {
    fn new(output: &'a mut [u8]) -> Self {
        Self { output, offset: 0 }
    }

    fn len(&self) -> usize {
        self.offset
    }

    fn bytes(&mut self, bytes: &[u8]) {
        let end = self.offset + bytes.len();
        self.output[self.offset..end].copy_from_slice(bytes);
        self.offset = end;
    }

    fn u8(&mut self, value: u8) {
        self.bytes(&[value]);
    }

    fn u64(&mut self, value: u64) {
        self.bytes(&value.to_le_bytes());
    }

    fn u128(&mut self, value: u128) {
        self.bytes(&value.to_le_bytes());
    }

    fn i64(&mut self, value: i64) {
        self.bytes(&value.to_le_bytes());
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
        let mut output = [0; TimedEvent::MAX_BINARY_LEN];
        let worker = Worker::new(move |event: TimedEvent| {
            let len = event.encode_into(&mut output);
            if let Err(err) = buf_writer.write_all(&output[..len]) {
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
    file: File,
    // needed to truncate the file correctly at the end
    total_file_size: Arc<AtomicUsize>,
}

struct MemoryMappedFileObserverState {
    mmap: MmapMut,
    file: File,
    offset: usize,
    total_file_size: Arc<AtomicUsize>,
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
        if required_size <= self.mmap.len() {
            return Ok(());
        }

        let new_size = required_size.max(
            self.mmap
                .len()
                .checked_add(self.mmap.len() / 2)
                .ok_or_else(|| {
                    std::io::Error::other("memory-mapped log grew beyond addressable capacity")
                })?,
        );
        self.file.set_len(new_size as u64)?;
        self.mmap = unsafe { MmapOptions::new().len(new_size).map_mut(&self.file)? };
        Ok(())
    }
}

impl Drop for MemoryMappedFileObserverState {
    fn drop(&mut self) {
        self.mmap
            .flush()
            .expect("Failed to flush memory-mapped log on worker shutdown");
        self.total_file_size
            .store(self.offset, std::sync::atomic::Ordering::Release);
    }
}

impl Drop for MemoryMappedFileObserver {
    fn drop(&mut self) {
        self.worker
            .shutdown()
            .expect("Failed to stop memory-mapped observer worker");
        self.file
            .set_len(
                self.total_file_size
                    .load(std::sync::atomic::Ordering::Acquire) as u64,
            )
            .expect("Failed to truncate file");
    }
}

impl MemoryMappedFileObserver {
    // we will just overwrite anything in the file, maybe in the future have a pointer into the file from where we can start writing
    pub fn new(file: File, initial_size: usize) -> Result<Self, Box<dyn Error>> {
        if initial_size == 0 {
            return Err("memory-mapped file observer requires a non-zero initial size".into());
        }
        file.set_len(initial_size as u64)?;
        let mmap = unsafe { MmapOptions::default().len(initial_size).map_mut(&file) }?;

        let total_file_size = Arc::new(AtomicUsize::new(0));
        let mut state = MemoryMappedFileObserverState {
            mmap,
            file: file.try_clone()?,
            offset: 0,
            total_file_size: Arc::clone(&total_file_size),
        };

        let worker = Worker::new(move |event: TimedEvent| {
            let st = &mut state;
            let len = event.binary_len();
            let required_size = st
                .offset
                .checked_add(len)
                .expect("memory-mapped log size overflow");
            st.ensure_capacity(required_size)
                .expect("Failed to grow memory-mapped log");

            event.encode_into(&mut st.mmap[st.offset..required_size]);
            st.offset = required_size;
        });
        Ok(Self {
            worker,
            file,
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
            let event: TimedEvent = Event::New(LimitOrder {
                id: 1,
                side: Side::Buy,
                price: NonZero::new(100).unwrap(),
                quantity: 1,
            })
            .into();
            observer.log(&event).unwrap();
        }

        let contents = fs::read(&path).unwrap();
        assert_eq!(contents.len(), 42);
        assert_eq!(contents[0], 1);
        assert!(fs::metadata(&path).unwrap().len() > 16);
        fs::remove_file(path).unwrap();
    }
}
