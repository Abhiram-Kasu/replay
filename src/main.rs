use std::{
    env,
    fs::{File, OpenOptions},
    num::NonZero,
    path::Path,
};

use replay::{
    LimitOrder, OrderBook, Side,
    observe::{BufferedFileObserver, ConsoleLogger, MemoryMappedFileObserver, MultiObserver},
};

fn main() {
    let observer = ConsoleLogger::new();
    let buffered_file_path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));

    let buffered_file = File::create(buffered_file_path.join("buffered_writer.log"))
        .expect("Failed to create file");
    let buffered_file_observer = BufferedFileObserver::new(buffered_file);

    let mut options = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(buffered_file_path.join("mmap_file_log.log"))
        .expect("Failed to open MMAP file");
    options.set_len(1024);

    let mmap_observer = MemoryMappedFileObserver::new(options, 1024)
        .expect("Failed to create MemoryMappedFileObserver");

    let mut book = OrderBook::new_with_observer(MultiObserver::new([
        Box::new(observer),
        Box::new(buffered_file_observer),
        Box::new(mmap_observer),
    ]));

    // Two resting asks at the same price. ID 1 arrived first, so it fills first.
    book.submit(LimitOrder {
        id: 1,
        side: Side::Sell,
        price: NonZero::new(101).unwrap(),
        quantity: 5,
    })
    .expect("valid order");
    book.submit(LimitOrder {
        id: 2,
        side: Side::Sell,
        price: NonZero::new(101).unwrap(),
        quantity: 3,
    })
    .expect("valid order");

    let trades = book
        .submit(LimitOrder {
            id: 3,
            side: Side::Buy,
            price: NonZero::new(101).unwrap(),
            quantity: 6,
        })
        .expect("valid order");

    println!("trades: {trades:#?}");
    println!("best ask: {:?}", book.best_ask());
}
