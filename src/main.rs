use std::{env, fs::File, num::NonZero, path::Path};

use replay::{
    LimitOrder, OrderBook, Side,
    observe::{BufferedFileObserver, ConsoleLogger, MultiObserver},
};

fn main() {
    let observer = ConsoleLogger::new();
    let buffered_file_path =
        std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("buffered_writer.log");
    let buffered_file = File::create(buffered_file_path).expect("Failed to create file");
    let buffered_file_observer = BufferedFileObserver::new(buffered_file);

    let mut book = OrderBook::new_with_observer(MultiObserver::new([
        Box::new(observer),
        Box::new(buffered_file_observer),
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
