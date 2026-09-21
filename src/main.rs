use std::num::NonZero;

use replay::{LimitOrder, OrderBook, Side, observe::ConsoleLogger};

fn main() {
    let observer = ConsoleLogger::new();
    let mut book = OrderBook::new_with_observer(observer);

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
