//! A deterministic, price-time-priority limit order book.
//!
//! Prices are expressed as integer ticks and quantities as integer lots.  That
//! is intentional: floating-point values are unsuitable for matching rules
//! because equal-looking decimal values may compare differently in binary.
pub mod observe;
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    num::NonZero,
};

use crate::observe::Event;
use crate::observe::Observer;

pub type OrderId = u64;
pub type Price = NonZero<i64>;
pub type Quantity = u64;

/// The side of an order.  A buy order matches the best ask; a sell order
/// matches the best bid.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Side {
    Buy,
    Sell,
}

/// A client instruction to enter one limit order.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LimitOrder {
    pub id: OrderId,
    pub side: Side,
    /// Integer price ticks, for example cents when one tick is one cent.
    pub price: Price,
    pub quantity: Quantity,
}

/// A fill created by the matching engine.  The trade takes place at the
/// resting (maker) order's price.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Trade {
    pub maker_id: OrderId,
    pub taker_id: OrderId,
    pub price: Price,
    pub quantity: Quantity,
}

/// The public view of a price level.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Level {
    pub price: Price,
    pub quantity: Quantity,
    pub order_count: usize,
}

/// Information returned when an order is cancelled.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CancelledOrder {
    pub id: OrderId,
    pub side: Side,
    pub price: Price,
    pub quantity: Quantity,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BookError {
    NonPositivePrice,
    ZeroQuantity,
    DuplicateOrderId(OrderId),
    UnknownOrderId(OrderId),
}

#[derive(Debug, Default)]
struct PriceLevel {
    /// FIFO is the "time priority" part of price-time priority.
    queue: VecDeque<OrderId>,
}

#[derive(Clone, Copy, Debug)]
struct RestingOrder {
    side: Side,
    price: Price,
    remaining: Quantity,
}

/// A single-instrument continuous limit order book.
///
/// `BTreeMap` makes the best-price selection deterministic: the lowest ask is
/// the first ask key, and the highest bid is the last bid key.  It also makes
/// the book's iteration order stable when we later serialize or replay it.
#[derive(Default)]
pub struct OrderBook {
    observer: Option<Box<dyn Observer>>,
    bids: BTreeMap<Price, PriceLevel>,
    asks: BTreeMap<Price, PriceLevel>,
    resting_orders: BTreeMap<OrderId, RestingOrder>,
    /// IDs are never reused, including when an order fully executes instead
    /// of becoming a resting order.  This makes an event stream unambiguous.
    seen_order_ids: BTreeSet<OrderId>,
}

impl std::fmt::Debug for OrderBook {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OrderBook")
            .field(
                "observer",
                &match self.observer {
                    Some(_) => "Attached Observer",
                    None => "No Observer",
                }
                .to_owned(),
            )
            .field("bids", &self.bids)
            .field("asks", &self.asks)
            .field("resting_orders", &self.resting_orders)
            .field("seen_order_ids", &self.seen_order_ids)
            .finish()
    }
}

impl OrderBook {
    pub fn new_with_observer(observer: impl Observer + 'static) -> Self {
        Self {
            observer: Some(Box::new(observer)),
            ..Self::default()
        }
    }
    pub fn new() -> Self {
        Self::default()
    }

    /// Submit a limit order, immediately matching any executable quantity.
    /// Returns fills in their exact matching order.
    pub fn submit(&mut self, order: LimitOrder) -> Result<Vec<Trade>, BookError> {
        if order.quantity == 0 {
            return Err(BookError::ZeroQuantity);
        }
        if !self.seen_order_ids.insert(order.id) {
            return Err(BookError::DuplicateOrderId(order.id));
        }

        let mut remaining = order.quantity;
        let mut trades = Vec::new();

        if let Some(ob) = &mut self.observer {
            ob.log(&Event::New(order.clone()).into())
                .expect("Failed to log");
        }

        match order.side {
            Side::Buy => {
                while remaining > 0 {
                    let Some(best_ask) = self.asks.first_key_value().map(|(&price, _)| price)
                    else {
                        break;
                    };
                    if best_ask > order.price {
                        // Too high
                        break;
                    }
                    self.match_ask(order.id, best_ask, &mut remaining, &mut trades);
                }
            }
            Side::Sell => {
                while remaining > 0 {
                    let Some(best_bid) = self.bids.last_key_value().map(|(&price, _)| price) else {
                        break;
                    };
                    if best_bid < order.price {
                        break;
                    }
                    self.match_bid(order.id, best_bid, &mut remaining, &mut trades);
                }
            }
        }

        if remaining > 0 {
            self.rest(
                RestingOrder {
                    side: order.side,
                    price: order.price,
                    remaining,
                },
                order.id,
            );
        }

        Ok(trades)
    }

    /// Remove the unfilled portion of a resting order.
    pub fn cancel(&mut self, id: OrderId) -> Result<CancelledOrder, BookError> {
        let order = self
            .resting_orders
            .remove(&id)
            .ok_or(BookError::UnknownOrderId(id))?;

        let levels = match order.side {
            Side::Buy => &mut self.bids,
            Side::Sell => &mut self.asks,
        };
        let level = levels
            .get_mut(&order.price)
            .expect("resting order must have a price level");
        let queue_position = level
            .queue
            .iter()
            .position(|&queued_id| queued_id == id)
            .expect("resting order must occur in its price-level queue");
        level.queue.remove(queue_position);
        if level.queue.is_empty() {
            levels.remove(&order.price);
        }

        if let Some(ob) = &mut self.observer {
            ob.log(&Event::Cancel { order_id: id }.into())
                .expect("Failed to Log");
        }

        Ok(CancelledOrder {
            id,
            side: order.side,
            price: order.price,
            quantity: order.remaining,
        })
    }

    pub fn best_bid(&self) -> Option<Level> {
        self.bids
            .last_key_value()
            .map(|(&price, level)| self.level(price, level))
    }

    pub fn best_ask(&self) -> Option<Level> {
        self.asks
            .first_key_value()
            .map(|(&price, level)| self.level(price, level))
    }

    pub fn resting_order_count(&self) -> usize {
        self.resting_orders.len()
    }

    fn rest(&mut self, order: RestingOrder, id: OrderId) {
        let levels = match order.side {
            Side::Buy => &mut self.bids,
            Side::Sell => &mut self.asks,
        };
        levels.entry(order.price).or_default().queue.push_back(id);
        self.resting_orders.insert(id, order);
    }

    /// Match against the front of the best ask level.  The queue front is the
    /// earliest order at that price, so it must fill before every later order.
    fn match_ask(
        &mut self,
        taker_id: OrderId,
        price: Price,
        remaining: &mut Quantity,
        trades: &mut Vec<Trade>,
    ) {
        let maker_id = *self
            .asks
            .get(&price)
            .expect("best ask price must exist")
            .queue
            .front()
            .expect("price level must contain an order");
        let maker = self
            .resting_orders
            .get_mut(&maker_id)
            .expect("queued ID must point to a resting order");
        let filled = (*remaining).min(maker.remaining);
        maker.remaining -= filled;
        *remaining -= filled;
        trades.push(Trade {
            maker_id,
            taker_id,
            price,
            quantity: filled,
        });

        if maker.remaining == 0 {
            self.remove_filled_front(Side::Sell, price, maker_id);
        }
    }

    fn match_bid(
        &mut self,
        taker_id: OrderId,
        price: Price,
        remaining: &mut Quantity,
        trades: &mut Vec<Trade>,
    ) {
        let maker_id = *self
            .bids
            .get(&price)
            .expect("best bid price must exist")
            .queue
            .front()
            .expect("price level must contain an order");
        let maker = self
            .resting_orders
            .get_mut(&maker_id)
            .expect("queued ID must point to a resting order");
        let filled = (*remaining).min(maker.remaining);
        maker.remaining -= filled;
        *remaining -= filled;
        trades.push(Trade {
            maker_id,
            taker_id,
            price,
            quantity: filled,
        });

        if maker.remaining == 0 {
            self.remove_filled_front(Side::Buy, price, maker_id);
        }
    }

    fn remove_filled_front(&mut self, side: Side, price: Price, id: OrderId) {
        self.resting_orders.remove(&id);
        let levels = match side {
            Side::Buy => &mut self.bids,
            Side::Sell => &mut self.asks,
        };
        let level = levels
            .get_mut(&price)
            .expect("filled order's price level must exist");
        let popped = level.queue.pop_front();
        debug_assert_eq!(popped, Some(id));
        if level.queue.is_empty() {
            levels.remove(&price);
        }
    }

    fn level(&self, price: Price, level: &PriceLevel) -> Level {
        let quantity = level
            .queue
            .iter()
            .map(|id| self.resting_orders[id].remaining)
            .sum();
        Level {
            price,
            quantity,
            order_count: level.queue.len(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn order(id: OrderId, side: Side, price: Price, quantity: Quantity) -> LimitOrder {
        LimitOrder {
            id,
            side,
            price,
            quantity,
        }
    }

    #[test]
    fn matches_in_price_then_time_priority() {
        let mut book = OrderBook::new();
        book.submit(order(1, Side::Sell, NonZero::new(101).unwrap(), 5))
            .unwrap();
        book.submit(order(2, Side::Sell, NonZero::new(101).unwrap(), 4))
            .unwrap();
        book.submit(order(3, Side::Sell, NonZero::new(102).unwrap(), 7))
            .unwrap();

        let trades = book
            .submit(order(4, Side::Buy, NonZero::new(102).unwrap(), 10))
            .unwrap();

        assert_eq!(
            trades,
            vec![
                Trade {
                    maker_id: 1,
                    taker_id: 4,
                    price: NonZero::new(101).unwrap(),
                    quantity: 5
                },
                Trade {
                    maker_id: 2,
                    taker_id: 4,
                    price: NonZero::new(101).unwrap(),
                    quantity: 4
                },
                Trade {
                    maker_id: 3,
                    taker_id: 4,
                    price: NonZero::new(102).unwrap(),
                    quantity: 1
                },
            ]
        );
        assert_eq!(
            book.best_ask(),
            Some(Level {
                price: NonZero::new(102).unwrap(),
                quantity: 6,
                order_count: 1
            })
        );
        assert_eq!(book.best_bid(), None);
    }

    #[test]
    fn execution_uses_the_resting_orders_price() {
        let mut book = OrderBook::new();
        book.submit(order(1, Side::Buy, NonZero::new(100).unwrap(), 3))
            .unwrap();

        let trades = book
            .submit(order(2, Side::Sell, NonZero::new(99).unwrap(), 2))
            .unwrap();

        assert_eq!(
            trades,
            vec![Trade {
                maker_id: 1,
                taker_id: 2,
                price: NonZero::new(100).unwrap(),
                quantity: 2
            }]
        );
        assert_eq!(
            book.best_bid(),
            Some(Level {
                price: NonZero::new(100).unwrap(),
                quantity: 1,
                order_count: 1
            })
        );
    }

    #[test]
    fn cancel_removes_only_the_requested_order() {
        let mut book = OrderBook::new();
        book.submit(order(1, Side::Buy, NonZero::new(100).unwrap(), 2))
            .unwrap();
        book.submit(order(2, Side::Buy, NonZero::new(100).unwrap(), 5))
            .unwrap();

        assert_eq!(
            book.cancel(1),
            Ok(CancelledOrder {
                id: 1,
                side: Side::Buy,
                price: NonZero::new(100).unwrap(),
                quantity: 2
            })
        );
        assert_eq!(
            book.best_bid(),
            Some(Level {
                price: NonZero::new(100).unwrap(),
                quantity: 5,
                order_count: 1
            })
        );
        assert_eq!(book.cancel(1), Err(BookError::UnknownOrderId(1)));
    }

    #[test]
    fn rejects_invalid_or_reused_ids() {
        let mut book = OrderBook::new();
        assert_eq!(
            book.submit(order(1, Side::Buy, NonZero::new(100).unwrap(), 0)),
            Err(BookError::ZeroQuantity)
        );
        book.submit(order(1, Side::Buy, NonZero::new(100).unwrap(), 1))
            .unwrap();
        assert_eq!(
            book.submit(order(1, Side::Sell, NonZero::new(100).unwrap(), 1)),
            Err(BookError::DuplicateOrderId(1))
        );
    }
}
