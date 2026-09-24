use std::{
    fs::{self, File, OpenOptions},
    hint::black_box,
    num::NonZero,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};

use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use replay::{
    LimitOrder, OrderBook, Side,
    observe::{BufferedFileObserver, MemoryMappedFileObserver, Observer},
};

const TRADE_COUNT: u64 = 10_000;
const MMAP_INITIAL_SIZE: usize = 1024 * 1024;
static NEXT_FILE_ID: AtomicU64 = AtomicU64::new(0);

fn submit_crossing_orders(book: &mut OrderBook) {
    let price = NonZero::new(100).unwrap();

    for trade_number in 0..TRADE_COUNT {
        let sell_id = trade_number * 2 + 1;
        let buy_id = sell_id + 1;
        book.submit(LimitOrder {
            id: sell_id,
            side: Side::Sell,
            price,
            quantity: 1,
        })
        .unwrap();

        let trades = book
            .submit(LimitOrder {
                id: buy_id,
                side: Side::Buy,
                price,
                quantity: 1,
            })
            .unwrap();
        assert_eq!(trades.len(), 1);
        black_box(trades);
    }
}

fn run_with_observer(observer: impl Observer + 'static) {
    let mut book = OrderBook::new_with_observer(observer);
    submit_crossing_orders(&mut book);
    drop(book);
}

fn benchmark_file_path(observer_name: &str) -> PathBuf {
    let id = NEXT_FILE_ID.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "replay-{observer_name}-bench-{}-{id}.log",
        std::process::id()
    ))
}

fn observer_benchmarks(c: &mut Criterion) {
    let mut group = c.benchmark_group("observers");
    group.throughput(Throughput::Elements(TRADE_COUNT));

    group.bench_function(BenchmarkId::new("no_observer", TRADE_COUNT), |b| {
        b.iter(|| {
            let mut book = OrderBook::new();
            submit_crossing_orders(&mut book);
            black_box(book);
        });
    });

    let buffered_path = benchmark_file_path("buffered");
    group.bench_function(BenchmarkId::new("buffered_file", TRADE_COUNT), |b| {
        b.iter_batched(
            || File::create(&buffered_path).unwrap(),
            |file| run_with_observer(BufferedFileObserver::new(file)),
            BatchSize::SmallInput,
        );
    });
    let _ = fs::remove_file(&buffered_path);

    let mmap_path = benchmark_file_path("mmap");
    group.bench_function(BenchmarkId::new("memory_mapped_file", TRADE_COUNT), |b| {
        b.iter_batched(
            || {
                OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .truncate(true)
                    .open(&mmap_path)
                    .unwrap()
            },
            |file| {
                run_with_observer(MemoryMappedFileObserver::new(file, MMAP_INITIAL_SIZE).unwrap())
            },
            BatchSize::SmallInput,
        );
    });
    let _ = fs::remove_file(&mmap_path);

    group.finish();
}

criterion_group!(benches, observer_benchmarks);
criterion_main!(benches);
