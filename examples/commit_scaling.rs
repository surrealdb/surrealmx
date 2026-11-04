//! Hammers concurrent commits of small, disjoint writes and reports the
//! commit throughput for a range of thread counts.
//!
//! Each thread count runs for a fixed time, so a pathological build cannot
//! hold the machine for long.
//!
//! Run with `cargo run --release --example commit_scaling -- [seconds] [threads...]`.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};
use surrealmx::Database;

#[allow(clippy::cast_precision_loss, reason = "a throughput figure for display")]
fn run(threads: usize, duration: Duration) -> f64 {
	let db = Arc::new(Database::new());
	let barrier = Arc::new(Barrier::new(threads + 1));
	let stop = Arc::new(AtomicBool::new(false));
	let total = Arc::new(AtomicU64::new(0));
	let handles: Vec<_> = (0..threads)
		.map(|t| {
			let db = Arc::clone(&db);
			let barrier = Arc::clone(&barrier);
			let stop = Arc::clone(&stop);
			let total = Arc::clone(&total);
			std::thread::spawn(move || {
				barrier.wait();
				let mut i = 0u64;
				while !stop.load(Ordering::Relaxed) {
					let mut tx = db.transaction(true);
					tx.put(format!("key_{t:03}_{i:010}"), vec![b'v'; 64]).unwrap();
					tx.commit().expect("disjoint commits must not conflict");
					i += 1;
				}
				total.fetch_add(i, Ordering::Relaxed);
			})
		})
		.collect();
	barrier.wait();
	let start = Instant::now();
	std::thread::sleep(duration);
	stop.store(true, Ordering::Relaxed);
	for h in handles {
		h.join().unwrap();
	}
	total.load(Ordering::Relaxed) as f64 / start.elapsed().as_secs_f64()
}

fn main() {
	let args: Vec<usize> = std::env::args().skip(1).filter_map(|a| a.parse().ok()).collect();
	let secs = args.first().copied().unwrap_or(3);
	let counts = if args.len() > 1 {
		args[1..].to_vec()
	} else {
		vec![1, 4, 16, 32, 48, 64]
	};
	for t in counts {
		let rate = run(t, Duration::from_secs(secs as u64));
		println!("threads={t:4} commits/s={rate:.0}");
	}
}
