use crate::pool::DEFAULT_POOL_SIZE;
use std::time::Duration;

/// Default threshold at which transaction state is fully reset.
pub(crate) const DEFAULT_RESET_THRESHOLD: usize = 100;

/// Default interval at which garbage collection is performed. With the
/// `gc_floor` barrier in `register_counter`, the sweeper is race-free
/// against concurrent reader registration, so we can run it frequently
/// to keep memory bounded.
pub(crate) const DEFAULT_GC_INTERVAL: Duration = Duration::from_millis(500);

/// Default number of garbage collector wake-ups between full datastore
/// scans. Each wake-up always drains the dirty-key queue; the full scan
/// runs every Nth wake-up (`1` = every tick, `4` = every fourth tick).
/// With a 500ms `gc_interval` this is one full scan per 10s.
pub(crate) const DEFAULT_GC_FULL_SCAN_FREQUENCY: u64 = 20;

/// Default interval at which transaction queue cleanup is performed.
pub(crate) const DEFAULT_CLEANUP_INTERVAL: Duration = Duration::from_secs(1);

/// Default interval at which the timestamp oracle resyncs with the system
/// clock.
pub(crate) const DEFAULT_RESYNC_INTERVAL: Duration = Duration::from_secs(5);

/// Configuration options for [`crate::Database`].
#[derive(Debug, Clone)]
pub struct DatabaseOptions {
	/// Maximum number of transactions kept in the pool (default: 512).
	pub pool_size: usize,
	/// Whether the garbage collector is started automatically (default: true).
	pub enable_gc: bool,
	/// Interval at which the garbage collector wakes up (default: 500ms).
	pub gc_interval: Duration,
	/// Number of garbage collector wake-ups between full datastore scans
	/// (default: 20, i.e. one full scan per 10 seconds at the default
	/// `gc_interval`). Each wake-up always drains the dirty-key queue;
	/// every Nth wake-up additionally walks the whole tree to reclaim
	/// versions on keys that haven't been touched recently.
	pub gc_full_scan_frequency: u64,
	/// Whether the cleanup worker is started automatically (default: true).
	pub enable_cleanup: bool,
	/// Interval at which the cleanup worker wakes up (default: 1 second).
	pub cleanup_interval: Duration,
	/// Threshold after which transaction state maps are reset (default: 100).
	pub reset_threshold: usize,
	/// Interval at which the timestamp oracle resyncs with the system clock
	/// (default: 5 seconds).
	pub resync_interval: Duration,
}

impl Default for DatabaseOptions {
	fn default() -> Self {
		Self {
			pool_size: DEFAULT_POOL_SIZE,
			enable_gc: true,
			gc_interval: DEFAULT_GC_INTERVAL,
			gc_full_scan_frequency: DEFAULT_GC_FULL_SCAN_FREQUENCY,
			enable_cleanup: true,
			cleanup_interval: DEFAULT_CLEANUP_INTERVAL,
			reset_threshold: DEFAULT_RESET_THRESHOLD,
			resync_interval: DEFAULT_RESYNC_INTERVAL,
		}
	}
}

impl DatabaseOptions {
	/// Create a new `DatabaseOptions` instance with default settings.
	pub fn new() -> Self {
		Self::default()
	}

	/// Set the maximum number of transactions kept in the pool.
	pub fn with_pool_size(mut self, pool_size: usize) -> Self {
		self.pool_size = pool_size;
		self
	}

	/// Set whether the garbage collector thread is started automatically.
	pub fn with_enable_gc(mut self, enable: bool) -> Self {
		self.enable_gc = enable;
		self
	}

	/// Set the interval at which the garbage collector wakes up.
	pub fn with_gc_interval(mut self, interval: Duration) -> Self {
		self.gc_interval = interval;
		self
	}

	/// Set how many garbage collector wake-ups elapse between full
	/// datastore scans. `1` runs a full scan every tick; higher values
	/// scan less often but rely more on the dirty-key queue.
	pub fn with_gc_full_scan_frequency(mut self, frequency: u64) -> Self {
		self.gc_full_scan_frequency = frequency.max(1);
		self
	}

	/// Set whether the cleanup worker thread is started automatically.
	pub fn with_enable_cleanup(mut self, enable: bool) -> Self {
		self.enable_cleanup = enable;
		self
	}

	/// Set the interval at which the cleanup worker wakes up.
	pub fn with_cleanup_interval(mut self, interval: Duration) -> Self {
		self.cleanup_interval = interval;
		self
	}

	/// Set the threshold after which transaction state maps are reset.
	pub fn with_reset_threshold(mut self, threshold: usize) -> Self {
		self.reset_threshold = threshold;
		self
	}

	/// Set the interval at which the timestamp oracle resyncs with the system
	/// clock.
	pub fn with_resync_interval(mut self, interval: Duration) -> Self {
		self.resync_interval = interval;
		self
	}

	/// Disable all background worker threads (gc and cleanup).
	pub fn with_all_workers_disabled(mut self) -> Self {
		self.enable_gc = false;
		self.enable_cleanup = false;
		self
	}

	/// Configure for high-throughput scenarios with a larger transaction
	/// pool and less-frequent background garbage collection. Trades
	/// memory for throughput: more dropped transactions are kept for
	/// reuse, and the gc sweeper interrupts the foreground less often.
	pub fn with_high_performance(mut self) -> Self {
		self.pool_size *= 2;
		self.gc_interval = Duration::from_secs(1);
		self.gc_full_scan_frequency = 30;
		self.cleanup_interval = Duration::from_millis(100);
		self.reset_threshold *= 2;
		self.resync_interval = Duration::from_secs(2);
		self
	}

	/// Configure for low-CPU scenarios (embedded / battery-powered)
	/// with slower background workers. Trades memory and reclamation
	/// latency for minimal background CPU.
	pub fn with_low_resource(mut self) -> Self {
		self.pool_size /= 2;
		self.gc_interval = Duration::from_secs(5);
		self.gc_full_scan_frequency = 12;
		self.cleanup_interval = Duration::from_millis(500);
		self.reset_threshold /= 2;
		self.resync_interval = Duration::from_secs(10);
		self
	}

	/// Configure for memory-constrained scenarios: aggressive garbage
	/// collection and a smaller transaction pool. Trades CPU for tighter
	/// memory bounds — version reclamation runs more often, full
	/// datastore scans happen more often, and fewer dropped transactions
	/// are kept around for reuse.
	pub fn with_reduced_memory(mut self) -> Self {
		self.pool_size /= 2;
		self.gc_interval = Duration::from_millis(100);
		self.gc_full_scan_frequency = 5;
		self.cleanup_interval = Duration::from_millis(100);
		self.reset_threshold /= 2;
		self
	}
}
