use crate::pool::DEFAULT_POOL_SIZE;
use std::time::Duration;

/// Default threshold at which transaction state is fully reset.
pub(crate) const DEFAULT_RESET_THRESHOLD: usize = 100;

/// Default interval at which the background garbage collection sweep is
/// performed. Steady-state reclamation happens inline at commit time,
/// and every chain a commit cannot trim to a single live value is
/// tracked as a garbage candidate — so each background tick sweeps only
/// the tracked keys, and its cost scales with the amount of
/// reader-pinned garbage rather than the dataset size.
pub(crate) const DEFAULT_GC_INTERVAL: Duration = Duration::from_secs(10);

/// Default interval at which transaction queue cleanup is performed.
pub(crate) const DEFAULT_CLEANUP_INTERVAL: Duration = Duration::from_secs(1);

/// Configuration options for [`crate::Database`].
#[derive(Debug, Clone)]
pub struct DatabaseOptions {
	/// Maximum number of transactions kept in the pool (default: 512).
	pub pool_size: usize,
	/// Whether the background garbage collection sweep is started
	/// automatically (default: true). Steady-state reclamation happens
	/// inline at commit time regardless of this setting; the background
	/// sweep reclaims the tracked leftovers — chains a commit could not
	/// fully trim because a reader was pinning older versions.
	pub enable_gc: bool,
	/// Interval at which the garbage collection sweep wakes up and
	/// visits the tracked garbage-candidate keys (default: 10 seconds).
	/// A tick costs proportional to the number of tracked keys, not the
	/// dataset size, so short intervals are affordable.
	pub gc_interval: Duration,
	/// Whether the cleanup worker is started automatically (default: true).
	pub enable_cleanup: bool,
	/// Interval at which the cleanup worker wakes up (default: 1 second).
	pub cleanup_interval: Duration,
	/// Threshold after which transaction state maps are reset (default: 100).
	pub reset_threshold: usize,
}

impl Default for DatabaseOptions {
	fn default() -> Self {
		Self {
			pool_size: DEFAULT_POOL_SIZE,
			enable_gc: true,
			gc_interval: DEFAULT_GC_INTERVAL,
			enable_cleanup: true,
			cleanup_interval: DEFAULT_CLEANUP_INTERVAL,
			reset_threshold: DEFAULT_RESET_THRESHOLD,
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

	/// Disable all background worker threads (gc and cleanup).
	pub fn with_all_workers_disabled(mut self) -> Self {
		self.enable_gc = false;
		self.enable_cleanup = false;
		self
	}

	/// Configure for high-throughput scenarios with a larger transaction
	/// pool and less-frequent background sweeps. Trades memory for
	/// throughput: more dropped transactions are kept for reuse, and
	/// reader-pinned garbage waits longer for its tracked sweep —
	/// steady-state reclamation happens inline at commit time either way.
	pub fn with_high_performance(mut self) -> Self {
		self.pool_size *= 2;
		self.gc_interval = Duration::from_secs(30);
		self.cleanup_interval = Duration::from_millis(100);
		self.reset_threshold *= 2;
		self
	}

	/// Configure for low-CPU scenarios (embedded / battery-powered)
	/// with slower background workers. Trades memory and reclamation
	/// latency for minimal background CPU.
	pub fn with_low_resource(mut self) -> Self {
		self.pool_size /= 2;
		self.gc_interval = Duration::from_secs(60);
		self.cleanup_interval = Duration::from_millis(500);
		self.reset_threshold /= 2;
		self
	}

	/// Configure for memory-constrained scenarios: frequent tracked
	/// sweeps and a smaller transaction pool. Reader-pinned garbage is
	/// reclaimed sooner after the reader departs, and fewer dropped
	/// transactions are kept around for reuse. Since a sweep visits only
	/// the tracked candidate keys, the extra ticks cost little CPU even
	/// on large datasets.
	pub fn with_reduced_memory(mut self) -> Self {
		self.pool_size /= 2;
		self.gc_interval = Duration::from_secs(5);
		self.cleanup_interval = Duration::from_millis(100);
		self.reset_threshold /= 2;
		self
	}
}
