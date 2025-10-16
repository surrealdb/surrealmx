use crate::pool::DEFAULT_POOL_SIZE;
use std::time::Duration;

/// Default threshold at which transaction state is fully reset.
pub const DEFAULT_RESET_THRESHOLD: usize = 100;

/// Default interval at which garbage collection is performed.
pub const DEFAULT_GC_INTERVAL: Duration = Duration::from_secs(60);

/// Default interval at which transaction queue cleanup is performed.
pub const DEFAULT_CLEANUP_INTERVAL: Duration = Duration::from_millis(250);

/// Default interval at which the timestamp oracle resyncs with the system clock.
pub const DEFAULT_RESYNC_INTERVAL: Duration = Duration::from_secs(5);

/// Configuration options for [`crate::Database`].
#[derive(Debug, Clone)]
pub struct DatabaseOptions {
	/// Maximum number of transactions kept in the pool.
	pub pool_size: usize,
	/// Whether the garbage collector thread is started automatically.
	pub enable_gc: bool,
	/// Interval at which the garbage collector wakes up.
	pub gc_interval: Duration,
	/// Whether the cleanup worker thread is started automatically.
	pub enable_cleanup: bool,
	/// Interval at which the cleanup worker wakes up.
	pub cleanup_interval: Duration,
	/// Threshold after which transaction state maps are reset.
	pub reset_threshold: usize,
	/// Interval at which the timestamp oracle resyncs with the system clock.
	pub resync_interval: Duration,
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

	/// Set the interval at which the timestamp oracle resyncs with the system clock.
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

	/// Configure for high-performance scenarios with faster intervals and larger thresholds.
	pub fn with_high_performance(mut self) -> Self {
		self.pool_size *= 2;
		self.gc_interval = Duration::from_secs(30);
		self.cleanup_interval = Duration::from_millis(100);
		self.reset_threshold *= 2;
		self.resync_interval = Duration::from_secs(2);
		self
	}

	/// Configure for low-resource scenarios with slower intervals and smaller thresholds.
	pub fn with_low_resource(mut self) -> Self {
		self.pool_size /= 2;
		self.gc_interval = Duration::from_secs(120);
		self.cleanup_interval = Duration::from_millis(500);
		self.reset_threshold /= 2;
		self.resync_interval = Duration::from_secs(10);
		self
	}
}
