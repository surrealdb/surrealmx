// Copyright © SurrealDB Ltd
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Deterministic Simulation & Differential Testing Runner.

use crate::{Database, DatabaseOptions, Transaction};
use byteslice::ByteSlice;
use std::collections::HashMap;

use super::generator::{SimAction, WorkloadGenerator};
use super::model::{ModelDb, ModelIsolation, ModelTxn};

/// The simulation test runner coordinating differential execution.
pub struct SimRunner {
	db: Option<Database>,
	model: ModelDb,
	active_db_txns: HashMap<usize, Transaction>,
	active_model_txns: HashMap<usize, ModelTxn>,
	seed: u64,
	step: usize,
	#[cfg(not(target_arch = "wasm32"))]
	temp_dir: Option<tempfile::TempDir>,
	#[cfg(not(target_arch = "wasm32"))]
	persistence_opts: Option<crate::PersistenceOptions>,
}

impl SimRunner {
	/// Creates a new in-memory runner with default options.
	pub fn new_in_memory(seed: u64) -> Self {
		let db = Database::new_with_options(DatabaseOptions::default().with_all_workers_disabled());
		Self {
			db: Some(db),
			model: ModelDb::new(),
			active_db_txns: HashMap::new(),
			active_model_txns: HashMap::new(),
			seed,
			step: 0,
			#[cfg(not(target_arch = "wasm32"))]
			temp_dir: None,
			#[cfg(not(target_arch = "wasm32"))]
			persistence_opts: None,
		}
	}

	/// Creates a new persistent runner backed by disk.
	#[cfg(not(target_arch = "wasm32"))]
	pub fn new_persistent(
		seed: u64,
		aol_mode: crate::AolMode,
		fsync_mode: crate::FsyncMode,
	) -> Self {
		let temp_dir = tempfile::tempdir().expect("failed to create temp dir for persistence");
		let persistence_opts = crate::PersistenceOptions::new(temp_dir.path())
			.with_aol_mode(aol_mode)
			.with_fsync_mode(fsync_mode);
		let db = Database::new_with_persistence(
			DatabaseOptions::default().with_all_workers_disabled(),
			persistence_opts.clone(),
		)
		.expect("failed to create persistent database");

		Self {
			db: Some(db),
			model: ModelDb::new(),
			active_db_txns: HashMap::new(),
			active_model_txns: HashMap::new(),
			seed,
			step: 0,
			temp_dir: Some(temp_dir),
			persistence_opts: Some(persistence_opts),
		}
	}

	/// Returns a reference to the `SurrealMX` database.
	pub const fn db(&self) -> &Database {
		self.db.as_ref().expect("database not open")
	}

	/// Returns the persistence temporary directory path if enabled.
	#[cfg(not(target_arch = "wasm32"))]
	pub fn temp_dir(&self) -> Option<&std::path::Path> {
		self.temp_dir.as_ref().map(tempfile::TempDir::path)
	}

	/// Executes N simulation steps driven by the workload generator.
	pub fn run(&mut self, mut gen: WorkloadGenerator, steps: usize) {
		for _ in 0..steps {
			self.step += 1;
			let action = gen.next_action();
			self.execute_action(action);
		}

		// Final check: drain remaining active transactions and assert full
		// datastore equivalence
		self.finish_and_verify();
	}

	/// Executes a single simulation action against both systems and asserts
	/// equivalence.
	pub fn execute_action(&mut self, action: SimAction) {
		let seed = self.seed;
		let step = self.step;

		match action {
			SimAction::Begin {
				txn_id,
				write,
				mode,
			} => {
				let db_tx = self.db().transaction(write);
				let db_tx = match mode {
					ModelIsolation::SnapshotIsolation => db_tx.with_snapshot_isolation(),
					ModelIsolation::SerializableSnapshotIsolation => {
						db_tx.with_serializable_snapshot_isolation()
					}
				};
				let model_tx = self.model.begin(write, mode);
				self.active_db_txns.insert(txn_id, db_tx);
				self.active_model_txns.insert(txn_id, model_tx);
			}

			SimAction::Get {
				txn_id,
				key,
			} => {
				let db_tx = &self.active_db_txns[&txn_id];
				let model_tx = self.active_model_txns.get_mut(&txn_id).unwrap();

				let db_val = db_tx.get(&key).expect("db get failed");
				let model_val = model_tx.get(&self.model, key.as_ref()).expect("model get failed");

				assert_eq!(
					db_val,
					model_val,
					"Divergence in Get at seed {seed}, step {step} for txn {txn_id} on key {:?}",
					String::from_utf8_lossy(&key)
				);
			}

			SimAction::GetForUpdate {
				txn_id,
				key,
			} => {
				let db_tx = self.active_db_txns.get_mut(&txn_id).unwrap();
				let model_tx = self.active_model_txns.get_mut(&txn_id).unwrap();

				let db_val = db_tx.get_for_update(&key).expect("db get_for_update failed");
				let model_val = model_tx
					.get_for_update(&self.model, key.as_ref())
					.expect("model get_for_update failed");

				assert_eq!(
					db_val, model_val,
					"Divergence in GetForUpdate at seed {seed}, step {step} for txn {txn_id} on key {:?}",
					String::from_utf8_lossy(&key)
				);
			}

			SimAction::Exists {
				txn_id,
				key,
			} => {
				let db_tx = &self.active_db_txns[&txn_id];
				let model_tx = self.active_model_txns.get_mut(&txn_id).unwrap();

				let db_exists = db_tx.exists(&key).expect("db exists failed");
				let model_exists =
					model_tx.exists(&self.model, key.as_ref()).expect("model exists failed");

				assert_eq!(
					db_exists,
					model_exists,
					"Divergence in Exists at seed {seed}, step {step} for txn {txn_id} on key {:?}",
					String::from_utf8_lossy(&key)
				);
			}

			SimAction::Set {
				txn_id,
				key,
				val,
			} => {
				let db_tx = self.active_db_txns.get_mut(&txn_id).unwrap();
				let model_tx = self.active_model_txns.get_mut(&txn_id).unwrap();

				let db_res = db_tx.set(&key, &val);
				let model_res = model_tx.set(key.as_ref(), val.as_ref());

				assert_eq!(
					db_res.is_ok(),
					model_res.is_ok(),
					"Divergence in Set at seed {seed}, step {step} for txn {txn_id}"
				);
			}

			SimAction::Put {
				txn_id,
				key,
				val,
			} => {
				let db_tx = self.active_db_txns.get_mut(&txn_id).unwrap();
				let model_tx = self.active_model_txns.get_mut(&txn_id).unwrap();

				let db_res = db_tx.put(&key, &val);
				let model_res = model_tx.put(&self.model, key.as_ref(), val.as_ref());

				assert_eq!(
					db_res.is_ok(),
					model_res.is_ok(),
					"Divergence in Put result at seed {seed}, step {step} for txn {txn_id}"
				);
			}

			SimAction::PutC {
				txn_id,
				key,
				val,
				chk,
			} => {
				let db_tx = self.active_db_txns.get_mut(&txn_id).unwrap();
				let model_tx = self.active_model_txns.get_mut(&txn_id).unwrap();

				let db_res = db_tx.putc(&key, &val, chk.as_ref());
				let model_res =
					model_tx.putc(&self.model, key.as_ref(), val.as_ref(), chk.as_deref());

				assert_eq!(
					db_res.is_ok(),
					model_res.is_ok(),
					"Divergence in PutC result at seed {seed}, step {step} for txn {txn_id}"
				);
			}

			SimAction::Del {
				txn_id,
				key,
			} => {
				let db_tx = self.active_db_txns.get_mut(&txn_id).unwrap();
				let model_tx = self.active_model_txns.get_mut(&txn_id).unwrap();

				let db_res = db_tx.del(&key);
				let model_res = model_tx.del(key.as_ref());

				assert_eq!(
					db_res.is_ok(),
					model_res.is_ok(),
					"Divergence in Del at seed {seed}, step {step} for txn {txn_id}"
				);
			}

			SimAction::DelC {
				txn_id,
				key,
				chk,
			} => {
				let db_tx = self.active_db_txns.get_mut(&txn_id).unwrap();
				let model_tx = self.active_model_txns.get_mut(&txn_id).unwrap();

				let db_res = db_tx.delc(&key, chk.as_ref());
				let model_res = model_tx.delc(&self.model, key.as_ref(), chk.as_deref());

				assert_eq!(
					db_res.is_ok(),
					model_res.is_ok(),
					"Divergence in DelC result at seed {seed}, step {step} for txn {txn_id}"
				);
			}

			SimAction::SetSavepoint {
				txn_id,
			} => {
				let db_tx = self.active_db_txns.get_mut(&txn_id).unwrap();
				let model_tx = self.active_model_txns.get_mut(&txn_id).unwrap();

				let db_res = db_tx.set_savepoint();
				let model_res = model_tx.set_savepoint();

				assert_eq!(
					db_res.is_ok(),
					model_res.is_ok(),
					"Divergence in SetSavepoint at seed {seed}, step {step} for txn {txn_id}"
				);
			}

			SimAction::RollbackSavepoint {
				txn_id,
			} => {
				let db_tx = self.active_db_txns.get_mut(&txn_id).unwrap();
				let model_tx = self.active_model_txns.get_mut(&txn_id).unwrap();

				let db_res = db_tx.rollback_to_savepoint();
				let model_res = model_tx.rollback_to_savepoint();

				assert_eq!(
					db_res.is_ok(),
					model_res.is_ok(),
					"Divergence in RollbackSavepoint at seed {seed}, step {step} for txn {txn_id}"
				);
			}

			SimAction::ReleaseSavepoint {
				txn_id,
			} => {
				let db_tx = self.active_db_txns.get_mut(&txn_id).unwrap();
				let model_tx = self.active_model_txns.get_mut(&txn_id).unwrap();

				let db_res = db_tx.release_savepoint();
				let model_res = model_tx.release_savepoint();

				assert_eq!(
					db_res.is_ok(),
					model_res.is_ok(),
					"Divergence in ReleaseSavepoint at seed {seed}, step {step} for txn {txn_id}"
				);
			}

			SimAction::Scan {
				txn_id,
				start,
				end,
				skip,
				limit,
				reverse,
			} => {
				let db_tx = &self.active_db_txns[&txn_id];
				let model_tx = self.active_model_txns.get_mut(&txn_id).unwrap();

				let db_res = if reverse {
					db_tx
						.scan_reverse(start.clone()..end.clone(), skip, limit)
						.expect("db scan failed")
				} else {
					db_tx.scan(start.clone()..end.clone(), skip, limit).expect("db scan failed")
				};

				let model_res = model_tx
					.scan(&self.model, start.as_ref(), end.as_ref(), skip, limit, reverse)
					.expect("model scan failed");

				assert_eq!(
					db_res, model_res,
					"Divergence in Scan at seed {seed}, step {step} for txn {txn_id} range {:?}..{:?} (reverse: {reverse}, skip: {skip:?}, limit: {limit:?})",
					String::from_utf8_lossy(&start),
					String::from_utf8_lossy(&end)
				);
			}

			SimAction::DirectPointRead {
				key,
			} => {
				let db_val = self.db().get(&key).expect("direct get failed");
				let model_val = self.model.get_at_version(key.as_ref(), self.model.current_version);

				assert_eq!(
					db_val,
					model_val,
					"Divergence in DirectPointRead at seed {seed}, step {step} for key {:?}",
					String::from_utf8_lossy(&key)
				);

				let with_val = self
					.db()
					.with_value(&key, |b| ByteSlice::from(b))
					.expect("direct with_value failed");
				assert_eq!(
					with_val,
					model_val,
					"Divergence in DirectPointRead with_value at seed {seed}, step {step} for key {:?}",
					String::from_utf8_lossy(&key)
				);
			}

			SimAction::DirectSet {
				key,
				val,
			} => {
				let db_res = self.db().set(&key, &val);
				self.model.set_direct(key.clone(), val);
				assert!(
					db_res.is_ok(),
					"DirectSet failed at seed {seed}, step {step} for key {:?}",
					String::from_utf8_lossy(&key)
				);
			}

			SimAction::DirectDel {
				key,
			} => {
				let db_res = self.db().del(&key);
				self.model.del_direct(key.clone());
				assert!(
					db_res.is_ok(),
					"DirectDel failed at seed {seed}, step {step} for key {:?}",
					String::from_utf8_lossy(&key)
				);
			}

			SimAction::DirectScan {
				start,
				end,
				skip,
				limit,
				reverse,
			} => {
				let tx = self.db().transaction(false);
				let db_res = if reverse {
					tx.scan_reverse(start.clone()..end.clone(), skip, limit)
						.expect("direct scan failed")
				} else {
					tx.scan(start.clone()..end.clone(), skip, limit).expect("direct scan failed")
				};

				let mut model_res = self.model.scan_at_version(
					start.as_ref(),
					end.as_ref(),
					self.model.current_version,
				);
				if reverse {
					model_res.reverse();
				}
				let skip_n = skip.unwrap_or(0);
				let limit_n = limit.unwrap_or(usize::MAX);
				let model_res: Vec<(ByteSlice, ByteSlice)> =
					model_res.into_iter().skip(skip_n).take(limit_n).collect();

				assert_eq!(
					db_res, model_res,
					"Divergence in DirectScan at seed {seed}, step {step} range {:?}..{:?} (reverse: {reverse}, skip: {skip:?}, limit: {limit:?})",
					String::from_utf8_lossy(&start),
					String::from_utf8_lossy(&end)
				);
			}

			SimAction::Commit {
				txn_id,
			} => {
				let mut db_tx = self.active_db_txns.remove(&txn_id).unwrap();
				let model_tx = self.active_model_txns.remove(&txn_id).unwrap();

				let db_res = db_tx.commit();
				let model_res = self.model.commit(model_tx);

				match (db_res.is_ok(), model_res.is_ok()) {
					(true, true) | (false, false) => {
						// Both succeeded or both failed due to conflicts
					}
					(true, false) => {
						panic!(
							"DIVERGENCE at seed {seed}, step {step}: SurrealMX committed txn {txn_id} but ModelDb detected conflict: {:?}",
							model_res.err()
						);
					}
					(false, true) => {
						panic!(
							"DIVERGENCE at seed {seed}, step {step}: ModelDb committed txn {txn_id} but SurrealMX rejected commit: {:?}",
							db_res.err()
						);
					}
				}
			}

			SimAction::Cancel {
				txn_id,
			} => {
				let mut db_tx = self.active_db_txns.remove(&txn_id).unwrap();
				let _ = self.active_model_txns.remove(&txn_id).unwrap();
				let _ = db_tx.cancel();
			}

			SimAction::MaintenanceCleanup => {
				self.db().run_cleanup();
			}

			SimAction::MaintenanceGc => {
				self.db().run_gc();
				self.db().run_gc_tracked();
			}

			SimAction::CrashAndReload => {
				#[cfg(not(target_arch = "wasm32"))]
				{
					// Drop all in-flight uncommitted transactions
					self.active_db_txns.clear();
					self.active_model_txns.clear();

					// Close database
					drop(self.db.take());

					// Reopen database from persistence
					if let Some(ref opts) = self.persistence_opts {
						let db = Database::new_with_persistence(
							DatabaseOptions::default().with_all_workers_disabled(),
							opts.clone(),
						)
						.expect("failed to reload persistent database");
						self.db = Some(db);
					} else {
						panic!("CrashAndReload invoked without persistence options");
					}
				}
			}
		}
	}

	/// Finishes the simulation run by committing/canceling remaining active
	/// transactions and asserting complete datastore state equality against
	/// `ModelDb`.
	pub fn finish_and_verify(&mut self) {
		// Cancel all remaining in-flight transactions
		for (_, mut tx) in self.active_db_txns.drain() {
			let _ = tx.cancel();
		}
		self.active_model_txns.clear();

		// Check full range scan across entire key space
		let tx = self.db().transaction(false);
		let db_all = tx
			.scan(b"".as_slice()..b"\xff\xff\xff\xff".as_slice(), None, None)
			.expect("final full scan failed");

		let model_all =
			self.model.scan_at_version(b"", b"\xff\xff\xff\xff", self.model.current_version);

		assert_eq!(
			db_all, model_all,
			"Final verification divergence at seed {}! Committed keys in SurrealMX differ from ModelDb.",
			self.seed
		);
	}
}
