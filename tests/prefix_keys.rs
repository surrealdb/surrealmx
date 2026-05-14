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

//! Tests that exercise the underlying B+tree's split/merge paths with keys
//! that share long prefixes — i.e. the shapes that show up in production
//! (`tb:id`, `ns:db:tb:id:field`, etc.) and that pack many adjacent entries
//! into the same leaf.

#![cfg(not(target_arch = "wasm32"))]

use bytes::Bytes;
use rand::seq::SliceRandom;
use rand::SeedableRng;
use surrealmx::Database;

/// Number of entries large enough to force many leaf splits. Ferntree's
/// LC=64 default means anything above ~256 entries gets multiple levels.
const LARGE: usize = 5_000;

fn pkey(prefix: &str, n: usize) -> Vec<u8> {
	format!("{prefix}:{n:010}").into_bytes()
}

/// Inclusive upper-bound sentinel for a given key prefix (all 0xFF suffix).
fn prefix_end(prefix: &str) -> Vec<u8> {
	let mut v = prefix.as_bytes().to_vec();
	v.push(0xFF);
	v
}

#[test]
fn sequential_inserts_with_shared_prefix() {
	let db = Database::new();
	let mut tx = db.transaction(true);
	for i in 0..LARGE {
		tx.set(pkey("account:user", i), format!("v{i}").into_bytes()).unwrap();
	}
	tx.commit().unwrap();
	// Every key resolvable via point lookup
	let mut tx = db.transaction(false);
	for i in 0..LARGE {
		let v = tx.get(pkey("account:user", i)).unwrap();
		assert_eq!(v, Some(Bytes::from(format!("v{i}"))), "missing key at i={i}");
	}
	// Full forward scan returns them in key order
	let lo: &[u8] = b"account:user:";
	let hi = prefix_end("account:user:");
	let res = tx.scan(lo..hi.as_slice(), None, None).unwrap();
	assert_eq!(res.len(), LARGE);
	for (i, (k, v)) in res.iter().enumerate() {
		assert_eq!(k.as_ref(), pkey("account:user", i).as_slice());
		assert_eq!(v.as_ref(), format!("v{i}").as_bytes());
	}
	tx.cancel().unwrap();
}

#[test]
fn random_order_insert_with_shared_prefix() {
	// Deterministic shuffle so failures are reproducible
	let mut rng = rand::rngs::StdRng::seed_from_u64(0xfeedface);
	let mut indices: Vec<usize> = (0..LARGE).collect();
	indices.shuffle(&mut rng);

	let db = Database::new();
	// Commit in small batches to exercise the merge queue / commit path too
	for chunk in indices.chunks(250) {
		let mut tx = db.transaction(true);
		for &i in chunk {
			tx.set(pkey("account:user", i), format!("v{i}").into_bytes()).unwrap();
		}
		tx.commit().unwrap();
	}

	// Despite random insert order, scan must return them sorted
	let mut tx = db.transaction(false);
	let lo: &[u8] = b"account:user:";
	let hi = prefix_end("account:user:");
	let res = tx.scan(lo..hi.as_slice(), None, None).unwrap();
	assert_eq!(res.len(), LARGE);
	for (i, (k, _)) in res.iter().enumerate() {
		assert_eq!(k.as_ref(), pkey("account:user", i).as_slice());
	}
	tx.cancel().unwrap();
}

#[test]
fn reverse_order_insert_with_shared_prefix() {
	let db = Database::new();
	let mut tx = db.transaction(true);
	for i in (0..LARGE).rev() {
		tx.set(pkey("account:user", i), format!("v{i}").into_bytes()).unwrap();
	}
	tx.commit().unwrap();
	let mut tx = db.transaction(false);
	let lo: &[u8] = b"account:user:";
	let hi = prefix_end("account:user:");
	let res = tx.scan(lo..hi.as_slice(), None, None).unwrap();
	assert_eq!(res.len(), LARGE);
	for (i, (k, _)) in res.iter().enumerate() {
		assert_eq!(k.as_ref(), pkey("account:user", i).as_slice());
	}
	// And in reverse
	let rev = tx.scan_reverse(lo..hi.as_slice(), None, None).unwrap();
	assert_eq!(rev.len(), LARGE);
	for (i, (k, _)) in rev.iter().enumerate() {
		let expected = LARGE - 1 - i;
		assert_eq!(k.as_ref(), pkey("account:user", expected).as_slice());
	}
	tx.cancel().unwrap();
}

#[test]
fn delete_every_other_forces_leaf_merges() {
	let db = Database::new();
	let mut tx = db.transaction(true);
	for i in 0..LARGE {
		tx.set(pkey("k", i), format!("v{i}").into_bytes()).unwrap();
	}
	tx.commit().unwrap();
	// Delete every other entry — leaves drop below fill threshold and merge
	let mut tx = db.transaction(true);
	for i in (0..LARGE).step_by(2) {
		tx.del(pkey("k", i)).unwrap();
	}
	tx.commit().unwrap();
	// Verify remaining entries are exactly the odd indices
	let mut tx = db.transaction(false);
	let lo: &[u8] = b"k:";
	let hi = prefix_end("k:");
	let res = tx.scan(lo..hi.as_slice(), None, None).unwrap();
	assert_eq!(res.len(), LARGE / 2);
	for (idx, (k, _)) in res.iter().enumerate() {
		let expected = idx * 2 + 1;
		assert_eq!(k.as_ref(), pkey("k", expected).as_slice());
	}
	// And the deleted ones are gone via point lookup
	for i in (0..LARGE).step_by(2) {
		assert!(tx.get(pkey("k", i)).unwrap().is_none());
	}
	tx.cancel().unwrap();
}

#[test]
fn delete_majority_then_reinsert_different_keys() {
	let db = Database::new();
	let mut tx = db.transaction(true);
	for i in 0..LARGE {
		tx.set(pkey("k", i), b"v".to_vec()).unwrap();
	}
	tx.commit().unwrap();
	// Delete 80% (the lower 4/5)
	let mut tx = db.transaction(true);
	for i in 0..(LARGE * 4 / 5) {
		tx.del(pkey("k", i)).unwrap();
	}
	tx.commit().unwrap();
	// Insert a different prefix at the same density
	let mut tx = db.transaction(true);
	for i in 0..(LARGE * 4 / 5) {
		tx.set(pkey("new", i), b"v".to_vec()).unwrap();
	}
	tx.commit().unwrap();
	let mut tx = db.transaction(false);
	let k_lo: &[u8] = b"k:";
	let k_hi = prefix_end("k:");
	let k_count = tx.total(k_lo..k_hi.as_slice(), None, None).unwrap();
	let n_lo: &[u8] = b"new:";
	let n_hi = prefix_end("new:");
	let new_count = tx.total(n_lo..n_hi.as_slice(), None, None).unwrap();
	assert_eq!(k_count, LARGE / 5);
	assert_eq!(new_count, LARGE * 4 / 5);
	tx.cancel().unwrap();
}

#[test]
fn multiple_prefix_groups_isolated_scans() {
	let db = Database::new();
	let groups = ["account:1", "account:2", "account:3", "account:10"];
	let per_group = 1_000;
	let mut tx = db.transaction(true);
	for g in &groups {
		for i in 0..per_group {
			tx.set(format!("{g}:post:{i:08}").into_bytes(), b"v".to_vec()).unwrap();
		}
	}
	tx.commit().unwrap();
	let mut tx = db.transaction(false);
	for g in &groups {
		let lo = format!("{g}:post:").into_bytes();
		let hi = prefix_end(&format!("{g}:post:"));
		let res = tx.scan(lo.as_slice()..hi.as_slice(), None, None).unwrap();
		assert_eq!(res.len(), per_group, "group {g}");
		assert_eq!(res.first().unwrap().0.as_ref(), format!("{g}:post:00000000").as_bytes());
		assert_eq!(
			res.last().unwrap().0.as_ref(),
			format!("{g}:post:{:08}", per_group - 1).as_bytes()
		);
	}
	tx.cancel().unwrap();
}

#[test]
fn scan_across_prefix_boundary() {
	let db = Database::new();
	let mut tx = db.transaction(true);
	for i in 0..500 {
		tx.set(format!("a:{i:06}").into_bytes(), b"va".to_vec()).unwrap();
		tx.set(format!("b:{i:06}").into_bytes(), b"vb".to_vec()).unwrap();
	}
	tx.commit().unwrap();
	let mut tx = db.transaction(false);
	// Range that crosses the a→b boundary
	let lo: &[u8] = b"a:000400";
	let hi: &[u8] = b"b:000100";
	let res = tx.scan(lo..hi, None, None).unwrap();
	// Entries: a:000400..a:000499 (100) + b:000000..b:000099 (100) = 200
	assert_eq!(res.len(), 200);
	assert_eq!(res.first().unwrap().0.as_ref(), b"a:000400".as_ref());
	assert_eq!(res.last().unwrap().0.as_ref(), b"b:000099".as_ref());
	for (i, (k, _)) in res.iter().enumerate() {
		if i < 100 {
			assert!(k.starts_with(b"a:"));
		} else {
			assert!(k.starts_with(b"b:"));
		}
	}
	tx.cancel().unwrap();
}

#[test]
fn long_shared_prefix_small_suffix() {
	// 200-byte shared prefix + 4-byte distinguishing suffix
	let db = Database::new();
	let prefix = "x".repeat(200);
	let mut tx = db.transaction(true);
	for i in 0..2_000u32 {
		tx.set(format!("{prefix}:{i:04}").into_bytes(), b"v".to_vec()).unwrap();
	}
	tx.commit().unwrap();
	let mut tx = db.transaction(false);
	let lo = format!("{prefix}:").into_bytes();
	let hi = prefix_end(&format!("{prefix}:"));
	let res = tx.scan(lo.as_slice()..hi.as_slice(), None, None).unwrap();
	assert_eq!(res.len(), 2_000);
	for (i, (k, _)) in res.iter().enumerate() {
		assert_eq!(k.as_ref(), format!("{prefix}:{i:04}").as_bytes());
	}
	tx.cancel().unwrap();
}

#[test]
fn cursor_seek_at_prefix_boundary() {
	let db = Database::new();
	let mut tx = db.transaction(true);
	for i in 0..500 {
		tx.set(format!("a:{i:06}").into_bytes(), b"va".to_vec()).unwrap();
		tx.set(format!("b:{i:06}").into_bytes(), b"vb".to_vec()).unwrap();
	}
	tx.commit().unwrap();

	let mut tx = db.transaction(false);
	let lo: &[u8] = b"a:";
	let hi: &[u8] = b"c:";
	{
		let mut cursor = tx.cursor(lo..hi).unwrap();
		// Seek to the start of group "b"
		cursor.seek(b"b:000000");
		assert!(cursor.valid());
		assert_eq!(cursor.key().unwrap().as_ref(), b"b:000000");

		// Walk forward — should yield exactly the 500 b: entries
		let mut forward_count = 0;
		while cursor.valid() {
			forward_count += 1;
			cursor.next();
		}
		assert_eq!(forward_count, 500);
	}
	// seek_for_prev to last "a" key
	{
		let mut cursor = tx.cursor(lo..hi).unwrap();
		cursor.seek_for_prev(b"a:999999");
		assert_eq!(cursor.key().unwrap().as_ref(), b"a:000499");
		cursor.prev();
		assert_eq!(cursor.key().unwrap().as_ref(), b"a:000498");
	}
	tx.cancel().unwrap();
}

#[test]
fn versioning_under_repeated_writes_shared_prefix() {
	let db = Database::new().with_gc_history(std::time::Duration::from_secs(60));
	// Five writes per key, on 200 prefix-sharing keys
	for round in 0..5 {
		let mut tx = db.transaction(true);
		for i in 0..200 {
			tx.set(pkey("doc", i), format!("round{round}-i{i}").into_bytes()).unwrap();
		}
		tx.commit().unwrap();
	}
	let mut tx = db.transaction(false);
	let lo: &[u8] = b"doc:";
	let hi = prefix_end("doc:");
	// Latest scan sees the last round
	let res = tx.scan(lo..hi.as_slice(), None, None).unwrap();
	assert_eq!(res.len(), 200);
	for (i, (_, v)) in res.iter().enumerate() {
		assert_eq!(v.as_ref(), format!("round4-i{i}").as_bytes());
	}
	// scan_all_versions sees the full chain (5 versions × 200 keys = 1000)
	let all = tx.scan_all_versions(lo..hi.as_slice(), None, None).unwrap();
	assert_eq!(all.len(), 1_000);
	tx.cancel().unwrap();
}

#[test]
fn range_query_bounds_inside_prefix_group() {
	let db = Database::new();
	let mut tx = db.transaction(true);
	for i in 0..1_000 {
		tx.set(pkey("g", i), b"v".to_vec()).unwrap();
	}
	tx.commit().unwrap();

	let mut tx = db.transaction(false);
	// [200, 700) — 500 keys
	let lo = pkey("g", 200);
	let hi = pkey("g", 700);
	let res = tx.scan(lo.as_slice()..hi.as_slice(), None, None).unwrap();
	assert_eq!(res.len(), 500);
	assert_eq!(res.first().unwrap().0.as_ref(), pkey("g", 200).as_slice());
	assert_eq!(res.last().unwrap().0.as_ref(), pkey("g", 699).as_slice());
	// limit + skip in the middle of the range
	let lo_all = pkey("g", 0);
	let hi_all = pkey("g", 1000);
	let lim = tx.scan(lo_all.as_slice()..hi_all.as_slice(), Some(50), Some(100)).unwrap();
	assert_eq!(lim.len(), 100);
	assert_eq!(lim.first().unwrap().0.as_ref(), pkey("g", 50).as_slice());
	assert_eq!(lim.last().unwrap().0.as_ref(), pkey("g", 149).as_slice());
	tx.cancel().unwrap();
}

#[test]
fn deeply_nested_surrealdb_style_keys() {
	let db = Database::new();
	let namespaces = ["ns_alpha", "ns_beta"];
	let dbs = ["main", "audit"];
	let tables = ["user", "post", "comment"];
	let per_table = 200;

	let mut tx = db.transaction(true);
	for ns in &namespaces {
		for d in &dbs {
			for tbl in &tables {
				for id in 0..per_table {
					let key = format!("surreal:{ns}:{d}:{tbl}:{id:06}").into_bytes();
					tx.set(key, format!("{ns}-{d}-{tbl}-{id}").into_bytes()).unwrap();
				}
			}
		}
	}
	tx.commit().unwrap();

	let mut tx = db.transaction(false);
	let lo: &[u8] = b"surreal:";
	let hi = prefix_end("surreal:");
	let total = namespaces.len() * dbs.len() * tables.len() * per_table;
	let all = tx.total(lo..hi.as_slice(), None, None).unwrap();
	assert_eq!(all, total);

	// Scope one table
	let t_lo = b"surreal:ns_alpha:main:user:".to_vec();
	let t_hi = prefix_end("surreal:ns_alpha:main:user:");
	let res = tx.scan(t_lo.as_slice()..t_hi.as_slice(), None, None).unwrap();
	assert_eq!(res.len(), per_table);
	for (i, (k, v)) in res.iter().enumerate() {
		assert_eq!(k.as_ref(), format!("surreal:ns_alpha:main:user:{i:06}").as_bytes());
		assert_eq!(v.as_ref(), format!("ns_alpha-main-user-{i}").as_bytes());
	}

	// Scope one whole namespace
	let n_lo = b"surreal:ns_beta:".to_vec();
	let n_hi = prefix_end("surreal:ns_beta:");
	let ns_res = tx.total(n_lo.as_slice()..n_hi.as_slice(), None, None).unwrap();
	assert_eq!(ns_res, dbs.len() * tables.len() * per_table);
	tx.cancel().unwrap();
}
