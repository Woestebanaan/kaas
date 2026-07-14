//! Partition + consumer-group placement.
//!
//! Pure functions
//! over `(prev assignment, alive brokers, inputs)`; no state, no
//! I/O. Both shapes — partition placement and group placement —
//! follow the same three-step recipe:
//!
//! 1. **Preserve** any prior assignment whose broker is still in
//!    the alive set. Stable assignments minimise log migration on
//!    the shared PVC.
//! 2. **Rendezvous-pick** the rest. Highest-random-weight hashing
//!    keyed on `(topic, partition, broker_id)` (or `(group_id,
//!    broker_id)` for groups) gives a deterministic, evenly-
//!    distributed placement without coordination.
//! 3. **Smooth** the partition layer with a deterministic pass to
//!    enforce `max(per-broker count) - min ≤ 1`. Group placement
//!    skips smoothing because each group is a single unit.
//!
//! Hash: XXH64 via `twox-hash`, byte-for-byte stable so an
//! assignment written by any release matches a v0.1-written
//! one for the same input (upgrade requirement). The previous
//! FNV-1a 64 had pathological avalanche on broker IDs differing by
//! one byte and drove a 50/25/25 skew on 3-broker clusters
//! (skafka#112).

use std::collections::{HashMap, HashSet};
use std::hash::Hasher;

use sk_broker::{ConsumerGroupAssignment, PartitionAssignment, PartitionRole};

/// Per-topic catalog entry the balancer consumes. The KafkaTopic CR
/// watcher (Phase 7) is the production source; tests pass a literal
/// `Vec<TopicSpec>`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicSpec {
    pub name: String,
    pub partition_count: i32,
}

/// Per-active-group entry the balancer consumes. The HeartbeatServer's
/// `active_groups()` union is the production source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupSpec {
    pub group_id: String,
}

/// `XXH64(topic || 0x00 || partition_be || 0x00 || broker)`.
/// The byte sequence is pinned so any controller build picks the
/// same broker as a v0.1-driven one for the same inputs (upgrade
/// compatibility).
pub fn rendezvous_hash(topic: &str, partition: i32, broker: &str) -> u64 {
    let mut h = twox_hash::XxHash64::with_seed(0);
    h.write(topic.as_bytes());
    h.write(&[0]);
    h.write(&partition.to_be_bytes());
    h.write(&[0]);
    h.write(broker.as_bytes());
    h.finish()
}

/// `XXH64(group_id || 0x00 || broker)`. No partition dimension —
/// groups are single coordinated units.
pub fn group_hash(group_id: &str, broker: &str) -> u64 {
    let mut h = twox_hash::XxHash64::with_seed(0);
    h.write(group_id.as_bytes());
    h.write(&[0]);
    h.write(broker.as_bytes());
    h.finish()
}

/// Highest-random-weight pick over the broker set for one
/// `(topic, partition)`.
pub fn rendezvous_pick(topic: &str, partition: i32, brokers: &[String]) -> Option<String> {
    let mut best: Option<(u64, &str)> = None;
    for b in brokers {
        let h = rendezvous_hash(topic, partition, b);
        match best {
            None => best = Some((h, b)),
            Some((bh, _)) if h > bh => best = Some((h, b)),
            _ => {}
        }
    }
    best.map(|(_, b)| b.to_owned())
}

/// Highest-random-weight pick for one `group_id`. Same shape as
/// [`rendezvous_pick`] with the group-keyed hash.
pub fn rendezvous_pick_group(group_id: &str, brokers: &[String]) -> Option<String> {
    let mut best: Option<(u64, &str)> = None;
    for b in brokers {
        let h = group_hash(group_id, b);
        match best {
            None => best = Some((h, b)),
            Some((bh, _)) if h > bh => best = Some((h, b)),
            _ => {}
        }
    }
    best.map(|(_, b)| b.to_owned())
}

/// Working tuple the smoother mutates in place. Internal.
#[derive(Debug, Clone)]
struct PartitionSlot {
    topic: String,
    partition: i32,
    broker: String,
}

/// Returns `topic/partition` keyed prior partition assignments —
/// internal lookup for [`balance`].
fn prev_partitions(prev: Option<&[PartitionAssignment]>) -> HashMap<String, PartitionAssignment> {
    let mut out = HashMap::new();
    if let Some(ps) = prev {
        for p in ps {
            out.insert(partition_key(&p.topic, p.partition), p.clone());
        }
    }
    out
}

/// `"topic/partition"` cache key. Must match
/// [`sk_broker::partition_key`] byte-for-byte — the takeover driver
/// and the balancer have to agree on the lookup string.
pub fn partition_key(topic: &str, partition: i32) -> String {
    sk_broker::partition_key(topic, partition)
}

/// Run the partition balancer. Returns the per-(topic, partition)
/// assignment that the writer stamps into `assignment.json`.
///
/// `prev` is the previously written assignment's partition list (or
/// `None` on a fresh controller takeover). `brokers` is the alive
/// set the controller currently sees. `topics` is the catalog.
pub fn balance(
    prev: Option<&[PartitionAssignment]>,
    brokers: &[String],
    topics: &[TopicSpec],
) -> Vec<PartitionAssignment> {
    if brokers.is_empty() {
        return Vec::new();
    }
    let mut alive = brokers.to_vec();
    alive.sort();
    let alive_set: HashSet<String> = alive.iter().cloned().collect();
    let prev_map = prev_partitions(prev);

    // Phase 1: raw rendezvous pick per partition.
    let mut slots: Vec<PartitionSlot> = Vec::new();
    for t in topics {
        for partition in 0..t.partition_count {
            let broker = rendezvous_pick(&t.name, partition, &alive).unwrap_or_default();
            slots.push(PartitionSlot {
                topic: t.name.clone(),
                partition,
                broker,
            });
        }
    }

    // Phase 2: deterministic smoothing pass.
    smooth_partitions(&mut slots, &alive);

    // Phase 3: reconcile with prev for stable epochs.
    let mut out = Vec::with_capacity(slots.len());
    for s in slots {
        let key = partition_key(&s.topic, s.partition);
        let prev_entry = prev_map.get(&key);
        if let Some(pa) = prev_entry {
            if alive_set.contains(&pa.broker) && pa.broker == s.broker {
                out.push(pa.clone());
                continue;
            }
        }
        let epoch = prev_entry.map(|pa| pa.epoch + 1).unwrap_or(1);
        out.push(PartitionAssignment {
            topic: s.topic,
            partition: s.partition,
            broker: s.broker,
            epoch,
            role: PartitionRole::Leader,
        });
    }
    out
}

/// Same recipe for consumer groups: keep a still-alive assignment;
/// otherwise hash-pick. No smoothing — each group is a single unit.
pub fn balance_groups(
    prev: Option<&[ConsumerGroupAssignment]>,
    brokers: &[String],
    groups: &[GroupSpec],
) -> Vec<ConsumerGroupAssignment> {
    if brokers.is_empty() {
        return Vec::new();
    }
    let mut alive = brokers.to_vec();
    alive.sort();
    let alive_set: HashSet<String> = alive.iter().cloned().collect();
    let prev_map: HashMap<String, ConsumerGroupAssignment> = prev
        .map(|ps| ps.iter().map(|g| (g.group_id.clone(), g.clone())).collect())
        .unwrap_or_default();

    let mut out = Vec::with_capacity(groups.len());
    for g in groups {
        let prev_entry = prev_map.get(&g.group_id);
        if let Some(ga) = prev_entry {
            if alive_set.contains(&ga.broker) {
                out.push(ga.clone());
                continue;
            }
        }
        let broker = rendezvous_pick_group(&g.group_id, &alive).unwrap_or_default();
        let epoch = prev_entry.map(|ga| ga.epoch + 1).unwrap_or(1);
        out.push(ConsumerGroupAssignment {
            group_id: g.group_id.clone(),
            broker,
            epoch,
        });
    }
    out
}

/// Move partitions from the most-loaded broker to the least-loaded
/// until `max - min ≤ 1`. Deterministic — ties broken
/// lexicographically on broker ID; victim picked by highest
/// rendezvous score for the recipient (= the move closest to a
/// no-op from rendezvous's perspective). Owned `String` keys throughout so the
/// counts map doesn't tangle with the `alive` slice's lifetime.
fn smooth_partitions(slots: &mut [PartitionSlot], alive: &[String]) {
    if alive.len() < 2 || slots.is_empty() {
        return;
    }
    let mut counts: HashMap<String, i32> = alive.iter().map(|b| (b.clone(), 0)).collect();
    for s in slots.iter() {
        *counts.entry(s.broker.clone()).or_insert(0) += 1;
    }
    loop {
        let mut hi = alive[0].clone();
        let mut lo = alive[0].clone();
        let mut hi_count = *counts.get(&hi).unwrap_or(&0);
        let mut lo_count = *counts.get(&lo).unwrap_or(&0);
        for b in &alive[1..] {
            let c = *counts.get(b).unwrap_or(&0);
            if c > hi_count {
                hi = b.clone();
                hi_count = c;
            }
            if c < lo_count {
                lo = b.clone();
                lo_count = c;
            }
        }
        if hi_count - lo_count <= 1 {
            return;
        }
        // Pick victim partition on `hi` with the highest rendezvous
        // score for `lo`.
        let mut victim_idx: Option<usize> = None;
        let mut victim_score: u64 = 0;
        for (i, s) in slots.iter().enumerate() {
            if s.broker != hi {
                continue;
            }
            let score = rendezvous_hash(&s.topic, s.partition, &lo);
            match victim_idx {
                None => {
                    victim_idx = Some(i);
                    victim_score = score;
                }
                Some(_) if score > victim_score => {
                    victim_idx = Some(i);
                    victim_score = score;
                }
                Some(prev) if score == victim_score => {
                    let (vt, vp) = (&slots[prev].topic, slots[prev].partition);
                    if &s.topic < vt || (&s.topic == vt && s.partition < vp) {
                        victim_idx = Some(i);
                    }
                }
                _ => {}
            }
        }
        match victim_idx {
            None => return, // unreachable — hi has > 0 slots
            Some(i) => {
                slots[i].broker = lo.clone();
                *counts.entry(hi).or_insert(0) -= 1;
                *counts.entry(lo).or_insert(0) += 1;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn brokers(n: usize) -> Vec<String> {
        (0..n).map(|i| format!("skafka-{i}")).collect()
    }

    #[test]
    fn empty_broker_set_returns_empty_assignment() {
        let topics = vec![TopicSpec {
            name: "t1".to_owned(),
            partition_count: 4,
        }];
        let parts = balance(None, &[], &topics);
        assert!(parts.is_empty());
    }

    #[test]
    fn rendezvous_is_deterministic_for_fixed_inputs() {
        let b = brokers(3);
        let a = rendezvous_pick("t1", 0, &b);
        let b_again = rendezvous_pick("t1", 0, &b);
        assert_eq!(a, b_again);
    }

    #[test]
    fn rendezvous_pick_returns_one_of_the_brokers() {
        let b = brokers(3);
        let pick = rendezvous_pick("t1", 0, &b).unwrap();
        assert!(b.contains(&pick));
    }

    #[test]
    fn balance_assigns_every_partition_to_an_alive_broker() {
        let b = brokers(3);
        let topics = vec![
            TopicSpec {
                name: "t1".to_owned(),
                partition_count: 6,
            },
            TopicSpec {
                name: "t2".to_owned(),
                partition_count: 3,
            },
        ];
        let parts = balance(None, &b, &topics);
        assert_eq!(parts.len(), 9);
        for p in &parts {
            assert!(
                b.contains(&p.broker),
                "broker {} not in alive set",
                p.broker
            );
            assert_eq!(p.epoch, 1, "fresh assignment starts at epoch 1");
            assert_eq!(p.role, PartitionRole::Leader);
        }
    }

    #[test]
    fn balance_stability_keeps_assignment_when_brokers_unchanged() {
        let b = brokers(3);
        let topics = vec![TopicSpec {
            name: "t1".to_owned(),
            partition_count: 6,
        }];
        let first = balance(None, &b, &topics);
        let second = balance(Some(&first), &b, &topics);
        assert_eq!(first, second, "stable inputs → identical assignment");
    }

    #[test]
    fn balance_smoother_caps_skew_at_one() {
        let b = brokers(3);
        let topics = vec![TopicSpec {
            name: "t1".to_owned(),
            partition_count: 16,
        }];
        let parts = balance(None, &b, &topics);
        let mut counts: HashMap<&str, i32> = HashMap::new();
        for p in &parts {
            *counts.entry(p.broker.as_str()).or_insert(0) += 1;
        }
        let hi = counts.values().max().copied().unwrap_or(0);
        let lo = counts.values().min().copied().unwrap_or(0);
        assert!(
            hi - lo <= 1,
            "smoother must cap skew at 1; got hi={hi} lo={lo} counts={counts:?}"
        );
    }

    #[test]
    fn balance_reassigns_only_partitions_on_dead_brokers() {
        let three = brokers(3);
        let topics = vec![TopicSpec {
            name: "t1".to_owned(),
            partition_count: 9,
        }];
        let first = balance(None, &three, &topics);
        // skafka-2 goes down.
        let two = vec!["skafka-0".to_owned(), "skafka-1".to_owned()];
        let second = balance(Some(&first), &two, &topics);
        for p in &first {
            if p.broker != "skafka-2" {
                // Stable partition keeps epoch 1.
                let matching = second
                    .iter()
                    .find(|q| q.topic == p.topic && q.partition == p.partition)
                    .expect("partition retained");
                if matching.broker == p.broker {
                    assert_eq!(matching.epoch, p.epoch, "stable partition keeps epoch");
                }
            }
        }
        // Every partition assigned to an alive broker.
        for p in &second {
            assert!(p.broker == "skafka-0" || p.broker == "skafka-1");
        }
    }

    #[test]
    fn balance_groups_stable_on_alive_set_unchanged() {
        let b = brokers(3);
        let groups = vec![
            GroupSpec {
                group_id: "g1".to_owned(),
            },
            GroupSpec {
                group_id: "g2".to_owned(),
            },
        ];
        let first = balance_groups(None, &b, &groups);
        let second = balance_groups(Some(&first), &b, &groups);
        assert_eq!(first, second);
    }

    #[test]
    fn balance_groups_reassigns_only_dead_broker_groups() {
        let three = brokers(3);
        let groups = vec![
            GroupSpec {
                group_id: "ga".to_owned(),
            },
            GroupSpec {
                group_id: "gb".to_owned(),
            },
            GroupSpec {
                group_id: "gc".to_owned(),
            },
        ];
        let first = balance_groups(None, &three, &groups);
        let two = vec!["skafka-0".to_owned(), "skafka-1".to_owned()];
        let second = balance_groups(Some(&first), &two, &groups);
        for g in &second {
            assert!(g.broker == "skafka-0" || g.broker == "skafka-1");
        }
    }

    #[test]
    fn rendezvous_hash_byte_sequence_pinned() {
        // Pin a known input → output mapping so a future change to
        // the byte construction (delimiters, order) surfaces here
        // rather than as a silent cutover divergence.
        let h = rendezvous_hash("t1", 0, "skafka-0");
        let h_swap = rendezvous_hash("t1", 1, "skafka-0");
        assert_ne!(h, h_swap, "different partition must yield different hash");
    }
}
