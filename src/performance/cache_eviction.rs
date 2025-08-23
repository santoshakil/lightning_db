use std::collections::{HashMap, VecDeque, LinkedList};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use parking_lot::{RwLock, Mutex};
use std::hash::Hash;
use std::time::{Duration, Instant};
use tracing::{debug, trace};

pub trait EvictionPolicy<K: Clone + Hash + Eq, V: Clone>: Send + Sync {
    fn get(&mut self, key: &K) -> Option<V>;
    fn put(&mut self, key: K, value: V) -> Option<(K, V)>;
    fn remove(&mut self, key: &K) -> Option<V>;
    fn clear(&mut self);
    fn size(&self) -> usize;
    fn capacity(&self) -> usize;
}

pub struct AdaptiveReplacementCache<K: Clone + Hash + Eq, V: Clone> {
    capacity: usize,
    t1: VecDeque<K>,
    t2: VecDeque<K>,
    b1: VecDeque<K>,
    b2: VecDeque<K>,
    cache: HashMap<K, V>,
    p: usize,
    hits: AtomicU64,
    misses: AtomicU64,
}

impl<K: Clone + Hash + Eq, V: Clone> AdaptiveReplacementCache<K, V> {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            t1: VecDeque::new(),
            t2: VecDeque::new(),
            b1: VecDeque::new(),
            b2: VecDeque::new(),
            cache: HashMap::new(),
            p: 0,
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }
    
    fn replace(&mut self, key: &K) -> Option<(K, V)> {
        let in_b2 = self.b2.contains(key);
        
        let t1_len = self.t1.len();
        let mut victim = None;
        
        if t1_len > 0 && (t1_len > self.p || (in_b2 && t1_len == self.p)) {
            if let Some(evict_key) = self.t1.pop_front() {
                self.b1.push_back(evict_key.clone());
                victim = self.cache.remove(&evict_key).map(|v| (evict_key, v));
            }
        } else if let Some(evict_key) = self.t2.pop_front() {
            self.b2.push_back(evict_key.clone());
            victim = self.cache.remove(&evict_key).map(|v| (evict_key, v));
        }
        
        if self.b1.len() + self.b2.len() > self.capacity {
            if self.b1.len() > self.capacity - self.p {
                self.b1.pop_front();
            } else {
                self.b2.pop_front();
            }
        }
        
        victim
    }
    
    fn adjust_p(&mut self, in_b1: bool, in_b2: bool) {
        if in_b1 {
            let delta = if self.b1.len() >= self.b2.len() { 1 } else { self.b2.len() / self.b1.len() };
            self.p = self.p.saturating_add(delta).min(self.capacity);
        } else if in_b2 {
            let delta = if self.b2.len() >= self.b1.len() { 1 } else { self.b1.len() / self.b2.len() };
            self.p = self.p.saturating_sub(delta);
        }
    }
}

impl<K: Clone + Hash + Eq, V: Clone> EvictionPolicy<K, V> for AdaptiveReplacementCache<K, V> {
    fn get(&mut self, key: &K) -> Option<V> {
        if let Some(value) = self.cache.get(key) {
            self.hits.fetch_add(1, Ordering::Relaxed);
            
            if let Some(pos) = self.t1.iter().position(|k| k == key) {
                self.t1.remove(pos);
                self.t2.push_back(key.clone());
            } else if let Some(pos) = self.t2.iter().position(|k| k == key) {
                self.t2.remove(pos);
                self.t2.push_back(key.clone());
            }
            
            Some(value.clone())
        } else {
            self.misses.fetch_add(1, Ordering::Relaxed);
            None
        }
    }
    
    fn put(&mut self, key: K, value: V) -> Option<(K, V)> {
        if self.cache.contains_key(&key) {
            self.cache.insert(key.clone(), value);
            self.get(&key);
            return None;
        }
        
        let mut evicted = None;
        
        let in_b1 = self.b1.iter().any(|k| k == &key);
        let in_b2 = self.b2.iter().any(|k| k == &key);
        
        if in_b1 || in_b2 {
            if self.t1.len() + self.t2.len() >= self.capacity {
                evicted = self.replace(&key);
            }
            
            self.adjust_p(in_b1, in_b2);
            
            if in_b1 {
                self.b1.retain(|k| k != &key);
            } else {
                self.b2.retain(|k| k != &key);
            }
            
            self.t2.push_back(key.clone());
        } else {
            if self.t1.len() + self.t2.len() >= self.capacity {
                evicted = self.replace(&key);
            }
            
            self.t1.push_back(key.clone());
        }
        
        self.cache.insert(key, value);
        evicted
    }
    
    fn remove(&mut self, key: &K) -> Option<V> {
        self.t1.retain(|k| k != key);
        self.t2.retain(|k| k != key);
        self.b1.retain(|k| k != key);
        self.b2.retain(|k| k != key);
        self.cache.remove(key)
    }
    
    fn clear(&mut self) {
        self.t1.clear();
        self.t2.clear();
        self.b1.clear();
        self.b2.clear();
        self.cache.clear();
        self.p = 0;
    }
    
    fn size(&self) -> usize {
        self.cache.len()
    }
    
    fn capacity(&self) -> usize {
        self.capacity
    }
}

pub struct ClockProCache<K: Clone + Hash + Eq, V: Clone> {
    capacity: usize,
    hot_capacity: usize,
    hand_hot: usize,
    hand_cold: usize,
    hand_test: usize,
    hot: Vec<ClockEntry<K, V>>,
    cold: Vec<ClockEntry<K, V>>,
    test: VecDeque<K>,
    cache: HashMap<K, CacheLocation>,
}

#[derive(Clone)]
struct ClockEntry<K, V> {
    key: K,
    value: V,
    reference: bool,
    is_hot: bool,
}

#[derive(Clone, Copy)]
enum CacheLocation {
    Hot(usize),
    Cold(usize),
}

impl<K: Clone + Hash + Eq, V: Clone> ClockProCache<K, V> {
    pub fn new(capacity: usize) -> Self {
        let hot_capacity = capacity * 3 / 4;
        Self {
            capacity,
            hot_capacity,
            hand_hot: 0,
            hand_cold: 0,
            hand_test: 0,
            hot: Vec::new(),
            cold: Vec::new(),
            test: VecDeque::new(),
            cache: HashMap::new(),
        }
    }
    
    fn evict_from_cold(&mut self) -> Option<(K, V)> {
        let cold_len = self.cold.len();
        if cold_len == 0 {
            return None;
        }
        
        for _ in 0..cold_len * 2 {
            let entry = &mut self.cold[self.hand_cold];
            
            if !entry.reference {
                let key = entry.key.clone();
                let value = entry.value.clone();
                self.cold.remove(self.hand_cold);
                self.cache.remove(&key);
                
                if self.hand_cold >= self.cold.len() && !self.cold.is_empty() {
                    self.hand_cold = 0;
                }
                
                return Some((key, value));
            }
            
            entry.reference = false;
            self.hand_cold = (self.hand_cold + 1) % self.cold.len();
        }
        
        if !self.cold.is_empty() {
            let entry = self.cold.remove(self.hand_cold);
            self.cache.remove(&entry.key);
            
            if self.hand_cold >= self.cold.len() && !self.cold.is_empty() {
                self.hand_cold = 0;
            }
            
            Some((entry.key, entry.value))
        } else {
            None
        }
    }
    
    fn promote_to_hot(&mut self, key: &K) {
        if let Some(CacheLocation::Cold(idx)) = self.cache.get(key).copied() {
            if idx < self.cold.len() {
                let mut entry = self.cold.remove(idx);
                entry.is_hot = true;
                entry.reference = true;
                
                self.hot.push(entry);
                self.cache.insert(key.clone(), CacheLocation::Hot(self.hot.len() - 1));
                
                if self.hand_cold >= self.cold.len() && !self.cold.is_empty() {
                    self.hand_cold = 0;
                }
                
                if self.hot.len() > self.hot_capacity {
                    self.demote_from_hot();
                }
            }
        }
    }
    
    fn demote_from_hot(&mut self) {
        let hot_len = self.hot.len();
        if hot_len == 0 {
            return;
        }
        
        for _ in 0..hot_len * 2 {
            if self.hand_hot >= self.hot.len() {
                self.hand_hot = 0;
            }
            
            let entry = &mut self.hot[self.hand_hot];
            
            if !entry.reference {
                let mut demoted = self.hot.remove(self.hand_hot);
                demoted.is_hot = false;
                
                self.cold.push(demoted.clone());
                self.cache.insert(demoted.key.clone(), CacheLocation::Cold(self.cold.len() - 1));
                
                if self.hand_hot >= self.hot.len() && !self.hot.is_empty() {
                    self.hand_hot = 0;
                }
                
                return;
            }
            
            entry.reference = false;
            self.hand_hot = (self.hand_hot + 1) % self.hot.len();
        }
    }
}

impl<K: Clone + Hash + Eq, V: Clone> EvictionPolicy<K, V> for ClockProCache<K, V> {
    fn get(&mut self, key: &K) -> Option<V> {
        match self.cache.get(key).copied() {
            Some(CacheLocation::Hot(idx)) => {
                if idx < self.hot.len() {
                    self.hot[idx].reference = true;
                    Some(self.hot[idx].value.clone())
                } else {
                    None
                }
            }
            Some(CacheLocation::Cold(idx)) => {
                if idx < self.cold.len() {
                    self.cold[idx].reference = true;
                    
                    if self.test.contains(key) {
                        self.promote_to_hot(key);
                    }
                    
                    Some(self.cold[idx].value.clone())
                } else {
                    None
                }
            }
            None => None,
        }
    }
    
    fn put(&mut self, key: K, value: V) -> Option<(K, V)> {
        if self.cache.contains_key(&key) {
            match self.cache[&key] {
                CacheLocation::Hot(idx) => {
                    self.hot[idx].value = value;
                    self.hot[idx].reference = true;
                }
                CacheLocation::Cold(idx) => {
                    self.cold[idx].value = value;
                    self.cold[idx].reference = true;
                }
            }
            return None;
        }
        
        let mut evicted = None;
        
        if self.hot.len() + self.cold.len() >= self.capacity {
            evicted = self.evict_from_cold();
        }
        
        let entry = ClockEntry {
            key: key.clone(),
            value,
            reference: false,
            is_hot: false,
        };
        
        self.cold.push(entry);
        self.cache.insert(key.clone(), CacheLocation::Cold(self.cold.len() - 1));
        
        self.test.push_back(key);
        if self.test.len() > self.capacity / 2 {
            self.test.pop_front();
        }
        
        evicted
    }
    
    fn remove(&mut self, key: &K) -> Option<V> {
        match self.cache.remove(key) {
            Some(CacheLocation::Hot(idx)) => {
                if idx < self.hot.len() {
                    let entry = self.hot.remove(idx);
                    Some(entry.value)
                } else {
                    None
                }
            }
            Some(CacheLocation::Cold(idx)) => {
                if idx < self.cold.len() {
                    let entry = self.cold.remove(idx);
                    Some(entry.value)
                } else {
                    None
                }
            }
            None => None,
        }
    }
    
    fn clear(&mut self) {
        self.hot.clear();
        self.cold.clear();
        self.test.clear();
        self.cache.clear();
        self.hand_hot = 0;
        self.hand_cold = 0;
        self.hand_test = 0;
    }
    
    fn size(&self) -> usize {
        self.cache.len()
    }
    
    fn capacity(&self) -> usize {
        self.capacity
    }
}

pub struct WTinyLFU<K: Clone + Hash + Eq, V: Clone> {
    capacity: usize,
    window_capacity: usize,
    main_capacity: usize,
    window: VecDeque<K>,
    probation: VecDeque<K>,
    protected: VecDeque<K>,
    cache: HashMap<K, (V, CacheSegment)>,
    frequency: CountMinSketch,
    hits: AtomicU64,
    misses: AtomicU64,
}

#[derive(Clone, Copy, PartialEq)]
enum CacheSegment {
    Window,
    Probation,
    Protected,
}

struct CountMinSketch {
    counters: Vec<Vec<u8>>,
    width: usize,
    depth: usize,
}

impl CountMinSketch {
    fn new(width: usize, depth: usize) -> Self {
        Self {
            counters: vec![vec![0; width]; depth],
            width,
            depth,
        }
    }
    
    fn increment<K: Hash>(&mut self, key: &K) {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::Hasher;
        
        for i in 0..self.depth {
            let mut hasher = DefaultHasher::new();
            hasher.write_usize(i);
            key.hash(&mut hasher);
            let hash = hasher.finish() as usize;
            let j = hash % self.width;
            
            self.counters[i][j] = self.counters[i][j].saturating_add(1);
        }
    }
    
    fn estimate<K: Hash>(&self, key: &K) -> u8 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::Hasher;
        
        let mut min_count = u8::MAX;
        
        for i in 0..self.depth {
            let mut hasher = DefaultHasher::new();
            hasher.write_usize(i);
            key.hash(&mut hasher);
            let hash = hasher.finish() as usize;
            let j = hash % self.width;
            
            min_count = min_count.min(self.counters[i][j]);
        }
        
        min_count
    }
    
    fn reset(&mut self) {
        for row in &mut self.counters {
            for counter in row {
                *counter = counter.saturating_sub(*counter / 2);
            }
        }
    }
}

impl<K: Clone + Hash + Eq, V: Clone> WTinyLFU<K, V> {
    pub fn new(capacity: usize) -> Self {
        let window_capacity = capacity / 100;
        let main_capacity = capacity - window_capacity;
        let protected_capacity = main_capacity * 8 / 10;
        
        Self {
            capacity,
            window_capacity,
            main_capacity,
            window: VecDeque::new(),
            probation: VecDeque::new(),
            protected: VecDeque::new(),
            cache: HashMap::new(),
            frequency: CountMinSketch::new(capacity * 4, 4),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }
    
    fn admit(&self, candidate: &K, victim: &K) -> bool {
        let candidate_freq = self.frequency.estimate(candidate);
        let victim_freq = self.frequency.estimate(victim);
        
        candidate_freq > victim_freq
    }
    
    fn evict_from_window(&mut self) -> Option<K> {
        self.window.pop_front()
    }
    
    fn evict_from_main(&mut self) -> Option<K> {
        if !self.probation.is_empty() {
            self.probation.pop_front()
        } else {
            self.protected.pop_front()
        }
    }
}

impl<K: Clone + Hash + Eq, V: Clone> EvictionPolicy<K, V> for WTinyLFU<K, V> {
    fn get(&mut self, key: &K) -> Option<V> {
        if let Some((value, segment)) = self.cache.get_mut(key) {
            self.hits.fetch_add(1, Ordering::Relaxed);
            self.frequency.increment(key);
            
            match *segment {
                CacheSegment::Window => {},
                CacheSegment::Probation => {
                    if let Some(pos) = self.probation.iter().position(|k| k == key) {
                        self.probation.remove(pos);
                        self.protected.push_back(key.clone());
                        *segment = CacheSegment::Protected;
                        
                        if self.protected.len() > self.main_capacity * 8 / 10 {
                            if let Some(demoted) = self.protected.pop_front() {
                                self.probation.push_back(demoted.clone());
                                if let Some((_, seg)) = self.cache.get_mut(&demoted) {
                                    *seg = CacheSegment::Probation;
                                }
                            }
                        }
                    }
                }
                CacheSegment::Protected => {
                    if let Some(pos) = self.protected.iter().position(|k| k == key) {
                        self.protected.remove(pos);
                        self.protected.push_back(key.clone());
                    }
                }
            }
            
            Some(value.clone())
        } else {
            self.misses.fetch_add(1, Ordering::Relaxed);
            None
        }
    }
    
    fn put(&mut self, key: K, value: V) -> Option<(K, V)> {
        if self.cache.contains_key(&key) {
            if let Some((v, _)) = self.cache.get_mut(&key) {
                *v = value;
            }
            return None;
        }
        
        self.frequency.increment(&key);
        
        let mut evicted = None;
        
        if self.window.len() >= self.window_capacity {
            if let Some(candidate) = self.evict_from_window() {
                if self.probation.len() + self.protected.len() >= self.main_capacity {
                    if let Some(victim) = self.evict_from_main() {
                        if self.admit(&candidate, &victim) {
                            if let Some((v, _)) = self.cache.remove(&victim) {
                                evicted = Some((victim, v));
                            }
                            self.probation.push_back(candidate.clone());
                            if let Some((_, seg)) = self.cache.get_mut(&candidate) {
                                *seg = CacheSegment::Probation;
                            }
                        } else {
                            if let Some((v, _)) = self.cache.remove(&candidate) {
                                evicted = Some((candidate, v));
                            }
                        }
                    }
                } else {
                    self.probation.push_back(candidate.clone());
                    if let Some((_, seg)) = self.cache.get_mut(&candidate) {
                        *seg = CacheSegment::Probation;
                    }
                }
            }
        }
        
        self.window.push_back(key.clone());
        self.cache.insert(key, (value, CacheSegment::Window));
        
        evicted
    }
    
    fn remove(&mut self, key: &K) -> Option<V> {
        if let Some((value, segment)) = self.cache.remove(key) {
            match segment {
                CacheSegment::Window => self.window.retain(|k| k != key),
                CacheSegment::Probation => self.probation.retain(|k| k != key),
                CacheSegment::Protected => self.protected.retain(|k| k != key),
            }
            Some(value)
        } else {
            None
        }
    }
    
    fn clear(&mut self) {
        self.window.clear();
        self.probation.clear();
        self.protected.clear();
        self.cache.clear();
        self.frequency.reset();
    }
    
    fn size(&self) -> usize {
        self.cache.len()
    }
    
    fn capacity(&self) -> usize {
        self.capacity
    }
}

pub struct SegmentedLRU<K: Clone + Hash + Eq, V: Clone> {
    segments: Vec<VecDeque<K>>,
    cache: HashMap<K, (V, usize)>,
    segment_sizes: Vec<usize>,
    capacity: usize,
}

impl<K: Clone + Hash + Eq, V: Clone> SegmentedLRU<K, V> {
    pub fn new(capacity: usize, num_segments: usize) -> Self {
        let segment_size = capacity / num_segments;
        let mut segment_sizes = vec![segment_size; num_segments];
        segment_sizes[num_segments - 1] += capacity % num_segments;
        
        Self {
            segments: (0..num_segments).map(|_| VecDeque::new()).collect(),
            cache: HashMap::new(),
            segment_sizes,
            capacity,
        }
    }
    
    fn promote(&mut self, key: &K, from_segment: usize) {
        if from_segment + 1 < self.segments.len() {
            self.segments[from_segment].retain(|k| k != key);
            self.segments[from_segment + 1].push_back(key.clone());
            
            if let Some((_, seg)) = self.cache.get_mut(key) {
                *seg = from_segment + 1;
            }
            
            if self.segments[from_segment + 1].len() > self.segment_sizes[from_segment + 1] {
                if let Some(demoted) = self.segments[from_segment + 1].pop_front() {
                    self.segments[from_segment].push_back(demoted.clone());
                    if let Some((_, seg)) = self.cache.get_mut(&demoted) {
                        *seg = from_segment;
                    }
                }
            }
        }
    }
}

impl<K: Clone + Hash + Eq, V: Clone> EvictionPolicy<K, V> for SegmentedLRU<K, V> {
    fn get(&mut self, key: &K) -> Option<V> {
        if let Some((value, segment)) = self.cache.get(key) {
            let segment = *segment;
            
            if let Some(pos) = self.segments[segment].iter().position(|k| k == key) {
                self.segments[segment].remove(pos);
                self.segments[segment].push_back(key.clone());
            }
            
            self.promote(key, segment);
            
            Some(value.clone())
        } else {
            None
        }
    }
    
    fn put(&mut self, key: K, value: V) -> Option<(K, V)> {
        if self.cache.contains_key(&key) {
            if let Some((v, _)) = self.cache.get_mut(&key) {
                *v = value;
            }
            self.get(&key);
            return None;
        }
        
        let mut evicted = None;
        
        if self.cache.len() >= self.capacity {
            for segment in &mut self.segments {
                if let Some(evict_key) = segment.pop_front() {
                    if let Some((v, _)) = self.cache.remove(&evict_key) {
                        evicted = Some((evict_key, v));
                    }
                    break;
                }
            }
        }
        
        self.segments[0].push_back(key.clone());
        self.cache.insert(key, (value, 0));
        
        evicted
    }
    
    fn remove(&mut self, key: &K) -> Option<V> {
        if let Some((value, segment)) = self.cache.remove(key) {
            self.segments[segment].retain(|k| k != key);
            Some(value)
        } else {
            None
        }
    }
    
    fn clear(&mut self) {
        for segment in &mut self.segments {
            segment.clear();
        }
        self.cache.clear();
    }
    
    fn size(&self) -> usize {
        self.cache.len()
    }
    
    fn capacity(&self) -> usize {
        self.capacity
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_arc_cache() {
        let mut cache = AdaptiveReplacementCache::new(4);
        
        cache.put(1, "a");
        cache.put(2, "b");
        cache.put(3, "c");
        cache.put(4, "d");
        
        assert_eq!(cache.get(&2), Some("b"));
        
        cache.put(5, "e");
        
        assert_eq!(cache.get(&1), None);
        assert_eq!(cache.get(&2), Some("b"));
    }
    
    #[test]
    fn test_clock_pro_cache() {
        let mut cache = ClockProCache::new(4);
        
        cache.put(1, "a");
        cache.put(2, "b");
        cache.put(3, "c");
        cache.put(4, "d");
        
        assert_eq!(cache.get(&1), Some("a"));
        assert_eq!(cache.get(&2), Some("b"));
        
        cache.put(5, "e");
        
        assert_eq!(cache.size(), 4);
    }
    
    #[test]
    fn test_wtinylfu_cache() {
        let mut cache = WTinyLFU::new(100);
        
        for i in 0..50 {
            cache.put(i, i.to_string());
        }
        
        for _ in 0..10 {
            cache.get(&5);
            cache.get(&10);
        }
        
        for i in 50..100 {
            cache.put(i, i.to_string());
        }
        
        assert_eq!(cache.get(&5), Some("5".to_string()));
        assert_eq!(cache.get(&10), Some("10".to_string()));
    }
    
    #[test]
    fn test_segmented_lru() {
        let mut cache = SegmentedLRU::new(4, 2);
        
        cache.put(1, "a");
        cache.put(2, "b");
        cache.put(3, "c");
        cache.put(4, "d");
        
        assert_eq!(cache.get(&1), Some("a"));
        assert_eq!(cache.get(&1), Some("a"));
        
        cache.put(5, "e");
        
        assert_eq!(cache.get(&1), Some("a"));
        assert_eq!(cache.size(), 4);
    }
}