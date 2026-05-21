use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::hash::{Hash, Hasher};
use std::ops::Deref;

#[derive(Debug, Clone, PartialEq, Deserialize, Default, Serialize, Eq)]
pub struct HashableSet<T: Hash + PartialEq + Eq + Serialize>(HashSet<T>);

impl<T: Hash + Serialize + Eq> Deref for HashableSet<T> {
    type Target = HashSet<T>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T: Hash + Serialize + Eq> From<HashSet<T>> for HashableSet<T> {
    fn from(value: HashSet<T>) -> Self {
        HashableSet(value)
    }
}

impl<T: Hash + Serialize + Eq> From<Vec<T>> for HashableSet<T> {
    fn from(value: Vec<T>) -> Self {
        HashableSet(value.into_iter().collect())
    }
}

impl<T: Hash + Eq + Serialize> Hash for HashableSet<T> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let mut hashes: Vec<u64> = self
            .0
            .iter()
            .map(|item| {
                let mut h = std::collections::hash_map::DefaultHasher::new();
                item.hash(&mut h);
                h.finish()
            })
            .collect();
        hashes.sort_unstable();
        hashes.hash(state);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    fn compute_hash<T: Hash>(value: &T) -> u64 {
        let mut hasher = DefaultHasher::new();
        value.hash(&mut hasher);
        hasher.finish()
    }

    #[test]
    fn from_hashset_roundtrip() {
        let hs: HashSet<i32> = [1, 2, 3].iter().cloned().collect();
        let wrapped = HashableSet::from(hs.clone());
        assert_eq!(*wrapped, hs);
    }

    #[test]
    fn from_vec_deduplicates() {
        let wrapped: HashableSet<i32> = vec![1, 2, 2, 3, 3, 3].into();
        assert_eq!(wrapped.len(), 3);
        assert!(wrapped.contains(&1));
        assert!(wrapped.contains(&2));
        assert!(wrapped.contains(&3));
    }

    #[test]
    fn from_empty_vec() {
        let wrapped: HashableSet<i32> = vec![].into();
        assert!(wrapped.is_empty());
    }

    #[test]
    fn deref_exposes_hashset_methods() {
        let wrapped: HashableSet<i32> = vec![10, 20, 30].into();
        assert!(wrapped.contains(&10));
        assert!(!wrapped.contains(&99));
        assert_eq!(wrapped.len(), 3);
    }

    #[test]
    fn same_elements_same_hash() {
        let a: HashableSet<i32> = vec![1, 2, 3].into();
        let b: HashableSet<i32> = vec![3, 1, 2].into(); // different insertion order
        assert_eq!(compute_hash(&a), compute_hash(&b));
    }

    #[test]
    fn different_elements_different_hash() {
        let a: HashableSet<i32> = vec![1, 2, 3].into();
        let b: HashableSet<i32> = vec![1, 2, 4].into();
        assert_ne!(compute_hash(&a), compute_hash(&b));
    }

    #[test]
    fn empty_sets_have_equal_hashes() {
        let a: HashableSet<i32> = vec![].into();
        let b: HashableSet<i32> = vec![].into();
        assert_eq!(compute_hash(&a), compute_hash(&b));
    }

    #[test]
    fn subset_has_different_hash() {
        let a: HashableSet<i32> = vec![1, 2, 3].into();
        let b: HashableSet<i32> = vec![1, 2].into();
        assert_ne!(compute_hash(&a), compute_hash(&b));
    }

    #[test]
    fn hashable_set_usable_as_hashmap_key() {
        let mut map: std::collections::HashMap<HashableSet<i32>, &str> =
            std::collections::HashMap::new();
        let key: HashableSet<i32> = vec![1, 2, 3].into();
        map.insert(key.clone(), "hello");
        assert_eq!(map[&key], "hello");

        let same_key: HashableSet<i32> = vec![3, 2, 1].into();
        assert_eq!(map[&same_key], "hello");
    }

    #[test]
    fn equal_sets_regardless_of_order() {
        let a: HashableSet<i32> = vec![1, 2, 3].into();
        let b: HashableSet<i32> = vec![3, 2, 1].into();
        assert_eq!(a, b);
    }

    #[test]
    fn unequal_sets_are_not_equal() {
        let a: HashableSet<i32> = vec![1, 2, 3].into();
        let b: HashableSet<i32> = vec![1, 2].into();
        assert_ne!(a, b);
    }

    #[test]
    fn serializes_and_deserializes() {
        let original: HashableSet<i32> = vec![1, 2, 3].into();
        let json = serde_json::to_string(&original).unwrap();
        let restored: HashableSet<i32> = serde_json::from_str(&json).unwrap();
        assert_eq!(original, restored);
    }

    #[test]
    fn deserializes_from_json_array() {
        let restored: HashableSet<i32> = serde_json::from_str("[1,2,3]").unwrap();
        assert!(restored.contains(&1));
        assert!(restored.contains(&2));
        assert!(restored.contains(&3));
    }

    #[test]
    fn default_is_empty() {
        let s: HashableSet<i32> = HashableSet::default();
        assert!(s.is_empty());
    }

    #[test]
    fn clone_is_equal_and_independent() {
        let a: HashableSet<i32> = vec![1, 2, 3].into();
        let b = a.clone();
        assert_eq!(a, b);
        assert_eq!(compute_hash(&a), compute_hash(&b));
    }
}
