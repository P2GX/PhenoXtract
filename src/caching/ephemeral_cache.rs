use crate::caching::error::CacheError;
use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;

#[derive(Debug)]
pub struct EphemeralCache<Key, Value>
where
    Key: Eq + Hash,
{
    cache: HashMap<Key, Value>,
}

impl<Key, Value> EphemeralCache<Key, Value>
where
    Key: Eq + Hash + Clone + Debug,
    Value: Clone,
{
    pub fn new(inner: HashMap<Key, Value>) -> Self {
        EphemeralCache { cache: inner }
    }
    fn write(&mut self, key: &Key, value: &Value) -> Result<(), CacheError> {
        self.cache.insert(key.clone(), value.clone());

        Ok(())
    }

    fn read(&self, key: &Key) -> Result<&Value, CacheError> {
        match self.cache.get(key) {
            None => Err(CacheError::ReadError {
                reason: format!("No cache entry found for key: {:?}", key),
            }),
            Some(value) => Ok(value),
        }
    }
}

impl<Key: Eq + Hash, Value> Default for EphemeralCache<Key, Value> {
    fn default() -> Self {
        EphemeralCache {
            cache: HashMap::new(),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_default_cache_is_empty() {
        let cache = EphemeralCache::<String, String>::new(HashMap::new());
        let result = cache.read(&"nonexistent".to_string());

        assert!(result.is_err());
        match result {
            Err(CacheError::ReadError { reason }) => {
                assert!(reason.contains("No cache entry found"));
            }
            _ => panic!("Expected ReadError"),
        }
    }

    #[test]
    fn test_write_and_read_string() {
        let mut cache = EphemeralCache::<String, String>::new(HashMap::new());
        let key = "test_key".to_string();
        let value = "test_value".to_string();

        let write_result = cache.write(&key, &value);
        assert!(write_result.is_ok());

        let read_result = cache.read(&key);
        assert!(read_result.is_ok());
        assert_eq!(read_result.unwrap(), &value);
    }

    #[test]
    fn test_write_and_read_integer() {
        let mut cache = EphemeralCache::<i32, String>::new(HashMap::new());
        let key = 42;
        let value = "forty-two".to_string();

        cache.write(&key, &value).unwrap();
        assert_eq!(cache.read(&key).unwrap(), &value);
    }

    #[test]
    fn test_overwrite_existing_key() {
        let mut cache = EphemeralCache::<String, i32>::new(HashMap::new());
        let key = "counter".to_string();

        cache.write(&key, &1).unwrap();
        assert_eq!(cache.read(&key).unwrap(), &1);

        cache.write(&key, &2).unwrap();
        assert_eq!(cache.read(&key).unwrap(), &2);
    }

    #[test]
    fn test_read_nonexistent_key() {
        let cache = EphemeralCache::<String, i32>::new(HashMap::new());
        let result = cache.read(&"missing".to_string());

        assert!(result.is_err());
        match result {
            Err(CacheError::ReadError { reason }) => {
                assert!(reason.contains("No cache entry found"));
                assert!(reason.contains("missing"));
            }
            _ => panic!("Expected ReadError"),
        }
    }

    #[test]
    fn test_multiple_keys() {
        let mut cache = EphemeralCache::<String, i32>::new(HashMap::new());

        cache.write(&"one".to_string(), &1).unwrap();
        cache.write(&"two".to_string(), &2).unwrap();
        cache.write(&"three".to_string(), &3).unwrap();

        assert_eq!(cache.read(&"one".to_string()).unwrap(), &1);
        assert_eq!(cache.read(&"two".to_string()).unwrap(), &2);
        assert_eq!(cache.read(&"three".to_string()).unwrap(), &3);
    }

    #[test]
    fn test_complex_value_types() {
        #[derive(Clone, Debug, PartialEq, Default)]
        struct ComplexData {
            id: u32,
            name: String,
            values: Vec<i32>,
        }

        let mut cache = EphemeralCache::<String, ComplexData>::new(HashMap::new());
        let key = "complex".to_string();
        let value = ComplexData {
            id: 1,
            name: "test".to_string(),
            values: vec![1, 2, 3],
        };

        cache.write(&key, &value).unwrap();
        let retrieved = cache.read(&key).unwrap();

        assert_eq!(retrieved.id, value.id);
        assert_eq!(retrieved.name, value.name);
        assert_eq!(retrieved.values, value.values);
    }

    #[test]
    fn test_cache_with_tuple_keys() {
        let mut cache = EphemeralCache::<(String, i32), String>::new(HashMap::new());
        let key = ("user".to_string(), 123);
        let value = "data".to_string();

        cache.write(&key, &value).unwrap();
        assert_eq!(cache.read(&key).unwrap(), &value);
    }

    #[test]
    fn test_reference_returned_is_valid() {
        let mut cache = EphemeralCache::<String, Vec<i32>>::new(HashMap::new());
        let key = "numbers".to_string();
        let value = vec![1, 2, 3, 4, 5];

        cache.write(&key, &value).unwrap();

        let reference = cache.read(&key).unwrap();
        assert_eq!(reference.len(), 5);
        assert_eq!(reference[0], 1);
    }

    #[test]
    fn test_error_message_contains_key_debug_output() {
        let cache = EphemeralCache::<i32, String>::new(HashMap::new());
        let key = 999;

        let result = cache.read(&key);
        match result {
            Err(CacheError::ReadError { reason }) => {
                assert!(reason.contains("999"));
            }
            _ => panic!("Expected ReadError with key in message"),
        }
    }
}
