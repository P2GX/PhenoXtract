use std::collections::HashMap;
use std::hash::Hash;

pub trait CacheRemovalPolicy<Key, Value, InnerCache: CacheGetter<Key, Value>> {
    fn get_expired_entries(&self, inner: &InnerCache) -> &Key;
}

pub trait CacheGetter<Key, Value> {
    fn get(&self, key: &Key) -> Option<&Value>;
}

impl<Key, Value> CacheGetter<Key, Value> for HashMap<Key, Value>
where
    Key: Eq + Hash,
{
    fn get(&self, key: &Key) -> Option<&Value> {
        self.get(key)
    }
}
