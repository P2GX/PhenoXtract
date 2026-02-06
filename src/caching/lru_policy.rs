use crate::caching::traits::{CacheGetter, CacheRemovalPolicy};

pub struct LruPolicy;

impl<Key, Value, InnerCache: CacheGetter<Key, Value>> CacheRemovalPolicy<Key, Value, InnerCache>
    for LruPolicy
{
    fn get_expired_entries(&self, key: &InnerCache) -> &Key {
        todo!()
    }
}
