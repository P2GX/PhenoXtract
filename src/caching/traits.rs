use crate::caching::error::CacheError;

pub trait Caching<Key, Value> {
    fn write(&mut self, key: &Key, value: &Value) -> Result<(), CacheError>;

    fn read(&self, key: &Key) -> Result<&Value, CacheError>;
}
