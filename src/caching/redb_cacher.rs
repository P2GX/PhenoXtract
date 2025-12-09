use crate::caching::error::CacherError;
use crate::caching::traits::Cacheable;
use directories::ProjectDirs;
use redb::{
    Database as RedbDatabase, Database, DatabaseError, ReadableDatabase, TableDefinition, TypeName,
    Value,
};
use std::any::type_name;
use std::env::home_dir;
use std::fs;
use std::marker::PhantomData;
use std::path::PathBuf;
use crate::hgnc::GeneDoc;

macro_rules! implement_value_for_local_type {
    ($type_name:ty) => {
        impl Value for $type_name {
            type SelfType<'a> = $type_name;
            type AsBytes<'a> = Vec<u8>;

            fn fixed_width() -> Option<usize> {
                None
            }

            fn from_bytes<'a>(data: &[u8]) -> Self::SelfType<'a> {
                serde_json::from_slice(data).expect("Could not convert to bytes.")
            }

            fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
            where
                Self: 'b,
            {
                serde_json::to_vec(value).unwrap()
            }

            fn type_name() -> TypeName {
                TypeName::new(type_name::<$type_name>())
            }
        }
    };
}

implement_value_for_local_type!(GeneDoc);

impl Cacheable for GeneDoc {
    fn keys(&self) -> Vec<&str> {
        let mut keys = vec![];
        if let Some(symbol) = self.symbol() {
            keys.push(symbol);
        }
        if let Some(id) = self.hgnc_id() {
            keys.push(id);
        }
        keys
    }
}

/// Given an object T that implements Cacheable,
/// the RedbCacher will be able to cache instances of T to a RedbDatabase at cache_file_path.
///
/// NOTE: in the RedbDatabase, a single table will be automatically constructed for the type T.
/// If the user would like to have multiple caches of type T, then a different file path would have to be used.
pub(crate) struct RedbCacher<T: Cacheable> {
    cache_file_path: PathBuf,
    _phantom: PhantomData<T>,
}

impl<T: Cacheable> Default for RedbCacher<T> {
    fn default() -> Self {
        let pkg_name = env!("CARGO_PKG_NAME");

        let phenox_cache_dir = ProjectDirs::from("", "", pkg_name)
            .map(|project_dir| project_dir.cache_dir().to_path_buf())
            .or_else(|| home_dir().map(|home| home.join(pkg_name)))
            .unwrap_or_else(|| panic!("Could not find cache directory or home directory."));

        if !phenox_cache_dir.exists() {
            fs::create_dir_all(&phenox_cache_dir)
                .expect("Failed to create default cache directory.");
        }

        RedbCacher::new(phenox_cache_dir.join(type_name::<T>()))
    }
}

impl<T: Cacheable> RedbCacher<T> {
    pub(crate) fn new(cache_file_path: PathBuf) -> Self {
        RedbCacher {
            cache_file_path,
            _phantom: PhantomData,
        }
    }

    fn table_definition() -> TableDefinition<'static, &'static str, T> {
        TableDefinition::new(type_name::<T>())
    }

    pub(crate) fn cache_file_path(&self) -> &PathBuf {
        &self.cache_file_path
    }

    pub(crate) fn init_cache(&self) -> Result<(), CacherError> {
        let cache = RedbDatabase::create(self.cache_file_path.clone())?;

        let write_txn = cache.begin_write()?;
        {
            write_txn.open_table(Self::table_definition())?;
        }
        write_txn.commit()?;
        Ok(())
    }

    pub(crate) fn open_cache(&self) -> Result<RedbDatabase, DatabaseError> {
        RedbDatabase::open(&self.cache_file_path)
    }
    pub(crate) fn find_cache_entry(&self, query: &str, cache: &Database) -> Option<T> {
        let cache_reader = cache.begin_read().ok()?;
        let table = cache_reader.open_table(Self::table_definition()).ok()?;

        if let Ok(Some(cache_entry)) = table.get(query) {
            return Some(cache_entry.value().into());
        }

        None
    }

    pub(crate) fn cache_object(
        &self,
        object_to_cache: T,
        cache: &Database,
    ) -> Result<(), CacherError> {
        let cache_writer = cache.begin_write()?;
        {
            let mut table = cache_writer.open_table(Self::table_definition())?;
            for key in object_to_cache.keys() {
                table.insert(key, object_to_cache.clone())?;
            }
        }
        cache_writer.commit()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::{fixture, rstest};
    use serde::{Deserialize, Serialize};
    use tempfile::TempDir;

    #[fixture]
    fn temp_dir() -> TempDir {
        tempfile::tempdir().expect("Failed to create temporary directory")
    }

    #[derive(Debug, Serialize, Deserialize, Clone)]
    struct MyFavouriteStruct {
        name: String,
        favourite_colour: String,
        favourite_number: i32,
        likes_cats: bool,
    }

    implement_value_for_local_type!(MyFavouriteStruct);

    impl Cacheable for MyFavouriteStruct {
        fn keys(&self) -> Vec<&str> {
            vec![self.name.as_str()]
        }
    }

    #[fixture]
    fn my_favourite_struct_alice() -> MyFavouriteStruct {
        MyFavouriteStruct {
            name: "alice mchale".to_string(),
            favourite_colour: "turquoise".to_string(),
            favourite_number: 314,
            likes_cats: false,
        }
    }

    #[fixture]
    fn my_favourite_struct_bob() -> MyFavouriteStruct {
        MyFavouriteStruct {
            name: "bob jones".to_string(),
            favourite_colour: "red".to_string(),
            favourite_number: 42,
            likes_cats: true,
        }
    }

    #[rstest]
    fn test_cache(temp_dir: TempDir) {
        let cache_file_path = temp_dir.path().join("cache.my_favourite_struct");
        let cacher = RedbCacher::<MyFavouriteStruct>::new(cache_file_path);

        cacher.init_cache().unwrap();
        let cache = cacher.open_cache().unwrap();

        cacher
            .cache_object(my_favourite_struct_alice(), &cache)
            .unwrap();
        cacher
            .cache_object(my_favourite_struct_bob(), &cache)
            .unwrap();

        let cached_alice = cacher.find_cache_entry("alice mchale", &cache).unwrap();
        assert!(!cached_alice.likes_cats);

        let cached_bob = cacher.find_cache_entry("bob jones", &cache).unwrap();
        assert!(cached_bob.likes_cats);

        assert!(cacher.find_cache_entry("janet smith", &cache).is_none());
    }

    #[rstest]
    fn test_cache_overwrite(temp_dir: TempDir) {
        let cache_file_path = temp_dir.path().join("cache.my_favourite_struct");
        let cacher = RedbCacher::<MyFavouriteStruct>::new(cache_file_path);

        cacher.init_cache().unwrap();
        let cache = cacher.open_cache().unwrap();

        cacher
            .cache_object(my_favourite_struct_alice(), &cache)
            .unwrap();

        let cached_alice = cacher.find_cache_entry("alice mchale", &cache).unwrap();
        assert!(!cached_alice.likes_cats);

        let alice_opinion_changed = MyFavouriteStruct {
            name: "alice mchale".to_string(),
            favourite_colour: "turquoise".to_string(),
            favourite_number: 314,
            likes_cats: true,
        };

        cacher.cache_object(alice_opinion_changed, &cache).unwrap();

        let cached_alice = cacher.find_cache_entry("alice mchale", &cache).unwrap();
        assert!(cached_alice.likes_cats);
    }
}
