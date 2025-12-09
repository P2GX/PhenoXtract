use crate::caching::redb_cacher::RedbCacher;
use std::path::PathBuf;
use pivot::{HGVSClient, HGVSData, HgvsVariant, SingleVariantResponse};
use crate::hgvs::error::HGVSError;

#[derive(Default)]
pub struct CachedHGVSClient {
    cacher: RedbCacher<HgvsVariant>,
    hgvs_client: HGVSClient,
}

impl CachedHGVSClient {
    pub fn new(cache_file_path: PathBuf, hgvs_client: HGVSClient) -> Result<Self, HGVSError> {
        let cacher = RedbCacher::new(cache_file_path);
        cacher.init_cache()?;
        Ok(CachedHGVSClient {
            cacher,
            hgvs_client,
        })
    }
}

impl HGVSData for CachedHGVSClient {
    fn get_full_validated_hgvs_data(&self, unvalidated_hgvs: &str) -> Result<SingleVariantResponse, pivot::HGVSError> {
        let cache = self.cacher.open_cache()?;
        if let Some(hgvs_variant) = self.cacher.find_cache_entry(unvalidated_hgvs, &cache) {
            return Ok(hgvs_variant);
        }

        let hgvs_variant = self
            .hgvs_client
            .request_and_validate_hgvs(unvalidated_hgvs)?;
        self.cacher.cache_object(hgvs_variant.clone(), &cache)?;
        Ok(hgvs_variant.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::caching::traits::Cacheable;
    use redb::{Database as RedbDatabase, ReadableDatabase};
    use rstest::{fixture, rstest};
    use tempfile::TempDir;

    #[fixture]
    fn temp_dir() -> TempDir {
        tempfile::tempdir().expect("Failed to create temporary directory")
    }

    #[rstest]
    fn test_request_and_validate_hgvs(temp_dir: TempDir) {
        let unvalidated_hgvs = "NM_001173464.1:c.2860C>T";
        let cache_file_path = temp_dir.path().join("cache.hgvs");
        let client = CachedHGVSClient::new(cache_file_path, HGVSClient::default()).unwrap();

        let validated_hgvs = client.request_and_validate_hgvs(unvalidated_hgvs).unwrap();
        assert_eq!(validated_hgvs.transcript_hgvs(), unvalidated_hgvs);
    }

    #[rstest]
    fn test_cache(temp_dir: TempDir) {
        let unvalidated_hgvs = "NM_001173464.1:c.2860C>T";
        let cache_file_path = temp_dir.path().join("cache.hgvs");
        let client = CachedHGVSClient::new(cache_file_path, HGVSClient::default()).unwrap();

        client.request_and_validate_hgvs(unvalidated_hgvs).unwrap();

        let cache = client.cacher.open_cache().unwrap();
        let cached_hgvs = client
            .cacher
            .find_cache_entry(unvalidated_hgvs, &cache)
            .unwrap();
        assert_eq!(cached_hgvs.transcript_hgvs(), unvalidated_hgvs);
    }
}
