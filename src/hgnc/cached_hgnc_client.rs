use crate::caching::redb_cacher::RedbCacher;
use crate::hgnc::enums::GeneQuery;
use crate::hgnc::error::HGNCError;
use crate::hgnc::hgnc_client::HGNCClient;
use crate::hgnc::json_schema::GeneDoc;
use crate::hgnc::traits::HGNCData;
use std::fmt::{Debug, Formatter};
use std::path::PathBuf;

#[derive(Default)]
pub struct CachedHGNCClient {
    cacher: RedbCacher<GeneDoc>,
    hgnc_client: HGNCClient,
}

impl HGNCData for CachedHGNCClient {
    fn request_gene_data(&self, query: GeneQuery) -> Result<GeneDoc, HGNCError> {
        let cache = self.cacher.open_cache()?;
        if let Some(gene_doc) = self.cacher.find_cache_entry(query.inner(), &cache) {
            return Ok(gene_doc);
        }

        let doc = self.hgnc_client.request_gene_data(query)?;
        self.cacher.cache_object(doc.clone(), &cache)?;
        Ok(doc)
    }
}

impl CachedHGNCClient {
    pub fn new(cache_file_path: PathBuf, hgnc_client: HGNCClient) -> Result<Self, HGNCError> {
        let cacher = RedbCacher::new(cache_file_path);
        cacher.init_cache()?;
        Ok(CachedHGNCClient {
            cacher,
            hgnc_client,
        })
    }
}

impl Debug for CachedHGNCClient {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HGNCClient")
            .field("cache_file_path", &self.cacher.cache_file_path())
            .field("api_url", &self.hgnc_client)
            .field("rate_limiter", &"<Ratelimiter>")
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::{fixture, rstest};
    use tempfile::TempDir;

    #[fixture]
    fn temp_dir() -> TempDir {
        tempfile::tempdir().expect("Failed to create temporary directory")
    }

    #[rstest]
    fn test_cache(temp_dir: TempDir) {
        let symbol = "CLOCK";
        let cache_file_path = temp_dir.path().join("cache.hgnc");
        let client = CachedHGNCClient::new(cache_file_path, HGNCClient::default()).unwrap();

        client.request_gene_data(GeneQuery::Symbol(symbol)).unwrap();

        let cache = client.cacher.open_cache().unwrap();
        let cached_gene_doc = client.cacher.find_cache_entry(symbol, &cache).unwrap();
        assert_eq!(cached_gene_doc.symbol, Some(symbol.to_string()));
        assert_eq!(cached_gene_doc.hgnc_id, Some("HGNC:2082".to_string()));
    }

    #[rstest]
    #[case(GeneQuery::Symbol("ZNF3"), ("ZNF3", "HGNC:13089"))]
    #[case(GeneQuery::HgncId("HGNC:13089"), ("ZNF3", "HGNC:13089"))]
    fn test_request_gene_identifier_pair(
        #[case] query: GeneQuery,
        #[case] expected_pair: (&str, &str),
        temp_dir: TempDir,
    ) {
        let cache_file_path = temp_dir.path().join("cache.hgnc");
        let client = CachedHGNCClient::new(cache_file_path, HGNCClient::default()).unwrap();

        let gene_doc = client.request_gene_identifier_pair(query).unwrap();

        assert_eq!(gene_doc.0, expected_pair.0);
        assert_eq!(gene_doc.1, expected_pair.1);
    }
}
