#![allow(dead_code)]
use crate::ontology::error::ClientError;
use directories::ProjectDirs;
use log::{debug, info};
use ratelimit::Ratelimiter;
use redb::{
    Database as RedbDatabase, DatabaseError, ReadableDatabase, TableDefinition, TypeName, Value,
};
use reqwest;
use reqwest::blocking::Client;
use serde::{Deserialize, Serialize};
use std::any::type_name;
use std::path::{Path, PathBuf};
use std::thread::sleep;
use std::time::Duration;

const TABLE: TableDefinition<&str, GeneResponse> = TableDefinition::new("hgnc_request_cache");

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct GeneResponse {
    #[serde(rename = "responseHeader")]
    pub response_header: ResponseHeader,
    pub response: Response,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ResponseHeader {
    pub status: i32,
    #[serde(rename = "QTime")]
    pub q_time: i32,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Response {
    #[serde(rename = "numFound")]
    pub num_found: i32,
    pub start: i32,
    #[serde(rename = "numFoundExact")]
    pub num_found_exact: bool,
    pub docs: Vec<GeneDoc>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[repr(C)]
pub struct GeneDoc {
    #[serde(default)]
    pub ena: Vec<String>,
    #[serde(default)]
    pub orphanet: Option<i64>,
    #[serde(default)]
    pub hgnc_id: Option<String>,
    #[serde(default)]
    pub pubmed_id: Vec<i64>,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub uuid: Option<String>,
    #[serde(default)]
    pub ensembl_gene_id: Option<String>,
    #[serde(default)]
    pub locus_group: Option<String>,
    #[serde(default)]
    pub mgd_id: Vec<String>,
    #[serde(default)]
    pub location: Option<String>,
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default)]
    pub cosmic: Option<String>,
    #[serde(default)]
    pub ucsc_id: Option<String>,
    #[serde(default)]
    pub date_name_changed: Option<String>,
    #[serde(default)]
    pub prev_name: Vec<String>,
    #[serde(default)]
    pub ccds_id: Vec<String>,
    #[serde(default)]
    pub mane_select: Vec<String>,
    #[serde(default)]
    pub refseq_accession: Vec<String>,
    #[serde(default)]
    pub rgd_id: Vec<String>,
    #[serde(default)]
    pub date_approved_reserved: Option<String>,
    #[serde(default)]
    pub entrez_id: Option<String>,
    #[serde(default)]
    pub uniprot_ids: Vec<String>,
    #[serde(default)]
    pub lsdb: Vec<String>,
    #[serde(default)]
    pub locus_type: Option<String>,
    #[serde(default)]
    pub gene_group: Vec<String>,
    #[serde(default)]
    pub alias_symbol: Vec<String>,
    #[serde(default)]
    pub agr: Option<String>,
    #[serde(default)]
    pub date_modified: Option<String>,
    #[serde(default)]
    pub omim_id: Vec<String>,
    #[serde(default)]
    pub gene_group_id: Vec<i32>,
    #[serde(default)]
    pub vega_id: Option<String>,
    #[serde(default)]
    pub symbol: Option<String>,
}

impl GeneResponse {
    fn as_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(self).unwrap()
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self, &'static str> {
        serde_json::from_slice(bytes).map_err(|_| "failed to decode json")
    }

    fn struct_name() -> String {
        type_name::<GeneResponse>()
            .split("::")
            .last()
            .unwrap()
            .to_string()
    }

    pub fn symbol_id_pair(&self) -> Vec<(&str, &str)> {
        self.response
            .docs
            .iter()
            .filter_map(|d| {
                if let Some(symbol) = d.symbol.as_ref()
                    && let Some(id) = d.hgnc_id.as_ref()
                {
                    Some((symbol.as_str(), id.as_str()))
                } else {
                    None
                }
            })
            .collect()
    }
}
impl Value for GeneResponse {
    type SelfType<'a> = GeneResponse;
    type AsBytes<'a> = Vec<u8>;

    fn fixed_width() -> Option<usize> {
        None
    }

    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        Self::from_bytes(data).unwrap()
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
    where
        Self: 'b,
    {
        value.as_bytes()
    }

    fn type_name() -> TypeName {
        TypeName::new(GeneResponse::struct_name().as_str())
    }
}

pub struct HGNCClient {
    rate_limiter: Ratelimiter,
    cache_file_path: PathBuf,
    api_url: String,
}

impl HGNCClient {
    fn new(
        rate_limiter: Ratelimiter,
        cache_file_path: PathBuf,
        api_url: String,
    ) -> Result<Self, ClientError> {
        Self::init_cache(&cache_file_path)?;
        Ok(HGNCClient {
            rate_limiter,
            cache_file_path,
            api_url,
        })
    }

    pub fn with_cache_dir(mut self, cache_dir: PathBuf) -> Result<Self, ClientError> {
        Self::init_cache(&cache_dir)?;
        self.cache_file_path = cache_dir.clone();
        Ok(self)
    }
    fn cache(&self) -> Result<RedbDatabase, DatabaseError> {
        RedbDatabase::create(&self.cache_file_path)
    }

    fn init_cache(cache_dir: &Path) -> Result<(), ClientError> {
        let cache = RedbDatabase::create(cache_dir)?;
        let write_txn = cache.begin_write()?;
        {
            write_txn.open_table(TABLE)?;
        }
        write_txn.commit()?;
        Ok(())
    }
    pub fn default_cache_dir() -> Option<PathBuf> {
        let pkg_name = env!("CARGO_PKG_NAME");

        ProjectDirs::from("", "", pkg_name)
            .map(|project_dirs| project_dirs.cache_dir().join("hgnc_cache"))
    }

    pub fn fetch_gene_data(&self, symbol: &str) -> Result<GeneResponse, ClientError> {
        let cache = self.cache()?;
        let cache_reader = cache.begin_read()?;
        let table = cache_reader.open_table(TABLE)?;

        if let Ok(Some(cache_entry)) = table.get(symbol) {
            debug!("Hit HGNC cache for {}", symbol);
            return Ok(cache_entry.value());
        }

        if let Err(duration) = self.rate_limiter.try_wait() {
            debug!("Waiting {:?} for rate limit", duration);
            sleep(duration);
        }
        let fetch_url = format!("{}fetch/symbol/{}", self.api_url, symbol);

        let client = Client::new();
        let response = client
            .get(fetch_url.clone())
            .header("User-Agent", "phenoxtractor")
            .header("Accept", "application/json")
            .send()?;

        let gene_response = response.json::<GeneResponse>()?;

        let cache_writer = cache.begin_write()?;
        {
            debug!("Caching response in HGNC cache for {}", symbol);
            let mut table = cache_writer.open_table(TABLE)?;
            table.insert(symbol, gene_response.clone())?;
        }
        cache_writer.commit()?;

        Ok(gene_response)
    }
}

impl Default for HGNCClient {
    fn default() -> Self {
        let rate_limiter = Ratelimiter::builder(10, Duration::from_secs(1))
            .max_tokens(10)
            .build()
            .expect("Building rate limiter failed");

        let cache_dir = Self::default_cache_dir().unwrap();
        info!("HGNC client cache dir: {:?}", cache_dir);
        HGNCClient {
            rate_limiter,
            cache_file_path: cache_dir,
            api_url: "https://rest.genenames.org/".to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::skip_in_ci;
    use rstest::rstest;
    use serial_test::serial;
    use tempfile::TempDir;

    fn build_client(cache_file_path: PathBuf) -> HGNCClient {
        let rate_limiter = Ratelimiter::builder(10, Duration::from_secs(1))
            .max_tokens(10)
            .build()
            .expect("Building rate limiter failed");
        HGNCClient::new(
            rate_limiter,
            cache_file_path,
            "https://rest.genenames.org/".to_string(),
        )
        .unwrap()
    }

    fn clear_cache(cache_dir: &PathBuf) {
        let cache = RedbDatabase::create(cache_dir).unwrap();
        let write_txn = cache.begin_write().unwrap();
        {
            let mut table = write_txn.open_table(TABLE).unwrap();
            table.retain(|_, _| false).unwrap();
        }
        write_txn.commit().unwrap();
    }

    #[rstest]
    #[serial]
    fn test_fetch_gene_data() {
        let temp_dir = TempDir::new().unwrap();
        let client = build_client(temp_dir.path().to_path_buf().join("hgnc_cache"));

        let symbol = "ZNF3";
        let gene_response = client.fetch_gene_data(symbol).unwrap();

        assert_eq!(
            gene_response.symbol_id_pair().first().unwrap(),
            &(symbol, "HGNC:13089")
        );
    }

    #[rstest]
    #[serial]
    fn test_fetch_gene_data_rate_limit() {
        skip_in_ci!();
        let temp_dir = TempDir::new().unwrap();
        let client = build_client(temp_dir.path().to_path_buf().join("hgnc_cache"));

        for _ in 0..50 {
            let _ = client.fetch_gene_data("ZNF3").unwrap();
            clear_cache(&client.cache_file_path);
        }
    }

    #[rstest]
    #[serial]
    fn test_cache() {
        let symbol = "CLOCK";
        let temp_dir = TempDir::new().unwrap();
        let client = build_client(temp_dir.path().to_path_buf().join("hgnc_cache"));

        let _ = client.fetch_gene_data(symbol).unwrap();

        let cache = RedbDatabase::create(&client.cache_file_path).unwrap();
        let cache_reader = cache.begin_read().unwrap();
        let table = cache_reader.open_table(TABLE).unwrap();

        if let Ok(Some(cache_entry)) = table.get(symbol) {
            let value = cache_entry.value();
            assert_eq!(
                value.symbol_id_pair().first().unwrap(),
                &(symbol, "HGNC:2082")
            );
        }
    }
}
