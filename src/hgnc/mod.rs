//! # HGNC
//!
//! This module exposes the public types and traits for requesting gene data from HGNC.
//!
//! # [`GeneQuery`]
//!
//! An enum with two variants: Symbol and HgncId. This enum can be used to query HGNC for data.
//! Variants:
//! - `GeneQuery::Symbol(&str)` — query by gene symbol
//! - `GeneQuery::HgncId(&str)` — query by HGNC ID
//!
//! # [`GeneDoc`]
//!
//! The full data on the gene provided by HGNC.
//!
//! # [`HGNCData`]
//!
//! A trait consisting of the following methods:
//!
//! - `request_gene_data(&self, query: GeneQuery) -> Result<GeneDoc, HGNCError>` — fetches the full GeneDoc for a given gene.
//! - `request_hgnc_id(query: GeneQuery) -> Result<String, HGNCError>` — returns the HGNC ID for a gene.
//! - `request_gene_symbol(query: GeneQuery) Result<String, HGNCError>` — returns the gene symbol for a gene.
//! - `request_gene_identifier_pair(query: GeneQuery) Result<(String, String), HGNCError>` — returns the symbol, HGNC ID pair for a given gene.
//!
//! # [`HGNCClient`]
//!
//! The basic implementation of the HGNCData trait. Request a GeneDoc from the HGNC API.
//!
//! # [`CachedHGNCClient`]
//!
//! A cached implementation of the HGNCData trait. The GeneDocs will be cached and can thereafter be accessed without an API call.
//!
//! # [`MockHGNCClient`]
//!
//! A mocked implementation of the HGNCData trait for tests and CI.
//!
//! # [`HGNCError`]
//!
//! An enum for errors returned by the API.
//!
//! # Examples
//!
//! ## HGNCClient
//!
//! ```rust
//! use pivot::hgnc::{HGNCClient, HGNCData, GeneQuery};
//!
//! let client = HGNCClient::default();
//! let gene_symbol = client.request_gene_symbol(GeneQuery::HgncId("HGNC:13089")).unwrap();
//! let expected = "ZNF3".to_string();
//! assert_eq!(gene_symbol,expected);
//! ```
//!
//! ## CachedHGNCClient
//!
//! ```rust
//! use pivot::hgnc::{HGNCClient, HGNCData, GeneQuery, CachedHGNCClient};
//!
//! let temp_dir = tempfile::tempdir().expect("Failed to create temporary directory");
//! let cache_file_path = temp_dir.path().join("cache.hgnc");
//!
//! let client = CachedHGNCClient::new(cache_file_path, HGNCClient::default()).unwrap();
//! let gene_doc = client.request_gene_data(GeneQuery::HgncId("HGNC:13089")).unwrap();
//! let expected_location = Some("7q22.1".to_string());
//! assert_eq!(gene_doc.location,expected_location);
//!
//! // if we request gene data again, the HGNC API will not be used, as the GeneDoc has been cached
//! let gene_doc = client.request_gene_data(GeneQuery::HgncId("HGNC:13089")).unwrap();
//! ```

pub use cached_hgnc_client::CachedHGNCClient;
pub use enums::GeneQuery;
pub use error::HGNCError;
pub use hgnc_client::HGNCClient;
pub use json_schema::GeneDoc;
pub use mock_hgnc_client::MockHGNCClient;
pub use traits::HGNCData;
mod cached_hgnc_client;
mod enums;
mod error;
mod hgnc_client;
mod json_schema;
mod mock_hgnc_client;
mod traits;
