use serde::{Deserialize, Serialize};
use std::fmt::Debug;

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

#[derive(Debug, Serialize, Deserialize, Clone, Default, PartialEq)]
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

impl GeneDoc {
    pub fn hgnc_id_owned(&self) -> Option<String> {
        self.hgnc_id.clone()
    }

    pub fn hgnc_id(&self) -> Option<&str> {
        self.hgnc_id.as_deref()
    }

    pub fn symbol_owned(&self) -> Option<String> {
        self.symbol.clone()
    }

    pub fn symbol(&self) -> Option<&str> {
        self.symbol.as_deref()
    }

    pub fn symbol_id_pair(&self) -> (Option<String>, Option<String>) {
        (self.symbol_owned(), self.hgnc_id_owned())
    }

    pub fn with_hgnc_id(mut self, hgnc_id: impl Into<String>) -> Self {
        self.hgnc_id = Some(hgnc_id.into());
        self
    }

    pub fn with_symbol(mut self, symbol: impl Into<String>) -> Self {
        self.symbol = Some(symbol.into());
        self
    }
}
