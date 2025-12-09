use crate::hgnc::enums::GeneQuery;
use crate::hgnc::error::HGNCError;
use crate::hgnc::json_schema::{GeneDoc, GeneResponse};
use crate::hgnc::traits::HGNCData;
use ratelimit::Ratelimiter;
use reqwest::blocking::Client;
use std::fmt::{Debug, Formatter};
use std::thread::sleep;
use std::time::Duration;

pub struct HGNCClient {
    rate_limiter: Ratelimiter,
    api_url: String,
    client: Client,
}

impl HGNCClient {
    pub fn new(rate_limiter: Ratelimiter, api_url: String) -> Self {
        HGNCClient {
            rate_limiter,
            api_url,
            client: Client::new(),
        }
    }

    fn fetch_request(&self, url: String) -> Result<Vec<GeneDoc>, HGNCError> {
        if let Err(duration) = self.rate_limiter.try_wait() {
            sleep(duration);
        }
        let response = self
            .client
            .get(url.clone())
            .header("User-Agent", "PIVOT")
            .header("Accept", "application/json")
            .send()?;

        let gene_response = response.json::<GeneResponse>()?;

        Ok(gene_response.response.docs)
    }
}

impl HGNCData for HGNCClient {
    fn request_gene_data(&self, query: GeneQuery) -> Result<GeneDoc, HGNCError> {
        let fetch_url = match &query {
            GeneQuery::Symbol(symbol) => format!("{}fetch/symbol/{}", self.api_url, symbol),
            GeneQuery::HgncId(id) => format!("{}fetch/hgnc_id/{}", self.api_url, id),
        };
        let docs = self.fetch_request(fetch_url)?;

        if docs.len() == 1 {
            Ok(docs.first().unwrap().clone())
        } else {
            Err(HGNCError::UnexpectedNumberOfDocuments {
                identifier: query.inner().to_string(),
                n_found: docs.len(),
                n_expected: 1,
            })
        }
    }
}

impl Default for HGNCClient {
    fn default() -> Self {
        let rate_limiter = Ratelimiter::builder(10, Duration::from_secs(1))
            .max_tokens(10)
            .build()
            .expect("Building rate limiter failed");

        HGNCClient::new(rate_limiter, "https://rest.genenames.org/".to_string())
    }
}

impl Debug for HGNCClient {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HGNCClient")
            .field("api_url", &self.api_url)
            .field("rate_limiter", &"<Ratelimiter>")
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    #[rstest]
    #[case(GeneQuery::Symbol("ZNF3"), "ZNF3", "HGNC:13089")]
    #[case(GeneQuery::HgncId("HGNC:13089"), "ZNF3", "HGNC:13089")]
    fn test_request_gene_data(
        #[case] query: GeneQuery,
        #[case] expected_symbol: String,
        #[case] expected_hgnc_id: String,
    ) {
        let client = HGNCClient::default();

        let gene_doc = client.request_gene_data(query).unwrap();

        assert_eq!(gene_doc.hgnc_id, Some(expected_hgnc_id));
        assert_eq!(gene_doc.symbol, Some(expected_symbol));
    }

    #[rstest]
    #[case(GeneQuery::Symbol("ZNF3"), ("ZNF3", "HGNC:13089"))]
    #[case(GeneQuery::HgncId("HGNC:13089"), ("ZNF3", "HGNC:13089"))]
    fn test_request_gene_identifier_pair(
        #[case] query: GeneQuery,
        #[case] expected_pair: (&str, &str),
    ) {
        let client = HGNCClient::default();

        let gene_doc = client.request_gene_identifier_pair(query).unwrap();

        assert_eq!(gene_doc.0, expected_pair.0);
        assert_eq!(gene_doc.1, expected_pair.1);
    }

    #[rstest]
    fn test_request_hgnc_id() {
        let client = HGNCClient::default();
        let hgnc_id = client.request_hgnc_id(GeneQuery::Symbol("CLOCK")).unwrap();
        assert_eq!(hgnc_id.as_str(), "HGNC:2082");
    }

    #[rstest]
    fn test_request_gene_symbol() {
        let client = HGNCClient::default();
        let gene_symbol = client
            .request_gene_symbol(GeneQuery::HgncId("HGNC:2082"))
            .unwrap();
        assert_eq!(gene_symbol.as_str(), "CLOCK");
    }
}
