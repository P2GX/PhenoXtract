use crate::hgnc::enums::GeneQuery;
use crate::hgnc::error::HGNCError;
use crate::hgnc::json_schema::GeneDoc;
use crate::hgnc::traits::HGNCData;
use std::collections::HashMap;

/// A Mock client for the HGNC interface.
///
/// This struct is intended for use in unit testing. Instead of making live HTTP
/// requests to the HGNC API, it serves data from an internal `HashMap`.
/// This allows for deterministic testing of components that rely on `HGNCData`.
pub struct MockHGNCClient {
    docs: HashMap<String, GeneDoc>,
}

impl MockHGNCClient {
    pub fn new(docs: HashMap<String, GeneDoc>) -> MockHGNCClient {
        MockHGNCClient { docs }
    }
}

impl HGNCData for MockHGNCClient {
    fn request_gene_data(&self, query: GeneQuery) -> Result<GeneDoc, HGNCError> {
        let identifier = query.inner();
        self.docs
            .get(identifier)
            .cloned()
            .ok_or(HGNCError::UnexpectedNumberOfDocuments {
                identifier: identifier.to_string(),
                n_found: 0,
                n_expected: 1,
            })
    }
}

impl Default for MockHGNCClient {
    fn default() -> Self {
        let mut docs = HashMap::new();
        docs.insert(
            "BRCA1".to_string(),
            GeneDoc::default()
                .with_hgnc_id("HGNC:1100")
                .with_symbol("BRCA1"),
        );

        docs.insert(
            "HGNC:1100".to_string(),
            GeneDoc::default()
                .with_hgnc_id("HGNC:1100")
                .with_symbol("BRCA1"),
        );

        docs.insert(
            "HGNC:2082".to_string(),
            GeneDoc::default()
                .with_hgnc_id("HGNC:2082")
                .with_symbol("CLOCK"),
        );

        docs.insert(
            "CLOCK".to_string(),
            GeneDoc::default()
                .with_hgnc_id("HGNC:2082")
                .with_symbol("CLOCK"),
        );

        docs.insert(
            "HGNC:10848".to_string(),
            GeneDoc::default()
                .with_hgnc_id("HGNC:10848")
                .with_symbol("SHH"),
        );

        docs.insert(
            "SHH".to_string(),
            GeneDoc::default()
                .with_hgnc_id("HGNC:10848")
                .with_symbol("SHH"),
        );

        docs.insert(
            "HGNC:11251".to_string(),
            GeneDoc::default()
                .with_hgnc_id("HGNC:11251")
                .with_symbol("SPOCK1"),
        );

        docs.insert(
            "SPOCK1".to_string(),
            GeneDoc::default()
                .with_hgnc_id("HGNC:11251")
                .with_symbol("SPOCK1"),
        );

        MockHGNCClient::new(docs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn setup_mock() -> MockHGNCClient {
        let mut docs = HashMap::new();

        docs.insert(
            "BRCA1".to_string(),
            GeneDoc::default()
                .with_hgnc_id("HGNC:1100")
                .with_symbol("BRCA1"),
        );

        docs.insert(
            "HGNC:1100".to_string(),
            GeneDoc::default()
                .with_hgnc_id("HGNC:1100")
                .with_symbol("BRCA1"),
        );

        MockHGNCClient::new(docs)
    }

    #[test]
    fn test_request_gene_data_success() {
        let mock = setup_mock();
        let query = GeneQuery::Symbol("BRCA1");

        let result = mock.request_gene_data(query);
        assert!(result.is_ok());

        let doc = result.unwrap();
        assert_eq!(doc.symbol, Some("BRCA1".to_string()));
    }

    #[test]
    fn test_request_gene_data_not_found() {
        let mock = setup_mock();
        let query = GeneQuery::Symbol("UNKNOWN_GENE");

        let result = mock.request_gene_data(query);
        assert!(result.is_err());

        if let Err(HGNCError::UnexpectedNumberOfDocuments {
            identifier,
            n_found,
            ..
        }) = result
        {
            assert_eq!(identifier, "UNKNOWN_GENE");
            assert_eq!(n_found, 0);
        } else {
            panic!("Returned wrong error type");
        }
    }

    #[test]
    fn test_request_hgnc_id_success() {
        let mock = setup_mock();
        let result = mock.request_hgnc_id(GeneQuery::HgncId("BRCA1"));
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "HGNC:1100");
    }

    #[test]
    fn test_request_gene_symbol_success() {
        let mock = setup_mock();
        let result = mock
            .request_gene_symbol(GeneQuery::HgncId("HGNC:1100"))
            .unwrap();
        assert_eq!(result, "BRCA1");
    }

    #[test]
    fn test_request_pair_success() {
        let mock = setup_mock();
        let query = GeneQuery::Symbol("BRCA1");

        let result = mock.request_gene_identifier_pair(query);
        assert!(result.is_ok());

        let (symbol, id) = result.unwrap();
        assert_eq!(symbol, "BRCA1");
        assert_eq!(id, "HGNC:1100");
    }
}
