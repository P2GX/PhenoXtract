use crate::hgnc::enums::GeneQuery;
use crate::hgnc::error::HGNCError;
use crate::hgnc::json_schema::GeneDoc;

pub trait HGNCData {
    fn request_gene_data(&self, query: GeneQuery) -> Result<GeneDoc, HGNCError>;
    fn request_hgnc_id(&self, query: GeneQuery) -> Result<String, HGNCError> {
        let doc = self.request_gene_data(query)?;
        doc.hgnc_id_owned()
            .ok_or_else(|| HGNCError::MissingElementInDocument {
                desired_element: "HGNC ID".to_string(),
            })
    }
    fn request_gene_symbol(&self, query: GeneQuery) -> Result<String, HGNCError> {
        let doc = self.request_gene_data(query)?;
        doc.symbol_owned()
            .ok_or_else(|| HGNCError::MissingElementInDocument {
                desired_element: "symbol".to_string(),
            })
    }
    fn request_gene_identifier_pair(
        &self,
        query: GeneQuery,
    ) -> Result<(String, String), HGNCError> {
        let doc = self.request_gene_data(query)?;
        let (symbol, id) = doc.symbol_id_pair();
        let symbol = symbol.ok_or_else(|| HGNCError::MissingElementInDocument {
            desired_element: "symbol".to_string(),
        })?;
        let id = id.ok_or_else(|| HGNCError::MissingElementInDocument {
            desired_element: "HGNC ID".to_string(),
        })?;
        Ok((symbol, id))
    }
}
