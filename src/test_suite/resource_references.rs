use crate::ontology::traits::{HasPrefixId, HasVersion};
use crate::ontology::{DatabaseRef, OntologyRef};
use once_cell::sync::Lazy;
use phenopackets::schema::v2::core::Resource;

pub(crate) static HPO_REF: Lazy<OntologyRef> =
    Lazy::new(|| OntologyRef::hp_with_version("2025-09-01"));
pub(crate) static MONDO_REF: Lazy<OntologyRef> =
    Lazy::new(|| OntologyRef::mondo_with_version("2025-10-07"));

pub(crate) fn mondo_meta_data_resource() -> Resource {
    Resource {
        id: MONDO_REF.prefix_id().to_lowercase(),
        name: "Mondo Disease Ontology".to_string(),
        url: "http://purl.obolibrary.org/obo/mondo.json".to_string(),
        version: MONDO_REF.version().to_string(),
        namespace_prefix: MONDO_REF.prefix_id().to_string(),
        iri_prefix: "http://purl.obolibrary.org/obo/MONDO_$1".to_string(),
    }
}

pub(crate) fn hp_meta_data_resource() -> Resource {
    Resource {
        id: HPO_REF.prefix_id().to_lowercase(),
        name: "Human Phenotype Ontology".to_string(),
        url: "http://purl.obolibrary.org/obo/hp.json".to_string(),
        version: HPO_REF.version().to_string(),
        namespace_prefix: HPO_REF.prefix_id().to_string(),
        iri_prefix: "http://purl.obolibrary.org/obo/HP_$1".to_string(),
    }
}

pub(crate) fn hgnc_meta_data_resource() -> Resource {
    Resource {
        id: DatabaseRef::hgnc().prefix_id().to_lowercase(),
        name: "HUGO Gene Nomenclature Committee".to_string(),
        url: "https://w3id.org/biopragmatics/resources/hgnc/2025-10-07/hgnc.ofn".to_string(),
        version: "-".to_string(),
        namespace_prefix: DatabaseRef::hgnc().prefix_id().to_string(),
        iri_prefix: "https://www.genenames.org/data/gene-symbol-report/#!/hgnc_id/$1".to_string(),
    }
}

pub(crate) fn geno_meta_data_resource() -> Resource {
    Resource {
        id: "geno".to_string(),
        name: "Genotype Ontology".to_string(),
        url: "http://purl.obolibrary.org/obo/geno.json".to_string(),
        version: "2025-07-25".to_string(),
        namespace_prefix: "geno".to_string(),
        iri_prefix: "http://purl.obolibrary.org/obo/GENO_$1".to_string(),
    }
}
