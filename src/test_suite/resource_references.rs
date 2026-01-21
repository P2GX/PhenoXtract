use crate::ontology::traits::{HasPrefixId, HasVersion};
use crate::ontology::{DatabaseRef, OntologyRef};
use once_cell::sync::Lazy;
use phenopackets::schema::v2::core::Resource;

pub(crate) static HPO_REF: Lazy<OntologyRef> =
    Lazy::new(|| OntologyRef::hp_with_version("2025-09-01"));
pub(crate) static MONDO_REF: Lazy<OntologyRef> =
    Lazy::new(|| OntologyRef::mondo_with_version("2026-01-06"));
pub(crate) static UO_REF: Lazy<OntologyRef> =
    Lazy::new(|| OntologyRef::uo_with_version("2026-01-09"));
pub(crate) static PATO_REF: Lazy<OntologyRef> =
    Lazy::new(|| OntologyRef::pato_with_version("2025-05-14"));
pub(crate) static LOINC_REF: Lazy<DatabaseRef> = Lazy::new(DatabaseRef::loinc);

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
        url: "https://w3id.org/biopragmatics/resources/hgnc/2026-01-06/hgnc.ofn".to_string(),
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
        namespace_prefix: "GENO".to_string(),
        iri_prefix: "http://purl.obolibrary.org/obo/GENO_$1".to_string(),
    }
}

pub(crate) fn uo_meta_data_resource() -> Resource {
    Resource {
        id: UO_REF.prefix_id().to_lowercase(),
        name: "Units of measurement ontology".to_string(),
        url: "http://purl.obolibrary.org/obo/uo.json".to_string(),
        version: UO_REF.version().to_string(),
        namespace_prefix: UO_REF.prefix_id().to_string(),
        iri_prefix: "http://purl.obolibrary.org/obo/UO_$1".to_string(),
    }
}

pub(crate) fn loinc_meta_data_resource() -> Resource {
    Resource {
        id: LOINC_REF.prefix_id().to_lowercase(),
        name: "Logical Observation Identifiers Names and Codes".to_string(),
        url: "https://loinc.org/".to_string(),
        version: "-".to_string(),
        namespace_prefix: LOINC_REF.prefix_id().to_string(),
        iri_prefix: "https://loinc.org/$1".to_string(),
    }
}

pub(crate) fn pato_meta_data_resource() -> Resource {
    Resource {
        id: PATO_REF.prefix_id().to_lowercase(),
        name: "Phenotype And Trait Ontology".to_string(),
        url: "http://purl.obolibrary.org/obo/pato.json".to_string(),
        version: PATO_REF.version().to_string(),
        namespace_prefix: PATO_REF.prefix_id().to_string(),
        iri_prefix: "http://purl.obolibrary.org/obo/PATO_$1".to_string(),
    }
}
