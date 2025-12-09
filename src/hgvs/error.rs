use crate::caching::error::CacherError;
use crate::hgvs::enums::{AlleleCount, ChromosomalSex};
use redb::{CommitError, DatabaseError, StorageError, TableError, TransactionError};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum HGVSError {
    #[error("Hgvs string {hgvs} not accepted due to format problem: {problem}.")]
    HgvsFormatNotAccepted { hgvs: String, problem: String },
    #[error(
        "VariantValidator response for {hgvs} did not have flag gene_variant. The flag was {flag} instead. "
    )]
    NonGeneVariant { hgvs: String, flag: String },
    #[error(
        "VariantValidator response for {hgvs} did not have genome_assembly {desired_assembly}. The following assemblies were found instead: {found_assemblies:?}"
    )]
    GenomeAssemblyNotFound {
        hgvs: String,
        desired_assembly: String,
        found_assemblies: Vec<String>,
    },
    #[error(
        "The provided {id_type} {expected_gene} does not match with the {hgvs_gene} that of HGVS variant {hgvs}"
    )]
    MismatchingGeneData {
        id_type: String,
        expected_gene: String,
        hgvs: String,
        hgvs_gene: String,
    },
    #[error(
        "VariantValidator response for {hgvs} has element {element} with following problem: {problem}"
    )]
    InvalidVariantValidatorResponseElement {
        hgvs: String,
        element: String,
        problem: String,
    },
    #[error(
        "The following data for a HGVS was contradictory: Chromosomal Sex: {chromosomal_sex:?}, AlleleCount: {allele_count:?}, is_x: {is_x}, is_y: {is_y}"
    )]
    ContradictoryAllelicData {
        chromosomal_sex: ChromosomalSex,
        allele_count: AlleleCount,
        is_x: bool,
        is_y: bool,
    },
    #[error(
        "VariantValidator response for {hgvs} could not be deserialized to schema. {hgvs} may be invalid. Error: {err}."
    )]
    DeserializeVariantValidatorResponseToSchema { hgvs: String, err: String },
    #[error("VariantValidator fetch request for {hgvs} failed. Error: {err}.")]
    FetchRequest { hgvs: String, err: String },
    #[error(transparent)]
    CacheDatabase(#[from] DatabaseError),
    #[error(transparent)]
    CacheTransaction(#[from] TransactionError),
    #[error(transparent)]
    CacheCommit(#[from] CommitError),
    #[error(transparent)]
    CacheTable(#[from] TableError),
    #[error(transparent)]
    CacheStorage(#[from] StorageError),
    #[error(transparent)]
    CacherError(#[from] CacherError),
}
