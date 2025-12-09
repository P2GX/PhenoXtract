use phenopackets::ga4gh::vrsatile::v1::{Expression, GeneDescriptor, MoleculeContext, VariationDescriptor, VcfRecord};
use phenopackets::schema::v2::core::{AcmgPathogenicityClassification, OntologyClass, TherapeuticActionability, VariantInterpretation};
use crate::constants::ISO8601_DUR_PATTERN;
use polars::prelude::{AnyValue, Column};
use regex::Regex;
use crate::transform::error::TransformError;

pub fn is_iso8601_duration(dur_string: &str) -> bool {
    let re = Regex::new(ISO8601_DUR_PATTERN).unwrap();
    re.is_match(dur_string)
}

/// A struct for creating columns which have HPO IDs in the header
/// and observation statuses in the cells.
/// The headers of HPO columns will have the format HP:1234567{separator}A
/// where {separator} is some char, which is by default #, and A is the block_id.
/// If block_id = None then the HPO column headers will have the format HP:1234567.
pub struct HpoColMaker {
    separator: char,
}

impl HpoColMaker {
    pub fn new() -> HpoColMaker {
        HpoColMaker { separator: '#' }
    }

    pub fn create_hpo_col(
        &self,
        hpo_id: &str,
        block_id: Option<&str>,
        data: Vec<AnyValue>,
    ) -> Column {
        let header = match block_id {
            None => hpo_id.to_string(),
            Some(block_id) => format!("{}{}{}", hpo_id, self.separator, block_id),
        };
        Column::new(header.into(), data)
    }

    pub fn decode_column_header<'a>(&self, hpo_col: &'a Column) -> (&'a str, Option<&'a str>) {
        let split_col_name: Vec<&str> = hpo_col.name().split(self.separator).collect();
        let hpo_id = split_col_name[0];
        let block_id = split_col_name.get(1).copied();
        (hpo_id, block_id)
    }
}

pub fn create_variant_interpretation(
    &self,
    allele_count: crate::hgvs::enums::AlleleCount,
    sex: crate::hgvs::enums::ChromosomalSex,
) -> Result<VariantInterpretation, TransformError> {
    let gene_context = GeneDescriptor {
        value_id: self.hgnc_id().to_string(),
        symbol: self.gene_symbol().to_string(),
        ..Default::default()
    };

    let mut expressions = vec![];

    if is_c_hgvs(self.allele()) {
        let hgvs_c = Expression {
            syntax: "hgvs.c".to_string(),
            value: self.transcript_hgvs().to_string(),
            version: String::default(),
        };
        expressions.push(hgvs_c);
    }

    if is_n_hgvs(self.allele()) {
        let hgvs_n = Expression {
            syntax: "hgvs.n".to_string(),
            value: self.transcript_hgvs().to_string(),
            version: String::default(),
        };
        expressions.push(hgvs_n);
    }

    expressions.push(Expression {
        syntax: "hgvs.g".to_string(),
        value: self.g_hgvs().to_string(),
        version: String::default(),
    });

    if let Some(hgvs_p) = &self.p_hgvs() {
        let hgvs_p = Expression {
            syntax: "hgvs.p".to_string(),
            value: hgvs_p.clone(),
            version: String::default(),
        };
        expressions.push(hgvs_p);
    }

    let vcf_record = VcfRecord {
        genome_assembly: self.assembly().to_string(),
        chrom: self.chr().to_string(),
        pos: self.position() as u64,
        r#ref: self.ref_allele().to_string(),
        alt: self.alt_allele().to_string(),
        ..Default::default()
    };

    let allelic_state = Self::get_allele_term(
        sex,
        allele_count,
        self.is_x_chromosomal(),
        self.is_y_chromosomal(),
    )?;

    let variation_descriptor = VariationDescriptor {
        id: self.g_hgvs().to_string(), // I'm not entirely happy with this
        gene_context: Some(gene_context),
        expressions,
        vcf_record: Some(vcf_record),
        molecule_context: MoleculeContext::Genomic.into(),
        allelic_state: Some(allelic_state),
        ..Default::default()
    };
    Ok(VariantInterpretation {
        acmg_pathogenicity_classification: AcmgPathogenicityClassification::Pathogenic.into(),
        therapeutic_actionability: TherapeuticActionability::UnknownActionability.into(),
        variation_descriptor: Some(variation_descriptor),
    })

    fn get_allele_term(
        chromosomal_sex: crate::hgvs::enums::ChromosomalSex,
        allele_count: crate::hgvs::enums::AlleleCount,
        is_x: bool,
        is_y: bool,
    ) -> Result<OntologyClass, HGVSError> {
        match (&chromosomal_sex, &allele_count, is_x, is_y) {
            // variants on non-sex chromosomes
            (_, crate::hgvs::enums::AlleleCount::Double, false, false) => Ok(OntologyClass {
                id: "GENO:0000136".to_string(),
                label: "homozygous".to_string(),
            }),
            (_, crate::hgvs::enums::AlleleCount::Single, false, false) => Ok(OntologyClass {
                id: "GENO:0000135".to_string(),
                label: "heterozygous".to_string(),
            }),
            // variants on x-chromosome
            (
                crate::hgvs::enums::ChromosomalSex::XX
                | crate::hgvs::enums::ChromosomalSex::XXY
                | crate::hgvs::enums::ChromosomalSex::XXX
                | crate::hgvs::enums::ChromosomalSex::Unknown,
                crate::hgvs::enums::AlleleCount::Double,
                true,
                false,
            ) => Ok(OntologyClass {
                id: "GENO:0000136".to_string(),
                label: "homozygous".to_string(),
            }),
            (
                crate::hgvs::enums::ChromosomalSex::XX | crate::hgvs::enums::ChromosomalSex::XXY | crate::hgvs::enums::ChromosomalSex::XXX,
                crate::hgvs::enums::AlleleCount::Single,
                true,
                false,
            ) => Ok(OntologyClass {
                id: "GENO:0000135".to_string(),
                label: "heterozygous".to_string(),
            }),
            (
                crate::hgvs::enums::ChromosomalSex::X | crate::hgvs::enums::ChromosomalSex::XY | crate::hgvs::enums::ChromosomalSex::XYY,
                crate::hgvs::enums::AlleleCount::Single,
                true,
                false,
            ) => Ok(OntologyClass {
                id: "GENO:0000134".to_string(),
                label: "hemizygous".to_string(),
            }),
            (crate::hgvs::enums::ChromosomalSex::Unknown, crate::hgvs::enums::AlleleCount::Single, true, false) => Ok(OntologyClass {
                id: "GENO:0000137".to_string(),
                label: "unspecified zygosity".to_string(),
            }),
            // variants on y-chromosome
            (crate::hgvs::enums::ChromosomalSex::XYY | crate::hgvs::enums::ChromosomalSex::Unknown, crate::hgvs::enums::AlleleCount::Double, false, true) => {
                Ok(OntologyClass {
                    id: "GENO:0000136".to_string(),
                    label: "homozygous".to_string(),
                })
            }
            (crate::hgvs::enums::ChromosomalSex::XYY, crate::hgvs::enums::AlleleCount::Single, false, true) => Ok(OntologyClass {
                id: "GENO:0000135".to_string(),
                label: "heterozygous".to_string(),
            }),
            (crate::hgvs::enums::ChromosomalSex::XY | crate::hgvs::enums::ChromosomalSex::XXY, crate::hgvs::enums::AlleleCount::Single, false, true) => {
                Ok(OntologyClass {
                    id: "GENO:0000134".to_string(),
                    label: "hemizygous".to_string(),
                })
            }
            (crate::hgvs::enums::ChromosomalSex::Unknown, crate::hgvs::enums::AlleleCount::Single, false, true) => Ok(OntologyClass {
                id: "GENO:0000137".to_string(),
                label: "unspecified zygosity".to_string(),
            }),
            // nothing else makes sense
            _ => Err(HGVSError::ContradictoryAllelicData {
                chromosomal_sex,
                allele_count,
                is_x,
                is_y,
            }),
        }
    }

    pub fn validate_against_gene(&self, gene: &str) -> Result<(), HGVSError> {
        let (expected, id_type) = if Self::is_hgnc_id(gene) {
            (self.hgnc_id.as_str(), "HGNC ID")
        } else {
            (self.symbol.as_str(), "gene symbol")
        };

        if gene == expected {
            Ok(())
        } else {
            Err(HGVSError::MismatchingGeneData {
                id_type: id_type.to_string(),
                expected_gene: gene.to_string(),
                hgvs: self.g_hgvs.to_string(),
                hgvs_gene: self.hgnc_id.to_string(),
            })
        }
    }

    fn is_hgnc_id(gene: &str) -> bool {
        let split_string = gene.split(':').collect::<Vec<&str>>();
        split_string.first() == Some(&"HGNC")
    }
}

#[cfg(test)]
mod tests {
    use pivot::HgvsVariant;
    use super::*;
    use rstest::{fixture, rstest};

    #[rstest]
    fn test_create_hpo_col() {
        let hpo_col_maker = HpoColMaker::new();

        let hpo_col = hpo_col_maker.create_hpo_col(
            "HP:1234567",
            Some("A"),
            vec![
                AnyValue::Boolean(true),
                AnyValue::Boolean(true),
                AnyValue::Boolean(false),
            ],
        );
        let expected_hpo_col = Column::new("HP:1234567#A".into(), vec![true, true, false]);
        assert_eq!(hpo_col, expected_hpo_col);

        let hpo_col2 = hpo_col_maker.create_hpo_col(
            "HP:1234567",
            None,
            vec![
                AnyValue::Boolean(true),
                AnyValue::Boolean(true),
                AnyValue::Boolean(false),
            ],
        );
        let expected_hpo_col2 = Column::new("HP:1234567".into(), vec![true, true, false]);
        assert_eq!(hpo_col2, expected_hpo_col2);
    }

    #[rstest]
    fn test_decode_column_header() {
        let hpo_col_maker = HpoColMaker::new();
        let hpo_col = Column::new("HP:1234567#A".into(), vec![true, true, false]);
        assert_eq!(
            ("HP:1234567", Some("A")),
            hpo_col_maker.decode_column_header(&hpo_col)
        );

        let hpo_col2 = Column::new("HP:1234567".into(), vec![true, true, false]);
        assert_eq!(
            ("HP:1234567", None),
            hpo_col_maker.decode_column_header(&hpo_col2)
        );
    }

    #[rstest]
    fn test_is_iso8601_duration() {
        assert!(is_iso8601_duration("P47Y"));
        assert!(is_iso8601_duration("P47Y5M"));
        assert!(is_iso8601_duration("P47Y5M29D"));
        assert!(is_iso8601_duration("P47Y5M29DT8H"));
        assert!(is_iso8601_duration("P47Y5M29DT8H12M"));
        assert!(is_iso8601_duration("P47Y5M29DT8H12M15S"));

        assert!(!is_iso8601_duration("asd"));
        assert!(!is_iso8601_duration("123"));
        assert!(!is_iso8601_duration("47Y"));
    }

    #[fixture]
    fn validated_c_hgvs() -> HgvsVariant {
        HgvsVariant::new(
            "hg38",
            "chr12",
            38332495,
            "G",
            "A",
            "KIF21A",
            "HGNC:19349",
            "NM_001173464.1",
            "c.2860C>T",
            "NM_001173464.1:c.2860C>T",
            "NC_000012.12:g.39332405G>A",
            Some("NP_001166935.1:p.(Arg954Trp)"),
        )
    }

    #[fixture]
    fn validated_n_hgvs() -> HgvsVariant {
        HgvsVariant::new(
            "hg38",
            "chr11",
            1997235,
            "C",
            "A",
            "H19",
            "HGNC:4713",
            "NR_002196.1",
            "n.601G>T",
            "NR_002196.1:n.601G>T",
            "NC_000011.10:g.1997235C>A",
            None::<&str>,
        )
    }

    #[rstest]
    fn test_validate_against_gene() {
        crate::hgvs::hgvs_variant::tests::validated_c_hgvs().validate_against_gene("KIF21A").unwrap();
        crate::hgvs::hgvs_variant::tests::validated_c_hgvs()
            .validate_against_gene("HGNC:19349")
            .unwrap();
    }

    #[rstest]
    fn test_validate_against_gene_err() {
        assert!(crate::hgvs::hgvs_variant::tests::validated_c_hgvs().validate_against_gene("CLOCK").is_err());
        assert!(
            crate::hgvs::hgvs_variant::tests::validated_c_hgvs()
                .validate_against_gene("HGNC:1234")
                .is_err()
        );
    }

    #[rstest]
    fn test_is_hgnc_id() {
        assert!(HgvsVariant::is_hgnc_id("HGNC:1234"));
        assert!(!HgvsVariant::is_hgnc_id("CLOCK"));
    }

    #[rstest]
    fn test_get_allele_term_heterozygous() {
        let allele_term =
            HgvsVariant::get_allele_term(crate::hgvs::enums::ChromosomalSex::XX, crate::hgvs::enums::AlleleCount::Single, false, false)
                .unwrap();
        assert_eq!(allele_term.label, "heterozygous");
    }

    #[rstest]
    fn test_get_allele_term_heterozygous_on_x() {
        let allele_term =
            HgvsVariant::get_allele_term(crate::hgvs::enums::ChromosomalSex::XX, crate::hgvs::enums::AlleleCount::Single, true, false)
                .unwrap();
        assert_eq!(allele_term.label, "heterozygous");
    }

    #[rstest]
    fn test_get_allele_term_homozygous() {
        let allele_term = HgvsVariant::get_allele_term(
            crate::hgvs::enums::ChromosomalSex::Unknown,
            crate::hgvs::enums::AlleleCount::Double,
            false,
            false,
        )
            .unwrap();
        assert_eq!(allele_term.label, "homozygous");
    }

    #[rstest]
    fn test_get_allele_term_hemizygous_on_x() {
        let allele_term =
            HgvsVariant::get_allele_term(crate::hgvs::enums::ChromosomalSex::XYY, crate::hgvs::enums::AlleleCount::Single, true, false)
                .unwrap();
        assert_eq!(allele_term.label, "hemizygous");
    }

    #[rstest]
    fn test_get_allele_term_hemizygous_on_y() {
        let allele_term =
            HgvsVariant::get_allele_term(crate::hgvs::enums::ChromosomalSex::XXY, crate::hgvs::enums::AlleleCount::Single, false, true)
                .unwrap();
        assert_eq!(allele_term.label, "hemizygous");
    }

    #[rstest]
    fn test_get_allele_term_unknown_on_x() {
        let allele_term =
            HgvsVariant::get_allele_term(crate::hgvs::enums::ChromosomalSex::Unknown, crate::hgvs::enums::AlleleCount::Single, true, false)
                .unwrap();
        assert_eq!(allele_term.label, "unspecified zygosity");
    }

    #[rstest]
    fn test_get_allele_term_unknown_on_y() {
        let allele_term =
            HgvsVariant::get_allele_term(crate::hgvs::enums::ChromosomalSex::Unknown, crate::hgvs::enums::AlleleCount::Single, false, true)
                .unwrap();
        assert_eq!(allele_term.label, "unspecified zygosity");
    }

    #[rstest]
    fn test_get_allele_term_unknown_not_on_x_or_y() {
        let allele_term = HgvsVariant::get_allele_term(
            crate::hgvs::enums::ChromosomalSex::Unknown,
            crate::hgvs::enums::AlleleCount::Single,
            false,
            false,
        )
            .unwrap();
        assert_eq!(allele_term.label, "heterozygous");
    }

    #[rstest]
    fn test_get_allele_term_on_x_and_y() {
        let result =
            HgvsVariant::get_allele_term(crate::hgvs::enums::ChromosomalSex::Unknown, crate::hgvs::enums::AlleleCount::Single, true, true);
        assert!(result.is_err());
    }

    #[rstest]
    fn test_get_allele_term_not_enough_x_chromosomes() {
        let result =
            HgvsVariant::get_allele_term(crate::hgvs::enums::ChromosomalSex::XY, crate::hgvs::enums::AlleleCount::Double, true, false);
        assert!(result.is_err());
    }

    #[rstest]
    fn test_create_variant_interpretation_c_hgvs() {
        let vi = crate::hgvs::hgvs_variant::tests::validated_c_hgvs()
            .create_variant_interpretation(crate::hgvs::enums::AlleleCount::Single, crate::hgvs::enums::ChromosomalSex::Unknown)
            .unwrap();

        let vi_allelic_state = vi
            .variation_descriptor
            .clone()
            .unwrap()
            .allelic_state
            .unwrap()
            .label;
        assert_eq!(vi_allelic_state, "heterozygous");

        let vi_expressions = vi.variation_descriptor.clone().unwrap().expressions;
        assert_eq!(vi_expressions.len(), 3);
        let c_hgvs_expressions = vi_expressions
            .iter()
            .filter(|exp| exp.syntax == "hgvs.c")
            .collect::<Vec<&Expression>>();
        let c_hgvs_expression = c_hgvs_expressions.first().unwrap();
        assert_eq!(c_hgvs_expression.value, crate::hgvs::hgvs_variant::tests::validated_c_hgvs().transcript_hgvs);
    }

    #[rstest]
    fn test_create_variant_interpretation_n_hgvs() {
        let vi = crate::hgvs::hgvs_variant::tests::validated_n_hgvs()
            .create_variant_interpretation(crate::hgvs::enums::AlleleCount::Double, crate::hgvs::enums::ChromosomalSex::Unknown)
            .unwrap();

        let vi_allelic_state = vi
            .variation_descriptor
            .clone()
            .unwrap()
            .allelic_state
            .unwrap()
            .label;
        assert_eq!(vi_allelic_state, "homozygous");

        let vi_expressions = vi.variation_descriptor.clone().unwrap().expressions;
        assert_eq!(vi_expressions.len(), 2);
        let n_hgvs_expressions = vi_expressions
            .iter()
            .filter(|exp| exp.syntax == "hgvs.n")
            .collect::<Vec<&Expression>>();
        let n_hgvs_expression = n_hgvs_expressions.first().unwrap();
        assert_eq!(n_hgvs_expression.value, crate::hgvs::hgvs_variant::tests::validated_n_hgvs().transcript_hgvs);
    }
}
