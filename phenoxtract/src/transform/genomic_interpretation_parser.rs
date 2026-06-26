#![allow(dead_code)]
#![allow(unused)]
use crate::ontology::resource_references::{KnownResourcePrefixes, ResourceRef};
use crate::transform::error::PhenopacketBuilderError;
use phenopackets::ga4gh::vrsatile::v1::GeneDescriptor;
use phenopackets::schema::v2::core::genomic_interpretation::Call;
use phenopackets::schema::v2::core::{
    GenomicInterpretation, OntologyClass, Sex, VariantInterpretation,
};
use pivotal::hgnc::{CachedHGNCClient, GeneQuery, HGNCData};
use pivotal::hgvs::{CachedHGVSClient, HGVSData, HGVSError, HgvsVariant};
use std::sync::Arc;

#[derive(Debug)]
pub struct GenomicInterpretationParser {
    hgvs_client: Arc<dyn HGVSData>,
    hgnc_client: Arc<dyn HGNCData>,
}

impl GenomicInterpretationParser {
    pub(crate) fn new(hgvs_client: Arc<dyn HGVSData>, hgnc_client: Arc<dyn HGNCData>) -> Self {
        Self {
            hgvs_client,
            hgnc_client,
        }
    }

    pub(crate) fn new_with_defaults() -> Result<Self, PhenopacketBuilderError> {
        Ok(Self {
            hgvs_client: Arc::new(CachedHGVSClient::new_with_defaults()?),
            hgnc_client: Arc::new(CachedHGNCClient::new_with_defaults()?),
        })
    }

    pub(crate) fn build_genomic_interpretations(
        &self,
        gene: Option<&str>,
        hgvs1: Option<&str>,
        hgvs2: Option<&str>,
        patient_id: &str,
        sex: Option<&str>,
    ) -> Result<(Vec<GenomicInterpretation>, Vec<ResourceRef>), PhenopacketBuilderError> {
        let mut gis = vec![];
        let mut resources = vec![];

        let gene_variant_data = GeneVariantData::from_options(gene, hgvs1, hgvs2);

        match gene_variant_data {
            GeneVariantData::None => {}

            GeneVariantData::Gene(gene) => {
                gis.push(self.genomic_interpretation_from_gene(patient_id, gene));
                resources.push(ResourceRef::hgnc())
            }

            GeneVariantData::SingleVariant(hgvs) => {
                let validated_hgvs = self.hgvs_client.request_and_validate_hgvs(hgvs)?;
                let allelic_state = Self::calculate_allelic_state(sex, &validated_hgvs, None);
                gis.push(Self::genomic_interpretation_from_var(
                    &validated_hgvs,
                    &allelic_state,
                ));
                resources.extend(Self::variant_resources())
            }

            GeneVariantData::VariantPair(h1, h2) => {
                let validated_hgvs1 = self.hgvs_client.request_and_validate_hgvs(h1)?;
                let validated_hgvs2 = self.hgvs_client.request_and_validate_hgvs(h2)?;
                let allelic_state =
                    Self::calculate_allelic_state(sex, &validated_hgvs1, Some(&validated_hgvs2));

                if validated_hgvs1 == validated_hgvs2 {
                    gis.push(Self::genomic_interpretation_from_var(
                        &validated_hgvs1,
                        &allelic_state,
                    ));
                } else {
                    gis.push(Self::genomic_interpretation_from_var(
                        &validated_hgvs1,
                        &allelic_state,
                    ));
                    gis.push(Self::genomic_interpretation_from_var(
                        &validated_hgvs2,
                        &allelic_state,
                    ));
                    resources.extend(Self::variant_resources())
                }
            }
        }

        Ok((gis, resources))
    }

    fn variant_resources() -> Vec<ResourceRef> {
        vec![
            ResourceRef::hgnc(),
            ResourceRef::from("geno").with_version("2025-07-25"),
        ]
    }

    fn calculate_allelic_state(
        sex: Option<&str>,
        hgvs1: &HgvsVariant,
        hgvs2: Option<&HgvsVariant>,
    ) -> OntologyClass {
        let sex_enum = if let Some(sex) = sex {
            Sex::from_str_name(sex).unwrap()
            // todo ! Error handling here
        } else {
            Sex::UnknownSex
        };

        match (&chromosomal_sex, &allele_count, is_x, is_y) {
            // variants on non-sex chromosomes
            (_, AlleleCount::Double, false, false) => Ok(OntologyClass {
                id: "GENO:0000136".to_string(),
                label: "homozygous".to_string(),
            }),
            (_, AlleleCount::Single, false, false) => Ok(OntologyClass {
                id: "GENO:0000135".to_string(),
                label: "heterozygous".to_string(),
            }),
            // variants on x-chromosome
            (
                ChromosomalSex::XX
                | ChromosomalSex::XXY
                | ChromosomalSex::XXX
                | ChromosomalSex::Unknown,
                AlleleCount::Double,
                true,
                false,
            ) => Ok(OntologyClass {
                id: "GENO:0000136".to_string(),
                label: "homozygous".to_string(),
            }),
            (
                ChromosomalSex::XX | ChromosomalSex::XXY | ChromosomalSex::XXX,
                AlleleCount::Single,
                true,
                false,
            ) => Ok(OntologyClass {
                id: "GENO:0000135".to_string(),
                label: "heterozygous".to_string(),
            }),
            (
                ChromosomalSex::X | ChromosomalSex::XY | ChromosomalSex::XYY,
                AlleleCount::Single,
                true,
                false,
            ) => Ok(OntologyClass {
                id: "GENO:0000134".to_string(),
                label: "hemizygous".to_string(),
            }),
            (ChromosomalSex::Unknown, AlleleCount::Single, true, false) => Ok(OntologyClass {
                id: "GENO:0000137".to_string(),
                label: "unspecified zygosity".to_string(),
            }),
            // variants on y-chromosome
            (ChromosomalSex::XYY | ChromosomalSex::Unknown, AlleleCount::Double, false, true) => {
                Ok(OntologyClass {
                    id: "GENO:0000136".to_string(),
                    label: "homozygous".to_string(),
                })
            }
            (ChromosomalSex::XYY, AlleleCount::Single, false, true) => Ok(OntologyClass {
                id: "GENO:0000135".to_string(),
                label: "heterozygous".to_string(),
            }),
            (ChromosomalSex::XY | ChromosomalSex::XXY, AlleleCount::Single, false, true) => {
                Ok(OntologyClass {
                    id: "GENO:0000134".to_string(),
                    label: "hemizygous".to_string(),
                })
            }
            (ChromosomalSex::Unknown, AlleleCount::Single, false, true) => Ok(OntologyClass {
                id: "GENO:0000137".to_string(),
                label: "unspecified zygosity".to_string(),
            }),
            // nothing else makes sense
            _ => Err(HGVSError::ContradictoryAllelicData {
                chromosomal_sex: chromosomal_sex.clone(),
                allele_count,
                is_x,
                is_y,
            }),
        }
    }

    fn genomic_interpretation_from_gene(
        &self,
        patient_id: &str,
        gene: &str,
    ) -> GenomicInterpretation {
        let (gene_symbol, gene_id) = self
            .hgnc_client
            .request_gene_identifier_pair(GeneQuery::from(gene))
            .unwrap();
        let gene_descriptor = GeneDescriptor {
            value_id: gene_id,
            symbol: gene_symbol,
            ..Default::default()
        };

        GenomicInterpretation {
            subject_or_biosample_id: patient_id.to_string(),
            call: Some(Call::Gene(gene_descriptor)),
            ..Default::default()
        }
    }

    pub(crate) fn genomic_interpretation_from_var(
        validated_hgvs: &HgvsVariant,
        allelic_state: &OntologyClass,
    ) -> GenomicInterpretation {
        let gene_context = GeneDescriptor {
            value_id: hgvs_variant.hgnc_id().to_string(),
            symbol: hgvs_variant.gene_symbol().to_string(),
            ..Default::default()
        };

        let mut expressions = vec![];

        if Self::is_c_hgvs(hgvs_variant.allele()) {
            let hgvs_c = Expression {
                syntax: "hgvs.c".to_string(),
                value: hgvs_variant.transcript_hgvs().to_string(),
                version: String::default(),
            };
            expressions.push(hgvs_c);
        }

        if Self::is_n_hgvs(hgvs_variant.allele()) {
            let hgvs_n = Expression {
                syntax: "hgvs.n".to_string(),
                value: hgvs_variant.transcript_hgvs().to_string(),
                version: String::default(),
            };
            expressions.push(hgvs_n);
        }

        if Self::is_m_hgvs(hgvs_variant.allele()) {
            let hgvs_m = Expression {
                syntax: "hgvs.m".to_string(),
                value: hgvs_variant.transcript_hgvs().to_string(),
                version: String::default(),
            };
            expressions.push(hgvs_m);
        }

        expressions.push(Expression {
            syntax: "hgvs.g".to_string(),
            value: hgvs_variant.g_hgvs().to_string(),
            version: String::default(),
        });

        if let Some(hgvs_p) = &hgvs_variant.p_hgvs() {
            let hgvs_p = Expression {
                syntax: "hgvs.p".to_string(),
                value: hgvs_p.clone(),
                version: String::default(),
            };
            expressions.push(hgvs_p);
        }

        let vcf_record = VcfRecord {
            genome_assembly: hgvs_variant.assembly().to_string(),
            chrom: hgvs_variant.chr().to_string(),
            pos: hgvs_variant.position() as u64,
            r#ref: hgvs_variant.ref_allele().to_string(),
            alt: hgvs_variant.alt_allele().to_string(),
            ..Default::default()
        };

        let variation_descriptor = VariationDescriptor {
            id: Uuid::new_v4().to_string(),
            gene_context: Some(gene_context),
            expressions,
            vcf_record: Some(vcf_record),
            molecule_context: MoleculeContext::Genomic.into(),
            allelic_state: Some(allelic_state),
            ..Default::default()
        };
        VariantInterpretation {
            acmg_pathogenicity_classification: AcmgPathogenicityClassification::Pathogenic.into(),
            therapeutic_actionability: TherapeuticActionability::UnknownActionability.into(),
            variation_descriptor: Some(variation_descriptor),
        }
    }
}

enum GeneVariantData<'a> {
    None,
    Gene(&'a str),
    SingleVariant(&'a str),
    VariantPair(&'a str, &'a str),
}

impl<'a> GeneVariantData<'a> {
    fn from_options(gene: Option<&'a str>, hgvs1: Option<&'a str>, hgvs2: Option<&'a str>) -> Self {
        match (gene, hgvs1, hgvs2) {
            (None, None, None) => Self::None,
            (Some(g), None, None) => Self::Gene(g),
            (_, Some(h), None) | (_, None, Some(h)) => Self::SingleVariant(h),
            (_, Some(h1), Some(h2)) => Self::VariantPair(h1, h2),
        }
    }
}
