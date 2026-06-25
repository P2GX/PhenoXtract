use crate::ontology::resource_references::{KnownResourcePrefixes, ResourceRef};
use crate::transform::pathogenic_gene_variant_info::PathogenicGeneVariantData;
use crate::transform::utils::chromosomal_sex_from_str;
use phenopackets::ga4gh::vrsatile::v1::{
    Expression, GeneDescriptor, MoleculeContext, VariationDescriptor, VcfRecord,
};
use phenopackets::schema::v2::core::genomic_interpretation::Call;
use phenopackets::schema::v2::core::{
    AcmgPathogenicityClassification, GenomicInterpretation, OntologyClass,
    TherapeuticActionability, VariantInterpretation,
};
use pivotal::hgnc::{GeneQuery, HGNCData};
use pivotal::hgvs::{AlleleCount, HGVSData, HgvsVariant};
use std::sync::Arc;
use uuid::Uuid;

pub(crate) struct VariantInterpretationBuilder {
    hgvs_client: Arc<dyn HGVSData>,
    hgnc_client: Arc<dyn HGNCData>,
}

impl VariantInterpretationBuilder {
    pub(crate) fn build_genomic_interpretations(
        &self,
        gene_variant_data: PathogenicGeneVariantData,
        patient_id: &str,
        sex: Option<&str>,
    ) -> (Vec<GenomicInterpretation>, Vec<ResourceRef>) {
        let mut genomic_interpretations: Vec<GenomicInterpretation> = vec![];
        let mut resources: Vec<ResourceRef> = vec![];

        match gene_variant_data {
            PathogenicGeneVariantData::CausativeGene(gene) => {
                let (symbol, id) = self
                    .hgnc_client
                    .request_gene_identifier_pair(GeneQuery::from(gene.as_str()))
                    .unwrap();

                let gi = GenomicInterpretation {
                    subject_or_biosample_id: patient_id.to_string(),
                    call: Some(Call::Gene(GeneDescriptor {
                        value_id: id.clone(),
                        symbol: symbol.clone(),
                        ..Default::default()
                    })),
                    ..Default::default()
                };
                genomic_interpretations.push(gi);
                resources.push(ResourceRef::from(KnownResourcePrefixes::HGNC));
            }
            PathogenicGeneVariantData::SingleVariant { gene, var } => {
                let validated_hgvs = self.get_data_from_pivotal(&var);
                // TODO: Validate against gene
                let vi = Self::build_variant_interpretation(
                    validated_hgvs,
                    Self::get_allele_term().unwrap(),
                );
            }
            PathogenicGeneVariantData::HomozygousVariant { gene, var } => {}
            PathogenicGeneVariantData::CompoundHeterozygousVariantPair { gene, var1, var2 } => {}
            PathogenicGeneVariantData::None => {}
        }

        (genomic_interpretations, resources)

        /*        if matches!(
            gene_variant_data,
            PathogenicGeneVariantData::SingleVariant { .. }
                | PathogenicGeneVariantData::HomozygousVariant { .. }
                | PathogenicGeneVariantData::CompoundHeterozygousVariantPair { .. }
        ) {
            for var in gene_variant_data.get_vars() {
                let validated_hgvs = self.ctx.hgvs_client().request_and_validate_hgvs(var)?;

                self.ensure_resource(patient_id, &ResourceRef::from(KnownResourcePrefixes::HGNC));
                self.ensure_resource(
                    patient_id,
                    &ResourceRef::from("geno").with_version("2025-07-25"),
                );

                if let Some(gene) = gene_variant_data.get_gene() {
                    validated_hgvs.validate_against_gene(gene)?;
                }

                let vi = validated_hgvs.create_variant_interpretation(
                    AlleleCount::try_from(gene_variant_data.get_allelic_count() as u8)?,
                    &chromosomal_sex,
                )?;

                let gi = GenomicInterpretation {
                    subject_or_biosample_id: patient_id.to_string(),
                    call: Some(Call::VariantInterpretation(vi)),
                    ..Default::default()
                };

                genomic_interpretations.push(gi);
            }
        }*/
    }
    pub(crate) fn get_data_from_pivotal(&self, hgvs: &str) -> HgvsVariant {
        self.hgvs_client.request_and_validate_hgvs(hgvs).unwrap()
    }

    pub(crate) fn build_gene_descriptor() -> GeneDescriptor {
        todo!()
    }

    pub(crate) fn build_variant_interpretation(
        hgvs_variant: HgvsVariant,
        allelic_state: OntologyClass,
    ) -> VariantInterpretation {
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

    pub fn is_c_hgvs(allele: &str) -> bool {
        allele.starts_with("c.")
    }

    pub fn is_n_hgvs(allele: &str) -> bool {
        allele.starts_with("n.")
    }

    pub fn is_m_hgvs(allele: &str) -> bool {
        allele.starts_with("m.")
    }

    fn get_allele_term(
        sex: Option<&str>,
        allele_count: AlleleCount,

        is_x: bool,
        is_y: bool,
    ) -> Result<OntologyClass, HGVSError> {
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
}
