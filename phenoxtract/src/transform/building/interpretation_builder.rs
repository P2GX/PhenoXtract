use crate::ontology::resource_references::{KnownResourcePrefixes, ResourceRef};
use crate::transform::building::ppb::PhenopacketBuilder;
use crate::transform::error::PhenopacketBuilderError;
use crate::transform::pathogenic_gene_variant_info::PathogenicGeneVariantData;
use crate::transform::utils::chromosomal_sex_from_str;
use phenopackets::ga4gh::vrsatile::v1::GeneDescriptor;
use phenopackets::schema::v2::core::genomic_interpretation::Call;
use phenopackets::schema::v2::core::interpretation::ProgressStatus;
use phenopackets::schema::v2::core::{Diagnosis, GenomicInterpretation, Interpretation};
use pivot::hgnc::GeneQuery;
use pivot::hgvs::AlleleCount;

pub struct InterpretationBuilder<'a> {
    pp_builder: &'a mut PhenopacketBuilder,
    patient_id: &'a str,
    disease: &'a str,
    gene_variant_data: &'a PathogenicGeneVariantData,
    sex: Option<&'a str>,
}

impl<'a> InterpretationBuilder<'a> {
    pub fn new(
        pp_builder: &'a mut PhenopacketBuilder,
        patient_id: &'a str,
        disease: &'a str,
        gene_variant_data: &'a PathogenicGeneVariantData,
    ) -> Self {
        Self {
            pp_builder,
            patient_id,
            disease,
            gene_variant_data,
            sex: None,
        }
    }

    pub fn sex(mut self, s: &'a str) -> Self {
        self.sex = Some(s);
        self
    }

    pub fn apply(mut self) -> Result<(), PhenopacketBuilderError> {
        let mut genomic_interpretations: Vec<GenomicInterpretation> = vec![];
        let phenopacket_id = self.pp_builder.generate_phenopacket_id(self.patient_id);

        if self
            .pp_builder
            .ctx
            .dictionary_registry
            .disease
            .get_bidicts()
            .is_empty()
        {
            return Err(PhenopacketBuilderError::MissingBiDict {
                bidict_type: "disease".to_string(),
            });
        }

        let (disease_term, res_ref) = self
            .pp_builder
            .ctx
            .dictionary_registry
            .disease
            .query_bidicts(self.disease)
            .ok_or_else(|| PhenopacketBuilderError::ParsingError {
                what: "Disease term".to_string(),
                value: self.disease.to_string(),
            })?;

        self.pp_builder.ensure_resource(self.patient_id, &res_ref);

        if let PathogenicGeneVariantData::CausativeGene(gene) = self.gene_variant_data {
            let (symbol, id) = self
                .pp_builder
                .ctx
                .hgnc_client
                .request_gene_identifier_pair(GeneQuery::from(gene.as_str()))?;
            self.pp_builder.ensure_resource(
                self.patient_id,
                &ResourceRef::from(KnownResourcePrefixes::HGNC),
            );

            let gi = GenomicInterpretation {
                subject_or_biosample_id: self.patient_id.to_string(),
                call: Some(Call::Gene(GeneDescriptor {
                    value_id: id.clone(),
                    symbol: symbol.clone(),
                    ..Default::default()
                })),
                ..Default::default()
            };
            genomic_interpretations.push(gi);
        }

        if matches!(
            self.gene_variant_data,
            PathogenicGeneVariantData::SingleVariant { .. }
                | PathogenicGeneVariantData::HomozygousVariant { .. }
                | PathogenicGeneVariantData::CompoundHeterozygousVariantPair { .. }
        ) {
            let chromosomal_sex = chromosomal_sex_from_str(self.sex.map(String::from))?;

            for var in self.gene_variant_data.get_vars() {
                let validated_hgvs = self
                    .pp_builder
                    .ctx
                    .hgvs_client
                    .request_and_validate_hgvs(var)?;
                self.pp_builder.ensure_resource(
                    self.patient_id,
                    &ResourceRef::from(KnownResourcePrefixes::HGNC),
                );
                self.pp_builder.ensure_resource(
                    self.patient_id,
                    &ResourceRef::from("geno").with_version("2025-07-25"),
                );

                if let Some(gene) = self.gene_variant_data.get_gene() {
                    validated_hgvs.validate_against_gene(gene)?;
                }

                let vi = validated_hgvs.create_variant_interpretation(
                    AlleleCount::try_from(self.gene_variant_data.get_allelic_count() as u8)?,
                    &chromosomal_sex,
                )?;

                let gi = GenomicInterpretation {
                    subject_or_biosample_id: self.patient_id.to_string(),
                    call: Some(Call::VariantInterpretation(vi)),
                    ..Default::default()
                };

                genomic_interpretations.push(gi);
            }
        }

        let interpretation_id = format!("{}-{}", phenopacket_id, disease_term.id);

        let interpretation =
            self.get_or_create_interpretation(self.patient_id, interpretation_id.as_str());

        interpretation.progress_status = ProgressStatus::UnknownProgress.into();

        interpretation.diagnosis = Some(Diagnosis {
            disease: Some(disease_term),
            genomic_interpretations,
        });

        Ok(())
    }

    fn get_or_create_interpretation(
        &mut self,
        patient_id: &str,
        interpretation_id: &str,
    ) -> &mut Interpretation {
        let pp = self.pp_builder.get_or_create_phenopacket(patient_id);
        let interpretation_index = pp
            .interpretations
            .iter()
            .position(|inter| inter.id == interpretation_id);

        match interpretation_index {
            Some(pos) => &mut pp.interpretations[pos],
            None => {
                pp.interpretations.push(Interpretation {
                    id: interpretation_id.to_string(),
                    progress_status: ProgressStatus::InProgress.into(),
                    ..Default::default()
                });
                pp.interpretations.last_mut().unwrap()
            }
        }
    }
}
