#![allow(dead_code)]
#![allow(unused)]
use crate::config::table_context::TableContext;
use crate::extract::contextualized_data_frame::ContextualizedDataFrame;
use crate::transform::phenopacket_builder::PhenopacketBuilder;
use phenopackets::schema::v2::Phenopacket;
use polars::prelude::DataFrame;

struct Collector {
    phenopacket_builder: PhenopacketBuilder,
}

impl Collector {
    pub fn collect(&mut self, cdfs: Vec<ContextualizedDataFrame>) -> Vec<Phenopacket> {
        for cdf in cdfs {
            // Get SeriesContext with patient_id in data_context
            // Get the column, get the header of the column
            // Collect all unique patient ids.
            // Iterate through them and
            // Do something like:
            // expected_df.column("Patient_IDs").unwrap().as_materialized_series().equal("PID_1").unwrap()
            // To get all entries of a patient in a dataframe.
            // Apply collect functions.
            // collect_individual()
            // collect_phenotypic_features()
        }

        self.phenopacket_builder.build()
    }

    fn collect_phenotypic_features(&mut self, data: DataFrame, tc: TableContext) {
        // Find the necessary values to construct a phenotypic feature building block and upsert them to the PhenopacketBuilder

        /*self.phenopacket_builder.upsert_phenotypic_feature(
            "SOME_SUBJECT_ID".to_string(), // This is how we know which data belongs to which phenopacket
            "SOME_HPO_LABEL/ID".to_string(),
            None,
            None,
        );*/
        todo!()
    }

    fn collect_individual(&self, data: DataFrame, tc: TableContext) {
        // Find the necessary values to construct an individual building block and upsert them to the PhenopacketBuilder
        todo!()
    }
}
