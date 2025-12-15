use phenopackets::schema::v2::Phenopacket;
use phenopackets::schema::v2::core::genomic_interpretation::Call;

#[macro_export]
macro_rules! skip_in_ci {
    ($test_name:expr) => {
        if std::env::var("CI").is_ok() {
            println!("Skipping {} in CI environment", $test_name);
            return;
        }
    };
    () => {
        if std::env::var("CI").is_ok() {
            println!("Skipping {} in CI environment", module_path!());
            return;
        }
    };
}

pub(crate) fn assert_phenopackets(actual: &mut Phenopacket, expected: &mut Phenopacket) {
    remove_created_from_metadata(actual);
    remove_created_from_metadata(expected);

    remove_id_from_variation_descriptor(actual);
    remove_id_from_variation_descriptor(expected);

    pretty_assertions::assert_eq!(actual, expected);
}

fn remove_created_from_metadata(pp: &mut Phenopacket) {
    if let Some(meta) = &mut pp.meta_data {
        meta.created = None;
    }
}

fn remove_id_from_variation_descriptor(pp: &mut Phenopacket) {
    for interpretation in pp.interpretations.iter_mut() {
        if let Some(diagnosis) = &mut interpretation.diagnosis {
            for gi in diagnosis.genomic_interpretations.iter_mut() {
                if let Some(call) = &mut gi.call
                    && let Call::VariantInterpretation(vi) = call
                    && let Some(vi) = &mut vi.variation_descriptor
                {
                    vi.id = "TEST_ID".to_string();
                }
            }
        }
    }
}
