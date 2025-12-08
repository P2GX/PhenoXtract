use phenopackets::schema::v2::Phenopacket;

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
    if let Some(meta) = &mut actual.meta_data {
        meta.created = None;
    }
    if let Some(meta) = &mut expected.meta_data {
        meta.created = None;
    }
    pretty_assertions::assert_eq!(actual, expected);
}
