use crate::ontology::error::DatasourceError;
use crate::ontology::ontology_bidict::OntologyBiDict;
use ontolius::io::OntologyLoaderBuilder;
use ontolius::ontology::csr::FullCsrOntology;
use polars::prelude::{CsvReadOptions, SerReader};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

#[allow(dead_code)]
pub fn init_ontolius(hpo_path: PathBuf) -> Result<Arc<FullCsrOntology>, anyhow::Error> {
    let loader = OntologyLoaderBuilder::new().obographs_parser().build();

    let ontolius = loader.load_from_path(hpo_path.clone())?;
    Ok(Arc::new(ontolius))
}

#[allow(dead_code)]
pub fn init_omim_dict(_hpoa_path: PathBuf) -> Result<OntologyBiDict, DatasourceError> {
    let sep = '\t';
    let mut csv_read_options = CsvReadOptions::default()
        .with_has_header(true)
        .with_skip_rows(4);
    let new_parse_options = (*csv_read_options.parse_options)
        .clone()
        .with_separator(sep as u8)
        .with_truncate_ragged_lines(true);
    csv_read_options.parse_options = Arc::from(new_parse_options);

    let hpoa_path =
        PathBuf::from("/Users/patrick/RustroverProjects/PhenoXtrackt/tests/phenotype.hpoa");

    let hpoa_data = csv_read_options
        .try_into_reader_with_file_path(Some(hpoa_path))
        .unwrap()
        .finish()
        .unwrap();

    let stringified_database_id_col = hpoa_data.column("database_id").map_err(|_|DatasourceError::HpoaError("Unexpectedly could not find column database_id in HPOA data. Dataframe loaded incorrectly?".to_string()))?.str().unwrap();
    let stringified_disease_id_col = hpoa_data.column("disease_name").map_err(|_|DatasourceError::HpoaError("Unexpectedly could not find column disease_name in HPOA data. Dataframe loaded incorrectly?".to_string()))?.str().unwrap();

    let mut label_to_id = HashMap::new();
    let mut id_to_label = HashMap::new();

    let id_disease_name_pairs = stringified_database_id_col
        .iter()
        .zip(stringified_disease_id_col.iter());

    for (id, disease_name) in id_disease_name_pairs {
        if let (Some(id), Some(disease_name)) = (id, disease_name) {
            let id_split = id.split(':').collect::<Vec<&str>>();
            if !id_to_label.contains_key(id) && id_split.first() == Some(&"OMIM") {
                label_to_id.insert(disease_name.to_string(), id.to_string());
                id_to_label.insert(id.to_string(), disease_name.to_string());
            }
        }
    }

    Ok(OntologyBiDict::new(
        label_to_id,
        HashMap::new(),
        id_to_label,
    ))
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use super::*;
    use crate::ontology::obolibrary_ontology_registry::ObolibraryOntologyRegistry;
    use crate::ontology::traits::OntologyRegistry;
    use ontolius::ontology::OntologyTerms;
    use ontolius::term::MinimalTerm;
    use ontolius::{Identified, TermId};
    use rstest::rstest;
    use tempfile::TempDir;

    #[rstest]
    fn test_init_ontolius() {
        let tmp = TempDir::new().unwrap();
        let hpo_registry = ObolibraryOntologyRegistry::default_hpo_registry()
            .unwrap()
            .with_registry_path(tmp.path().into());
        let path = hpo_registry.register("latest").unwrap();
        let ontolius = init_ontolius(path).unwrap();

        let term_id: TermId = "HP:0100729".parse().unwrap();

        let term = ontolius.as_ref().term_by_id(&term_id).unwrap();
        assert_eq!(term.name(), "Large face");

        let term = ontolius
            .as_ref()
            .iter_terms()
            .find(|term| term.name() == "Large face")
            .unwrap();
        assert_eq!(term.identifier(), &term_id);
    }

    #[rstest]
    fn test_init_omim_dict() {
        // assert that the database_id is exclusively OMIM and Orpha?
        let res = init_omim_dict(PathBuf::from("fake_path"));
        let bidict = res.unwrap();
        let label_to_id_keys = bidict.label_to_id.keys().cloned().collect::<HashSet<String>>();
        let label_to_id_vals = bidict.label_to_id.values().cloned().collect::<HashSet<String>>();
        let id_to_label_keys = bidict.id_to_label.keys().cloned().collect::<HashSet<String>>();
        let id_to_label_vals = bidict.id_to_label.values().cloned().collect::<HashSet<String>>();
        let diff1: HashSet<String> = id_to_label_keys.difference(&label_to_id_vals).cloned().collect();
        let diff2: HashSet<String> = label_to_id_vals.difference(&id_to_label_keys).cloned().collect();
/*        let diff1: HashSet<String>= label_keys.difference(&label_vals).cloned().collect();
        let diff2: HashSet<String>= label_vals.difference(&label_keys).cloned().collect();
        dbg!(&diff1);
        dbg!(&diff2);*/
        println!("label_to_id_key");
    }
}
