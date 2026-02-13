use crate::ontology::ontology_bidict::OntologyBiDict;
use crate::test_suite::mocks::ONTOLOGY_FACTORY;
use crate::test_suite::resource_references::{HPO_REF, MONDO_REF, PATO_REF, UO_REF};
use once_cell::sync::Lazy;
use ontolius::ontology::csr::FullCsrOntology;
use std::collections::HashMap;
use std::sync::Arc;

pub(crate) static MONDO_BIDICT: Lazy<Arc<OntologyBiDict>> = Lazy::new(|| {
    let mock_mondo_label_to_id: HashMap<String, String> = HashMap::from_iter([
        (
            "platelet signal processing defect".to_string(),
            "MONDO:0008258".to_string(),
        ),
        (
            "heart defects-limb shortening syndrome".to_string(),
            "MONDO:0008917".to_string(),
        ),
        (
            "macular degeneration, age-related, 3".to_string(),
            "MONDO:0012145".to_string(),
        ),
        (
            "spondylocostal dysostosis".to_string(),
            "MONDO:0000359".to_string(),
        ),
        (
            "inflammatory diarrhea".to_string(),
            "MONDO:0000252".to_string(),
        ),
    ]);

    let mock_mondo_id_to_label: HashMap<String, String> = mock_mondo_label_to_id
        .iter()
        .map(|(label, id)| (id.to_string(), label.to_string()))
        .collect();

    Arc::new(OntologyBiDict::new(
        MONDO_REF.clone(),
        mock_mondo_label_to_id,
        HashMap::new(),
        mock_mondo_id_to_label,
    ))
});
pub(crate) static HPO: Lazy<Arc<FullCsrOntology>> = Lazy::new(|| {
    let result = ONTOLOGY_FACTORY
        .lock()
        .unwrap()
        .build_ontology(&HPO_REF, None);

    result.unwrap_or_else(|err| panic!("{}", err))
});
pub(crate) static HPO_DICT: Lazy<Arc<OntologyBiDict>> = Lazy::new(|| {
    let result = ONTOLOGY_FACTORY
        .lock()
        .unwrap()
        .build_bidict(&HPO_REF.clone(), None);

    result.unwrap_or_else(|err| panic!("{}", err))
});

pub(crate) static UO_DICT: Lazy<Arc<OntologyBiDict>> = Lazy::new(|| {
    ONTOLOGY_FACTORY
        .lock()
        .unwrap()
        .build_bidict(&UO_REF.clone(), None)
        .unwrap()
});

pub(crate) static PATO_DICT: Lazy<Arc<OntologyBiDict>> = Lazy::new(|| {
    ONTOLOGY_FACTORY
        .lock()
        .unwrap()
        .build_bidict(&PATO_REF.clone(), None)
        .unwrap()
});
