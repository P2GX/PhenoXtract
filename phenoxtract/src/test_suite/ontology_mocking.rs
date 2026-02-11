use crate::ontology::ontology_bidict::OntologyBiDict;
use crate::test_suite::mocks::ONTOLOGY_FACTORY;
use crate::test_suite::phenopacket_component_generation::{
    default_disease_oc, default_procedure_body_side_oc, default_procedure_oc,
    default_treatment_intent, default_treatment_response, default_treatment_termination_reason,
};
use crate::test_suite::resource_references::{
    HPO_REF, MAXO_REF, MONDO_REF, NCIT_REF, PATO_REF, UBERON_REF, UO_REF,
};
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
        (default_disease_oc().label, default_disease_oc().id),
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

pub(crate) static UBERON_BIDICT: Lazy<Arc<OntologyBiDict>> = Lazy::new(|| {
    let mock_uberon_label_to_id: HashMap<String, String> = HashMap::from_iter([
        (
            default_procedure_body_side_oc().label,
            default_procedure_body_side_oc().id,
        ),
        ("head".to_string(), "UBERON:0000033".to_string()),
        ("bone spine".to_string(), "UBERON:0013706".to_string()),
        ("fascia lata".to_string(), "UBERON:0003669".to_string()),
        ("nasal septum".to_string(), "UBERON:0001706".to_string()),
    ]);

    let mock_uberon_id_to_label: HashMap<String, String> = mock_uberon_label_to_id
        .iter()
        .map(|(label, id)| (id.to_string(), label.to_string()))
        .collect();

    Arc::new(OntologyBiDict::new(
        UBERON_REF.clone(),
        mock_uberon_label_to_id,
        HashMap::new(),
        mock_uberon_id_to_label,
    ))
});

pub(crate) static MAXO_BIDICT: Lazy<Arc<OntologyBiDict>> = Lazy::new(|| {
    let mock_maxo_label_to_id: HashMap<String, String> = HashMap::from_iter([
        (default_procedure_oc().label, default_procedure_oc().id),
        (
            "clinical laboratory procedure".to_string(),
            "MAXO:0000006".to_string(),
        ),
        ("nephrostomy".to_string(), "MAXO:0001350".to_string()),
        (
            "microwave diathermy".to_string(),
            "MAXO:0000027".to_string(),
        ),
        (
            "cognitive behavior therapy".to_string(),
            "MAXO:0000883".to_string(),
        ),
    ]);

    let mock_maxo_id_to_label: HashMap<String, String> = mock_maxo_label_to_id
        .iter()
        .map(|(label, id)| (id.to_string(), label.to_string()))
        .collect();

    Arc::new(OntologyBiDict::new(
        MAXO_REF.clone(),
        mock_maxo_label_to_id,
        HashMap::new(),
        mock_maxo_id_to_label,
    ))
});

pub(crate) static NCIT_BIDICT: Lazy<Arc<OntologyBiDict>> = Lazy::new(|| {
    let mock_uberon_label_to_id: HashMap<String, String> = HashMap::from_iter([
        (
            default_treatment_intent().label,
            default_treatment_intent().id,
        ),
        (
            default_treatment_termination_reason().label,
            default_treatment_termination_reason().id,
        ),
        (
            default_treatment_response().label,
            default_treatment_response().id,
        ),
    ]);

    let mock_uberon_id_to_label: HashMap<String, String> = mock_uberon_label_to_id
        .iter()
        .map(|(label, id)| (id.to_string(), label.to_string()))
        .collect();

    Arc::new(OntologyBiDict::new(
        NCIT_REF.clone(),
        mock_uberon_label_to_id,
        HashMap::new(),
        mock_uberon_id_to_label,
    ))
});

pub(crate) static HPO: Lazy<Arc<FullCsrOntology>> = Lazy::new(|| {
    ONTOLOGY_FACTORY
        .lock()
        .unwrap()
        .build_ontology(&HPO_REF, None)
        .unwrap()
});
pub(crate) static HPO_DICT: Lazy<Arc<OntologyBiDict>> = Lazy::new(|| {
    ONTOLOGY_FACTORY
        .lock()
        .unwrap()
        .build_bidict(&HPO_REF.clone(), None)
        .unwrap()
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
