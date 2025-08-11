use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct Phenopacket {
    // Wrong structure, subject ID is usually lower in the hierarchy, but its here so the code compiles
    pub _subject_id: String,
}

impl Phenopacket {
    //TODO
    #[allow(dead_code)]
    pub fn new(subject_id: String) -> Self {
        Phenopacket {
            _subject_id: subject_id,
        }
    }
}
