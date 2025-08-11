use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct Phenopacket {
    // Wrong structure, subject ID is usually lower in the hierarchy, but its here so the code compiles
    pub subject_id: String,
}

impl Phenopacket {
    //TODO
    pub fn new(subject_id: String) -> Self {
        Phenopacket { subject_id }
    }
}
