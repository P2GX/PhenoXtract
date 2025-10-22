type Version = String;
type OntologyPrefix = String;

#[derive(Eq, Hash, PartialEq, Clone, Debug)]
pub enum OntologyRef {
    Hpo(Option<Version>),
    Mondo(Option<Version>),
    Geno(Option<Version>),
    Omim(Option<Version>),
    Other(OntologyPrefix, Option<Version>),
}

impl OntologyRef {
    pub fn version(&self) -> &str {
        match self {
            OntologyRef::Hpo(version)
            | OntologyRef::Mondo(version)
            | OntologyRef::Geno(version)
            | OntologyRef::Omim(version)
            | OntologyRef::Other(_, version) => {
                version.as_ref().map(|s| s.as_str()).unwrap_or("latest")
            }
        }
    }

    pub fn with_version(&self, version: &str) -> OntologyRef {
        match self {
            OntologyRef::Hpo(_) => OntologyRef::Hpo(Some(version.to_string())),
            OntologyRef::Mondo(_) => OntologyRef::Mondo(Some(version.to_string())),
            OntologyRef::Geno(_) => OntologyRef::Geno(Some(version.to_string())),
            OntologyRef::Omim(_) => OntologyRef::Omim(Some(version.to_string())),
            OntologyRef::Other(prefix, _) => {
                OntologyRef::Other(prefix.clone(), Some(version.to_string()))
            }
        }
    }

    pub fn into_tuple(self) -> (OntologyPrefix, Version) {
        (self.to_string(), self.version().to_string())
    }
}

impl From<String> for OntologyRef {
    fn from(s: String) -> Self {
        match s.to_lowercase().as_ref() {
            "hp" => OntologyRef::Hpo(None),
            "mondo" => OntologyRef::Mondo(None),
            "geno" => OntologyRef::Geno(None),
            "omim" => OntologyRef::Omim(None),
            _ => OntologyRef::Other(s, None),
        }
    }
}

impl std::fmt::Display for OntologyRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Hpo(_) => write!(f, "HP"),
            Self::Mondo(_) => write!(f, "MONDO"),
            Self::Geno(_) => write!(f, "GENO"),
            Self::Omim(_) => write!(f, "OMIM"),
            Self::Other(prefix, _) => write!(f, "{}", prefix),
        }
    }
}
