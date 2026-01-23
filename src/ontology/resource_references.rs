use crate::ontology::traits::{HasPrefixId, HasVersion};
use ontology_registry;
use ontology_registry::enums::Version;
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};
use std::ops::Deref;
use strum_macros::{EnumString, VariantNames};

#[derive(Debug, PartialEq, Clone, Default, Eq, Hash, Deserialize, Serialize)]
pub struct ResourceRef {
    version: String,
    prefix_id: String,
}

impl ResourceRef {
    pub fn new(prefix_id: impl Into<String>, version: String) -> Self {
        Self {
            version,
            prefix_id: prefix_id.into(),
        }
    }

    pub fn as_version(&self) -> Version {
        match self.version.as_str() {
            "latest" => Version::Latest,
            _ => Version::Declared(self.version.clone()),
        }
    }
}

impl Display for ResourceRef {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.prefix_id, self.version)
    }
}

impl HasVersion for ResourceRef {
    fn version(&self) -> &str {
        &self.version
    }
}

impl HasPrefixId for ResourceRef {
    fn prefix_id(&self) -> &str {
        &self.prefix_id
    }
}

#[derive(Debug, PartialEq, Clone, Default, Eq, Hash, Deserialize, Serialize)]
pub struct OntologyRef(ResourceRef);

impl OntologyRef {
    pub fn new(prefix_id: impl Into<String>, version: Option<String>) -> Self {
        OntologyRef(ResourceRef {
            prefix_id: prefix_id.into(),
            version: version.unwrap_or_else(|| "latest".to_string()),
        })
    }
    #[allow(dead_code)]
    fn with_prefix(mut self, prefix: &str) -> Self {
        self.0.prefix_id = prefix.to_string();
        self
    }

    pub fn with_version(mut self, version: &str) -> Self {
        self.0.version = version.to_string();
        self
    }

    pub fn into_inner(self) -> ResourceRef {
        self.0
    }
    pub fn as_inner(&self) -> &ResourceRef {
        &self.0
    }
    pub fn hp() -> Self {
        Self::new(KnownResourcePrefixes::HP, None)
    }

    pub fn mondo() -> Self {
        Self::new(KnownResourcePrefixes::MONDO, None)
    }

    pub fn uo() -> Self {
        Self::new(KnownResourcePrefixes::UO, None)
    }
}

impl From<ResourceRef> for OntologyRef {
    fn from(value: ResourceRef) -> Self {
        Self(value)
    }
}

impl HasVersion for OntologyRef {
    fn version(&self) -> &str {
        self.0.version()
    }
}

impl HasPrefixId for OntologyRef {
    fn prefix_id(&self) -> &str {
        self.0.prefix_id()
    }
}

impl Deref for OntologyRef {
    type Target = ResourceRef;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Display for OntologyRef {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.0.prefix_id, self.0.version)
    }
}

impl From<String> for OntologyRef {
    fn from(prefix_id: String) -> Self {
        OntologyRef::new(prefix_id, None)
    }
}

impl From<&str> for OntologyRef {
    fn from(prefix_id: &str) -> Self {
        OntologyRef::from(prefix_id.to_string())
    }
}

#[derive(Debug, PartialEq, Clone, EnumString, VariantNames)]
#[allow(clippy::upper_case_acronyms)]
pub(crate) enum KnownResourcePrefixes {
    HP,
    MONDO,
    HGNC,
    HGVS,
    LOINC,
    UO,
    OMIM,
    PATO,
}

impl Display for KnownResourcePrefixes {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let as_str = match self {
            KnownResourcePrefixes::HP => "HP",
            KnownResourcePrefixes::MONDO => "MONDO",
            KnownResourcePrefixes::HGNC => "HGNC",
            KnownResourcePrefixes::HGVS => "HGVS",
            KnownResourcePrefixes::LOINC => "LOINC",
            KnownResourcePrefixes::UO => "UO",
            KnownResourcePrefixes::OMIM => "OMIM",
            KnownResourcePrefixes::PATO => "PATO",
        };
        write!(f, "{}", as_str)
    }
}

impl From<KnownResourcePrefixes> for ResourceRef {
    fn from(value: KnownResourcePrefixes) -> Self {
        ResourceRef {
            prefix_id: value.to_string(),
            version: "latest".to_string(),
        }
    }
}

impl From<KnownResourcePrefixes> for String {
    fn from(value: KnownResourcePrefixes) -> Self {
        value.to_string()
    }
}
