use crate::ontology::traits::{HasPrefixId, HasVersion};
use ontology_registry;
use ontology_registry::enums::Version;
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};
use strum_macros::{AsRefStr, EnumString, VariantNames};

#[derive(Debug, PartialEq, Clone, Default, Eq, Hash, Deserialize, Serialize)]
pub struct ResourceRef {
    version: String,
    prefix_id: String,
}

impl ResourceRef {
    pub fn new(prefix_id: impl Into<String>, version: Option<impl Into<String>>) -> Self {
        Self {
            version: match version {
                None => "latest".to_string(),
                Some(v) => v.into(),
            },
            prefix_id: prefix_id.into(),
        }
    }

    pub(crate) fn as_version(&self) -> Version {
        match self.version.as_str() {
            "latest" => Version::Latest,
            _ => Version::Declared(self.version.clone()),
        }
    }

    pub fn with_version(mut self, version: &str) -> Self {
        self.version = version.to_string();
        self
    }

    pub fn with_latest(mut self) -> Self {
        self.version = "latest".to_string();
        self
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

impl From<&str> for ResourceRef {
    fn from(prefix_id: &str) -> Self {
        ResourceRef::new(prefix_id, Some("latest"))
    }
}

#[derive(Debug, PartialEq, Clone, EnumString, VariantNames, AsRefStr)]
#[allow(clippy::upper_case_acronyms)]
pub(crate) enum KnownResourcePrefixes {
    HP,
    MONDO,
    HGNC,
    LOINC,
    UO,
    OMIM,
    PATO,
    UBERON,
    MAXO,
    NCIT,
}

/// Auto implementation of convenience functions to construct `ResourceRef`s from `KnownResourcePrefixes`
macro_rules! impl_resource_constructors_no_dep {
    ($($variant:ident => $func_name:ident),* $(,)?) => {
        impl ResourceRef {
            $(
                pub fn $func_name() -> Self {
                    Self::from(KnownResourcePrefixes::$variant).with_latest()
                }
            )*
        }
    };
}

impl_resource_constructors_no_dep!(
    HP => hp,
    MONDO => mondo,
    HGNC => hgnc,
    LOINC => loinc,
    UO => uo,
    OMIM => omim,
    PATO => pato,
    UBERON => uberon,
    MAXO => maxo,
    NCIT => ncit,
);

impl Display for KnownResourcePrefixes {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let as_str = match self {
            KnownResourcePrefixes::HP => "HP",
            KnownResourcePrefixes::MONDO => "MONDO",
            KnownResourcePrefixes::HGNC => "HGNC",
            KnownResourcePrefixes::LOINC => "LOINC",
            KnownResourcePrefixes::UO => "UO",
            KnownResourcePrefixes::OMIM => "OMIM",
            KnownResourcePrefixes::PATO => "PATO",
            KnownResourcePrefixes::UBERON => "UBERON",
            KnownResourcePrefixes::MAXO => "MAXO",
            KnownResourcePrefixes::NCIT => "NCIT",
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
