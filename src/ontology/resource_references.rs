use crate::ontology::traits::{HasPrefixId, HasVersion};
use std::fmt::{Display, Formatter};
use std::ops::Deref;

#[derive(Debug, PartialEq, Clone, Default, Eq, Hash)]
pub struct ResourceRef {
    version: String,
    prefix_id: String,
}

impl ResourceRef {
    pub fn new(prefix_id: String, version: String) -> Self {
        Self { version, prefix_id }
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

#[derive(Debug, PartialEq, Clone, Default, Eq, Hash)]
pub struct OntologyRef(ResourceRef);

impl OntologyRef {
    pub const HPO_PREFIX: &'static str = "hp";
    pub const MONDO_PREFIX: &'static str = "mondo";
    pub const GENO_PREFIX: &'static str = "geno";

    pub fn new(prefix_id: String, version: Option<String>) -> Self {
        OntologyRef(ResourceRef {
            prefix_id,
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
    pub fn hp(version: Option<String>) -> Self {
        Self::new(Self::HPO_PREFIX.to_string(), version)
    }

    pub fn mondo(version: Option<String>) -> Self {
        Self::new(Self::MONDO_PREFIX.to_string(), version)
    }

    pub fn geno(version: Option<String>) -> Self {
        Self::new(Self::GENO_PREFIX.to_string(), version)
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

#[derive(Debug, PartialEq, Clone, Default, Eq, Hash)]
pub struct DatabaseRef(ResourceRef);

impl DatabaseRef {
    pub const OMIM_PREFIX: &'static str = "omim";
    pub const HGNC_PREFIX: &'static str = "hgnc";

    pub fn new(prefix_id: String, version: Option<String>) -> Self {
        DatabaseRef(ResourceRef {
            prefix_id,
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

    pub fn omim(version: Option<String>) -> Self {
        Self::new(Self::OMIM_PREFIX.to_string(), version)
    }

    pub fn hgnc(version: Option<String>) -> Self {
        Self::new(Self::HGNC_PREFIX.to_string(), version)
    }
}

impl HasVersion for DatabaseRef {
    fn version(&self) -> &str {
        self.0.version()
    }
}

impl HasPrefixId for DatabaseRef {
    fn prefix_id(&self) -> &str {
        self.0.prefix_id()
    }
}

impl Deref for DatabaseRef {
    type Target = ResourceRef;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Display for DatabaseRef {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.0.prefix_id, self.0.version)
    }
}

impl From<String> for DatabaseRef {
    fn from(prefix_id: String) -> Self {
        DatabaseRef::new(prefix_id, None)
    }
}

impl From<&str> for DatabaseRef {
    fn from(prefix_id: &str) -> Self {
        DatabaseRef::from(prefix_id.to_string())
    }
}
