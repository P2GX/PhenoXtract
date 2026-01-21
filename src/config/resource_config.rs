use crate::ontology::OntologyRef;
use crate::ontology::resource_references::ResourceRef;
use crate::ontology::traits::{HasPrefixId, HasVersion};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
pub struct ResourceConfig {
    pub id: String,
    pub version: Option<String>,
    pub secrets: Option<Secrets>,
}

impl ResourceConfig {
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            version: None,
            secrets: None,
        }
    }

    pub fn with_version(mut self, version: impl Into<String>) -> Self {
        self.version = Some(version.into());
        self
    }

    pub fn with_token(mut self, token: impl Into<String>) -> Self {
        self.secrets = Some(Secrets::Token {
            token: token.into(),
        });
        self
    }

    pub fn with_credentials(
        mut self,
        user: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        self.secrets = Some(Secrets::Credentials {
            user: user.into(),
            password: password.into(),
        });
        self
    }
}

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
#[serde(untagged)]
pub enum Secrets {
    Credentials { user: String, password: String },
    Token { token: String },
}

impl From<ResourceRef> for ResourceConfig {
    fn from(value: ResourceRef) -> Self {
        ResourceConfig {
            id: value.prefix_id().to_string(),
            version: Some(value.version().to_string()),
            secrets: None,
        }
    }
}

impl From<OntologyRef> for ResourceConfig {
    fn from(value: OntologyRef) -> Self {
        ResourceConfig {
            id: value.prefix_id().to_string(),
            version: Some(value.version().to_string()),
            secrets: None,
        }
    }
}

impl Default for ResourceConfig {
    fn default() -> Self {
        Self {
            id: "".to_string(),
            version: None,
            secrets: None,
        }
    }
}
