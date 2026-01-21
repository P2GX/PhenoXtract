use crate::config::resource_config::{ResourceConfig, Secrets};
use crate::ontology::error::FactoryError;
use crate::ontology::loinc_client::LoincClient;
use crate::ontology::resource_references::KnownPrefixes;
use crate::ontology::traits::BiDict;
use crate::ontology::{CachedOntologyFactory, OntologyRef};

#[derive(Default)]
pub(crate) struct ResourceConfigFactory {
    ontology_factory: CachedOntologyFactory,
}

impl ResourceConfigFactory {
    pub fn build(&mut self, config: ResourceConfig) -> Result<Box<dyn BiDict>, FactoryError> {
        if config.id.to_uppercase() == KnownPrefixes::LOINC.to_string() {
            Self::build_loinc_client(config)
        } else {
            let ontology_bidict = self
                .ontology_factory
                .build_bidict(&OntologyRef::new(config.id, config.version), None)?;
            Ok(Box::new(ontology_bidict))
        }
    }

    fn build_loinc_client(config: ResourceConfig) -> Result<Box<dyn BiDict>, FactoryError> {
        match config.secrets {
            None => Err(FactoryError::CantBuild {
                reason: "No LOINC credentials provided.".to_string(),
            }),
            Some(secrets) => match secrets {
                Secrets::Credentials { user, password } => {
                    Ok(Box::new(LoincClient::new(user, password, None)))
                }
                Secrets::Token { .. } => Err(FactoryError::CantBuild {
                    reason: "LOINC API needs password and username to be configured".to_string(),
                }),
            },
        }
    }

    pub fn into_ontology_factory(self) -> CachedOntologyFactory {
        self.ontology_factory
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ontology::error::FactoryError;
    use crate::ontology::resource_references::KnownPrefixes;

    fn get_factory() -> ResourceConfigFactory {
        ResourceConfigFactory::default()
    }

    #[test]
    fn test_build_loinc_success() {
        let mut factory = get_factory();

        let config = ResourceConfig {
            id: KnownPrefixes::LOINC.into(),
            version: None,
            secrets: Some(Secrets::Credentials {
                user: "test_user".to_string(),
                password: "test_password".to_string(),
            }),
        };

        let result = factory.build(config);

        assert!(
            result.is_ok(),
            "Should successfully build LOINC client with credentials"
        );
    }

    #[test]
    fn test_build_loinc_case_insensitive() {
        let mut factory = get_factory();

        let config = ResourceConfig {
            id: "loinc".to_string(),
            version: None,
            secrets: Some(Secrets::Credentials {
                user: "u".to_string(),
                password: "p".to_string(),
            }),
        };

        let result = factory.build(config);
        assert!(
            result.is_ok(),
            "Should handle 'loinc' (lowercase) same as 'LOINC'"
        );
    }

    #[test]
    fn test_build_loinc_no_secrets_error() {
        let mut factory = get_factory();

        let config = ResourceConfig {
            id: KnownPrefixes::LOINC.into(),
            version: None,
            secrets: None,
        };

        let result = factory.build(config);

        match result {
            Err(FactoryError::CantBuild { reason }) => {
                assert_eq!(reason, "No LOINC credentials provided.");
            }
            _ => panic!(
                "Expected CantBuild error for missing secrets, got {:?}",
                result
            ),
        }
    }

    #[test]
    fn test_build_loinc_token_error() {
        let mut factory = get_factory();

        let config = ResourceConfig {
            id: KnownPrefixes::LOINC.into(),
            version: None,
            secrets: Some(Secrets::Token {
                token: "12345".to_string(),
            }),
        };

        let result = factory.build(config);

        match result {
            Err(FactoryError::CantBuild { reason }) => {
                assert!(reason.contains("needs password and username"));
            }
            _ => panic!("Expected CantBuild error for token usage, got {:?}", result),
        }
    }

    #[test]
    fn test_build_generic_ontology() {
        let mut factory = get_factory();

        let config = ResourceConfig {
            id: "RO".to_string(),
            version: Some("2025-06-24".to_string()),
            secrets: None,
        };

        let result = factory.build(config);

        assert!(result.is_ok());
    }
}
