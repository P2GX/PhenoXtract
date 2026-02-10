use crate::config::resource_config::{ResourceConfig, Secrets};
use crate::ontology::CachedOntologyFactory;
use crate::ontology::bioportal_client::BioPortalClient;
use crate::ontology::error::FactoryError;
use crate::ontology::loinc_client::LoincClient;
use crate::ontology::resource_references::{KnownResourcePrefixes, ResourceRef};
use crate::ontology::traits::BiDict;
use strum::VariantNames;

#[derive(Default)]
pub(crate) struct ResourceConfigFactory {
    ontology_factory: CachedOntologyFactory,
}

impl ResourceConfigFactory {
    const NON_CONFIGURABLE: [KnownResourcePrefixes; 1] = [KnownResourcePrefixes::HGNC];

    pub fn build(&mut self, config: &ResourceConfig) -> Result<Box<dyn BiDict>, FactoryError> {
        if config
            .id
            .eq_ignore_ascii_case(KnownResourcePrefixes::LOINC.as_ref())
        {
            Self::build_loinc_client(config)
        } else if config
            .id
            .eq_ignore_ascii_case(KnownResourcePrefixes::OMIM.as_ref())
        {
            Self::build_bioportal_client(config)
        } else {
            match self.ontology_factory.build_bidict(
                &ResourceRef::new(config.id.clone(), config.version.clone()),
                None,
            ) {
                Ok(bi_dict) => Ok(Box::new(bi_dict)),
                Err(err) => {
                    let non_creatable_strs: Vec<&str> = Self::NON_CONFIGURABLE
                        .iter()
                        .map(|prefix| prefix.as_ref())
                        .collect();

                    let supported_resources: Vec<&str> = KnownResourcePrefixes::VARIANTS
                        .iter()
                        .copied()
                        .filter(|&variant_id| {
                            !non_creatable_strs
                                .iter()
                                .any(|&nc_id| nc_id.eq_ignore_ascii_case(variant_id))
                        })
                        .collect();

                    let is_known = supported_resources
                        .iter()
                        .any(|&id| id.eq_ignore_ascii_case(&config.id));

                    let reason = if is_known {
                        format!(
                            "Failed to build known resource '{}': {}. This is a supported ontology, so this may indicate a configuration, network, or data source issue.",
                            config.id, err
                        )
                    } else {
                        format!(
                            "Failed to build custom resource '{}': {}. While the system can load compatible external ontologies, this resource could not be built. Known supported resources are: {}. If the configured resource is not supported the system will try to load it as an ontology. The provided id '{}' is either an unsupported service or an ontology that cannot be built.",
                            config.id,
                            err,
                            supported_resources.join(", "),
                            config.id
                        )
                    };

                    Err(FactoryError::CantBuild { reason })
                }
            }
        }
    }

    fn build_loinc_client(config: &ResourceConfig) -> Result<Box<dyn BiDict>, FactoryError> {
        match &config.secrets {
            None => Err(FactoryError::CantBuild {
                reason: "No LOINC credentials provided.".to_string(),
            }),
            Some(secrets) => match secrets {
                Secrets::Credentials { user, password } => {
                    let loinc_ref = config
                        .version
                        .as_ref()
                        .map(|version| ResourceRef::loinc().with_version(version));

                    Ok(Box::new(LoincClient::new(
                        user.clone(),
                        password.clone(),
                        loinc_ref,
                    )))
                }
                Secrets::Token { .. } => Err(FactoryError::CantBuild {
                    reason:
                        "LOINC API needs password and username instead of token to be configured"
                            .to_string(),
                }),
            },
        }
    }

    fn build_bioportal_client(config: &ResourceConfig) -> Result<Box<dyn BiDict>, FactoryError> {
        let secrets = config
            .secrets
            .as_ref()
            .ok_or_else(|| FactoryError::CantBuild {
                reason: format!(
                    "Can't build BioPortal client for '{}': no secrets provided",
                    config.id
                ),
            })?;

        let token = match secrets {
            Secrets::Credentials { .. } => {
                return Err(FactoryError::CantBuild {
                    reason: format!(
                        "BioPortal API requires an API Key, not username/password (resource: '{}')",
                        config.id
                    ),
                });
            }
            Secrets::Token { token } => token,
        };

        let resource_ref = ResourceRef::new(config.id.clone(), config.version.clone());

        let client =
            BioPortalClient::new(token, &config.id, Some(resource_ref)).map_err(|err| {
                FactoryError::CantBuild {
                    reason: err.to_string(),
                }
            })?;

        Ok(Box::new(client))
    }

    pub fn into_ontology_factory(self) -> CachedOntologyFactory {
        self.ontology_factory
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ontology::error::FactoryError;
    use crate::ontology::resource_references::KnownResourcePrefixes;

    fn get_factory() -> ResourceConfigFactory {
        ResourceConfigFactory::default()
    }

    #[test]
    fn test_build_loinc_success() {
        let mut factory = get_factory();

        let config = ResourceConfig {
            id: KnownResourcePrefixes::LOINC.into(),
            version: None,
            secrets: Some(Secrets::Credentials {
                user: "test_user".to_string(),
                password: "test_password".to_string(),
            }),
        };

        let result = factory.build(&config);

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

        let result = factory.build(&config);
        assert!(
            result.is_ok(),
            "Should handle 'loinc' (lowercase) same as 'LOINC'"
        );
    }

    #[test]
    fn test_build_loinc_no_secrets_error() {
        let mut factory = get_factory();

        let config = ResourceConfig {
            id: KnownResourcePrefixes::LOINC.into(),
            version: None,
            secrets: None,
        };

        let result = factory.build(&config);

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
            id: KnownResourcePrefixes::LOINC.into(),
            version: None,
            secrets: Some(Secrets::Token {
                token: "12345".to_string(),
            }),
        };

        let result = factory.build(&config);

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

        let result = factory.build(&config);

        assert!(result.is_ok());
    }

    #[test]
    fn test_build_check_error_message() {
        let mut factory = get_factory();

        let config = ResourceConfig {
            id: "NOT_A_RESOURCE".to_string(),
            version: Some("2025-06-24".to_string()),
            secrets: None,
        };

        let result = factory.build(&config);

        let err = result.err().unwrap();

        let non_configurable_strs: Vec<&str> = ResourceConfigFactory::NON_CONFIGURABLE
            .iter()
            .map(|id| id.as_ref())
            .collect();

        for not_supported in non_configurable_strs {
            assert!(!err.to_string().contains(not_supported));
        }
    }

    #[test]
    fn test_build_bioportal_client_success() {
        let mut factory = ResourceConfigFactory::default();
        let id: String = KnownResourcePrefixes::OMIM.into();
        let version = "2025-06-24".to_string();

        let config = ResourceConfig {
            id: id.clone(),
            version: Some(version.to_string()),
            secrets: Some(Secrets::Token {
                token: "valid_bioportal_api_key".to_string(),
            }),
        };

        let client = factory.build(&config).unwrap();

        assert_eq!(
            client.reference(),
            &ResourceRef::from(id.as_ref()).with_version(&version)
        )
    }

    #[test]
    fn test_bioportal_missing_secrets() {
        let mut factory = ResourceConfigFactory::default();
        let config = ResourceConfig {
            id: KnownResourcePrefixes::OMIM.into(),
            secrets: None,
            ..Default::default()
        };

        let result = factory.build(&config);

        assert!(result.is_err());
    }

    #[test]
    fn test_bioportal_wrong_secret_type() {
        let mut factory = ResourceConfigFactory::default();
        let config = ResourceConfig {
            id: KnownResourcePrefixes::OMIM.into(),
            secrets: Some(Secrets::Credentials {
                user: "u".into(),
                password: "p".into(),
            }),
            ..Default::default()
        };

        let result = factory.build(&config);

        assert!(result.is_err());
    }
}
