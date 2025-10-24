use crate::ontology::ObolibraryOntologyRegistry;
use crate::ontology::error::OntologyFactoryError;
use crate::ontology::ontology_bidict::OntologyBiDict;
use crate::ontology::resource_references::OntologyRef;
use crate::ontology::traits::{HasPrefixId, HasVersion, OntologyRegistry};
use ontolius::io::OntologyLoaderBuilder;
use ontolius::ontology::csr::FullCsrOntology;
use serde::de::StdError;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, OnceLock};

#[derive(Debug, Clone, Eq, Hash, PartialEq)]
struct CacheKey {
    ontology: OntologyRef,
    file_name: Option<String>,
}

#[derive(Debug)]
struct CachedOntology {
    ontology: Arc<FullCsrOntology>,
    bidict: OnceLock<Arc<OntologyBiDict>>,
}

#[derive(Default, Debug)]
pub struct CachedOntologyFactory {
    cache: HashMap<CacheKey, CachedOntology>,
}

impl CachedOntologyFactory {
    pub fn build_ontology(
        &mut self,
        ontology: &OntologyRef,
        file_name: Option<&str>,
    ) -> Result<Arc<FullCsrOntology>, OntologyFactoryError> {
        let cache_key = CacheKey {
            ontology: ontology.clone(),
            file_name: file_name.map(str::to_string),
        };

        if let Some(onto) = self.cache.get(&cache_key) {
            return Ok(onto.ontology.clone());
        }

        let mut registry = match ontology.prefix_id() {
            OntologyRef::HPO_PREFIX => ObolibraryOntologyRegistry::default_hpo_registry(),
            OntologyRef::MONDO_PREFIX => ObolibraryOntologyRegistry::default_mondo_registry(),
            OntologyRef::GENO_PREFIX => ObolibraryOntologyRegistry::default_geno_registry(),
            _ => {
                let registry_path =
                    ObolibraryOntologyRegistry::default_registry_path(ontology.prefix_id())
                        .map_err(|err| Self::wrap_error(err, ontology))?;
                Ok(ObolibraryOntologyRegistry::new(
                    registry_path,
                    file_name,
                    ontology.prefix_id(),
                ))
            }
        }
        .map_err(|err| Self::wrap_error(err, ontology))?;

        let ontology_path = registry
            .register(ontology.version())
            .map_err(|err| Self::wrap_error(err, ontology))?;

        let ontology_build =
            Self::init_ontolius(ontology_path).map_err(|err| Self::wrap_error(err, ontology))?;

        self.cache.insert(
            cache_key,
            CachedOntology {
                ontology: ontology_build.clone(),
                bidict: OnceLock::new(),
            },
        );

        Ok(ontology_build)
    }

    pub fn build_bidict(
        &mut self,
        ontology: &OntologyRef,
        file_name: Option<&str>,
    ) -> Result<Arc<OntologyBiDict>, OntologyFactoryError> {
        let key = CacheKey {
            ontology: ontology.clone(),
            file_name: file_name.map(str::to_string),
        };

        self.build_ontology(ontology, file_name)?;

        let cached = self.cache.get(&key).expect("Just inserted");

        let bidict = cached.bidict.get_or_init(|| {
            Arc::new(OntologyBiDict::from_ontology(
                cached.ontology.clone(),
                &ontology.to_string(),
            ))
        });

        Ok(bidict.clone())
    }

    fn init_ontolius(hpo_path: PathBuf) -> Result<Arc<FullCsrOntology>, anyhow::Error> {
        let loader = OntologyLoaderBuilder::new().obographs_parser().build();

        let ontolius = loader.load_from_path(hpo_path.clone())?;
        Ok(Arc::new(ontolius))
    }

    fn wrap_error<E: Into<Box<dyn StdError + Send + Sync>>>(
        err: E,
        ontology: &OntologyRef,
    ) -> OntologyFactoryError {
        OntologyFactoryError::CantBuild {
            source: err.into(),
            ontology: ontology.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    #[rstest]
    fn test_build_ontology_success() -> Result<(), OntologyFactoryError> {
        let ontology = OntologyRef::geno(Option::from("2025-07-25".to_string()));

        let mut factory = CachedOntologyFactory::default();
        let result = factory.build_ontology(&ontology, None)?;

        assert!(Arc::strong_count(&result) >= 1);

        assert!(factory.cache.contains_key(&CacheKey {
            ontology: ontology.clone(),
            file_name: None,
        }));

        Ok(())
    }

    #[rstest]
    fn test_build_bidict() -> Result<(), OntologyFactoryError> {
        let ontology = OntologyRef::geno(Option::from("2025-07-25".to_string()));

        let mut factory = CachedOntologyFactory::default();
        let result = factory.build_bidict(&ontology, None)?;

        assert!(Arc::strong_count(&result) >= 1);

        assert!(factory.cache.contains_key(&CacheKey {
            ontology: ontology.clone(),
            file_name: None,
        }));

        Ok(())
    }

    #[rstest]
    fn test_build_bidict_other() -> Result<(), OntologyFactoryError> {
        let ontology = OntologyRef::new("ro".to_string(), None);

        let mut factory = CachedOntologyFactory::default();
        let result = factory.build_bidict(&ontology, None)?;

        assert!(Arc::strong_count(&result) >= 1);

        assert!(factory.cache.contains_key(&CacheKey {
            ontology: ontology.clone(),
            file_name: None,
        }));

        Ok(())
    }
}
