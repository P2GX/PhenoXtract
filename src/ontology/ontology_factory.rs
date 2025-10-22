use crate::ontology::ObolibraryOntologyRegistry;
use crate::ontology::enums::OntologyRef;
use crate::ontology::error::OntologyFactoryError;
use crate::ontology::ontology_bidict::OntologyBiDict;
use crate::ontology::traits::OntologyRegistry;
use ontolius::io::OntologyLoaderBuilder;
use ontolius::ontology::csr::FullCsrOntology;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

#[derive(Debug)]
enum CacheEntry {
    Ontology(Arc<FullCsrOntology>),
    BiDict(Arc<OntologyBiDict>),
}
#[derive(Debug, Eq, Hash, PartialEq, Clone)]
enum CacheKey {
    Ontology(OntologyRef, Option<String>),
    BiDict(OntologyRef, Option<String>),
}

#[derive(Default, Debug)]
pub struct CachedOntologyFactory {
    cache: HashMap<CacheKey, CacheEntry>,
}

impl CachedOntologyFactory {
    pub fn build_ontology(
        &mut self,
        ontology: &OntologyRef,
        file_name: Option<&str>,
    ) -> Result<Arc<FullCsrOntology>, OntologyFactoryError> {
        let cache_key = CacheKey::Ontology(ontology.clone(), file_name.map(|s| s.to_string()));
        if let Some(CacheEntry::Ontology(cached_dict)) = self.cache.get(&cache_key) {
            return Ok(cached_dict.clone());
        }

        let mut registry = match &ontology {
            OntologyRef::Hpo(_) => ObolibraryOntologyRegistry::default_hpo_registry(),
            OntologyRef::Mondo(_) => ObolibraryOntologyRegistry::default_mondo_registry(),
            OntologyRef::Geno(_) => ObolibraryOntologyRegistry::default_geno_registry(),
            OntologyRef::Omim(_) => {
                panic!("Ontology factory does not support omim registry.")
            }
            OntologyRef::Other(prefix, _) => {
                let registry_path = ObolibraryOntologyRegistry::default_registry_path(prefix)
                    .map_err(|err| Self::wrap_error(err, ontology))?;
                Ok(ObolibraryOntologyRegistry::new(
                    registry_path,
                    file_name,
                    prefix,
                ))
            }
        }
        .map_err(|err| Self::wrap_error(err, ontology))?;

        let ontology_path = registry
            .register(ontology.version())
            .map_err(|err| Self::wrap_error(err, ontology))?;

        let ontology_build =
            Self::init_ontolius(ontology_path).map_err(|err| Self::wrap_error(err, ontology))?;

        self.cache
            .insert(cache_key, CacheEntry::Ontology(ontology_build.clone()));

        Ok(ontology_build)
    }

    pub fn build_bidict(
        &mut self,
        ontology: &OntologyRef,
        file_name: Option<&str>,
    ) -> Result<Arc<OntologyBiDict>, OntologyFactoryError> {
        let cache_key = CacheKey::BiDict(ontology.clone(), file_name.map(|s| s.to_string()));
        if let Some(CacheEntry::BiDict(cached_dict)) = self.cache.get(&cache_key) {
            return Ok(cached_dict.clone());
        }

        let ont = self.build_ontology(ontology, file_name)?;
        let bi_dict = Arc::new(OntologyBiDict::from(ont.as_ref()));

        self.cache
            .insert(cache_key, CacheEntry::BiDict(bi_dict.clone()));

        Ok(bi_dict)
    }

    fn init_ontolius(hpo_path: PathBuf) -> Result<Arc<FullCsrOntology>, anyhow::Error> {
        let loader = OntologyLoaderBuilder::new().obographs_parser().build();

        let ontolius = loader.load_from_path(hpo_path.clone())?;
        Ok(Arc::new(ontolius))
    }

    fn wrap_error<E: Into<anyhow::Error>>(err: E, ontology: &OntologyRef) -> OntologyFactoryError {
        OntologyFactoryError::CantBuild(err.into(), ontology.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    #[rstest]
    fn test_build_ontology_success() -> Result<(), OntologyFactoryError> {
        let ontology = OntologyRef::Geno(Option::from("2025-07-25".to_string()));

        let mut factory = CachedOntologyFactory::default();
        let result = factory.build_ontology(&ontology, None)?;

        assert!(Arc::strong_count(&result) >= 1);

        assert!(
            factory
                .cache
                .contains_key(&CacheKey::Ontology(ontology, None))
        );

        Ok(())
    }

    #[rstest]
    fn test_build_bidict() -> Result<(), OntologyFactoryError> {
        let ontology = OntologyRef::Geno(Option::from("2025-07-25".to_string()));

        let mut factory = CachedOntologyFactory::default();
        let result = factory.build_bidict(&ontology, None)?;

        assert!(Arc::strong_count(&result) >= 1);

        assert!(
            factory
                .cache
                .contains_key(&CacheKey::Ontology(ontology.clone(), None))
        );
        assert!(
            factory
                .cache
                .contains_key(&CacheKey::BiDict(ontology, None))
        );

        Ok(())
    }

    #[rstest]
    fn test_build_bidict_wildcard() -> Result<(), OntologyFactoryError> {
        let ontology = OntologyRef::Other("ro".to_string(), None);

        let mut factory = CachedOntologyFactory::default();
        let result = factory.build_bidict(&ontology, None)?;

        assert!(Arc::strong_count(&result) >= 1);

        assert!(
            factory
                .cache
                .contains_key(&CacheKey::Ontology(ontology.clone(), None))
        );
        assert!(
            factory
                .cache
                .contains_key(&CacheKey::BiDict(ontology.clone(), None))
        );

        Ok(())
    }
}
