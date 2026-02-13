use crate::ontology::error::FactoryError;
use crate::ontology::ontology_bidict::OntologyBiDict;
use crate::ontology::resource_references::{KnownResourcePrefixes, ResourceRef};
use crate::ontology::traits::HasPrefixId;
use crate::ontology::types::OntologyRegistry;
use crate::utils::default_cache_dir;
use ontolius::io::OntologyLoaderBuilder;
use ontolius::ontology::csr::FullCsrOntology;
use ontology_registry::blocking::bio_registry_metadata_provider::BioRegistryMetadataProvider;
use ontology_registry::blocking::file_system_ontology_registry::FileSystemOntologyRegistry;
use ontology_registry::blocking::obolib_ontology_provider::OboLibraryProvider;
use ontology_registry::enums::FileType;
use ontology_registry::traits::OntologyRegistration;
use ontology_registry::traits::OntologyRegistration;
use std::collections::HashMap;
use std::fmt::Display;
use std::io::Read;
use std::io::Read;
use std::sync::{Arc, OnceLock};

#[derive(Debug, Clone, Eq, Hash, PartialEq)]
struct CacheKey {
    ontology: ResourceRef,
    file_name: Option<String>,
}

#[derive(Debug)]
struct CachedOntology {
    ontology: Arc<FullCsrOntology>,
    bidict: OnceLock<Arc<OntologyBiDict>>,
}

#[derive(Debug)]
pub struct CachedOntologyFactory<OR: OntologyRegistration> {
pub struct CachedOntologyFactory<OR: OntologyRegistration> {
    cache: HashMap<CacheKey, CachedOntology>,
    registry: OR,
    registry: OR,
}

/// A factory for creating and caching ontology instances.
///
/// `CachedOntologyFactory` manages the lifecycle of ontology objects, providing efficient
/// reuse through caching. It prevents redundant loading of the same ontology by maintaining
/// an internal cache keyed by ontology reference and optional file name.
///
/// # Caching Behavior
///
/// The factory caches ontologies based on their `ResourceRef` and an optional file name.
/// Once an ontology is built, subsequent requests for the same ontology will return the
/// cached instance, avoiding expensive I/O and parsing operations.
///
/// # Thread Safety
///
/// The ontology instances themselves are wrapped in `Arc`, making them safe to share
/// across threads. However, the factory itself requires mutable access for building
/// operations.
///
/// # Examples
///
/// ```ignore
/// use phenoxtract::ontology::CachedOntologyFactory;
///
/// let mut factory = CachedOntologyFactory::default();
///
/// // Load the latest HPO ontology
/// let hpo = factory.hp(None)?;
///
/// // Load a specific version of MONDO
/// let mondo = factory.mondo(Some("2023-01-01".to_string()))?;
///
/// // Get the bidirectional dictionary for HPO
/// let hpo_bidict = factory.hp_bi_dict(None)?;
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
impl<OR: OntologyRegistration> CachedOntologyFactory<OR> {
    pub fn new(registry: OR) -> Self {
impl<OR: OntologyRegistration> CachedOntologyFactory<OR> {
    pub fn new(registry: OR) -> Self {
        Self {
            cache: HashMap::new(),
            registry,
        }
    }

    /// Builds or retrieves a cached ontology instance.
    ///
    /// This is the core method for loading ontologies. It first checks the cache for an
    /// existing instance matching the given `ontology` reference and `file_name`. If found,
    /// it returns the cached instance. Otherwise, it loads the ontology from disk, caches
    /// it, and returns the newly created instance.
    ///
    /// # Arguments
    ///
    /// * `ontology` - Reference specifying which ontology to load (HPO, MONDO, GENO, etc.)
    /// * `file_name` - Optional specific file name to load. If `None`, uses the default
    ///   file for the ontology type.
    ///
    /// # Returns
    ///
    /// Returns an `Arc<FullCsrOntology>` on success, allowing the ontology to be shared
    /// efficiently across multiple consumers.
    ///
    /// # Errors
    ///
    /// Returns `OntologyFactoryError` if:
    /// - The registry path cannot be determined
    /// - The ontology file cannot be registered or located
    /// - The ontology file cannot be parsed or initialized
    pub fn build_ontology(
        &mut self,
        ontology: &ResourceRef,
        file_name: Option<&str>,
    ) -> Result<Arc<FullCsrOntology>, FactoryError> {
        let cache_key = CacheKey {
            ontology: ontology.clone(),
            file_name: file_name.map(str::to_string),
        };

        if let Some(onto) = self.cache.get(&cache_key) {
            return Ok(onto.ontology.clone());
        }

        let lower = ontology.prefix_id().to_lowercase();
        let lower = ontology.prefix_id().to_lowercase();
        let ontology_path = self
            .registry
            .register(
                &lower,
                ontology.clone().as_version(),
                FileType::Json, // Hardcoded json, because ontolius depends on it
                &lower,
                ontology.clone().as_version(),
                FileType::Json, // Hardcoded json, because ontolius depends on it
            )
            .map_err(|err| Self::cant_build_err_wrap(err, ontology))?;

        let ontology_build = Self::init_ontolius(ontology_path)
            .map_err(|err| Self::cant_build_err_wrap(err, ontology))?;

        self.cache.insert(
            cache_key,
            CachedOntology {
                ontology: ontology_build.clone(),
                bidict: OnceLock::new(),
            },
        );

        Ok(ontology_build)
    }

    /// Builds or retrieves a cached bidirectional dictionary for an ontology.
    ///
    /// Creates an `OntologyBiDict` that provides efficient bidirectional lookups between
    /// ontology term IDs and their string representations. The bidict is lazily initialized
    /// and cached alongside its parent ontology.
    ///
    /// This method first ensures the ontology itself is loaded (calling `build_ontology`
    /// internally), then creates or retrieves the cached bidirectional dictionary.
    ///
    /// # Arguments
    ///
    /// * `ontology_ref` - Reference specifying which ontology to use
    /// * `file_name` - Optional specific file name. If `None`, uses the default file.
    ///
    /// # Returns
    ///
    /// Returns an `Arc<OntologyBiDict>` on success.
    ///
    /// # Errors
    ///
    /// Returns `OntologyFactoryError` if the underlying ontology cannot be built.
    pub fn build_bidict(
        &mut self,
        ontology_ref: &ResourceRef,
        file_name: Option<&str>,
    ) -> Result<Arc<OntologyBiDict>, FactoryError> {
        let key = CacheKey {
            ontology: ontology_ref.clone(),
            file_name: file_name.map(str::to_string),
        };

        self.build_ontology(ontology_ref, file_name)?;

        let cached = self.cache.get(&key).expect("Just inserted");

        let bidict = cached.bidict.get_or_init(|| {
            Arc::new(OntologyBiDict::from_ontology(
                cached.ontology.clone(),
                ontology_ref.prefix_id(),
            ))
        });

        Ok(bidict.clone())
    }

    /// Loads the Human Phenotype Ontology (HPO).
    ///
    /// Convenience method for loading the HPO ontology without needing to construct
    /// an `ResourceRef` manually.
    ///
    /// # Arguments
    ///
    /// * `version` - Optional version string (e.g., "2023-04-01"). If `None`, loads
    ///   the latest available version.
    ///
    /// # Returns
    ///
    /// Returns an `Arc<FullCsrOntology>` containing the HPO ontology.
    ///
    /// # Errors
    ///
    /// Returns `OntologyFactoryError` if the ontology cannot be loaded.
    pub fn hp(&mut self, version: Option<String>) -> Result<Arc<FullCsrOntology>, FactoryError> {
        let onto_ref = ResourceRef::new(KnownResourcePrefixes::HP, version);
        self.build_ontology(&onto_ref, None)
    }
    /// Loads the bidirectional dictionary for the Human Phenotype Ontology (HPO).
    ///
    /// Convenience method for loading the HPO bidict without needing to construct
    /// an `ResourceRef` manually.
    ///
    /// # Arguments
    ///
    /// * `version` - Optional version string. If `None`, uses the latest version.
    ///
    /// # Returns
    ///
    /// Returns an `Arc<OntologyBiDict>` for the HPO ontology.
    ///
    /// # Errors
    ///
    /// Returns `OntologyFactoryError` if the ontology or bidict cannot be created.
    pub fn hp_bi_dict(
        &mut self,
        version: Option<String>,
    ) -> Result<Arc<OntologyBiDict>, FactoryError> {
        let onto_ref = ResourceRef::new(KnownResourcePrefixes::HP, version);

        self.build_bidict(&onto_ref, None)
    }
    /// Loads the Mondo Disease Ontology (MONDO).
    ///
    /// Convenience method for loading the MONDO ontology.
    ///
    /// # Arguments
    ///
    /// * `version` - Optional version string. If `None`, loads the latest version.
    ///
    /// # Returns
    ///
    /// Returns an `Arc<FullCsrOntology>` containing the MONDO ontology.
    ///
    /// # Errors
    ///
    /// Returns `OntologyFactoryError` if the ontology cannot be loaded.
    pub fn mondo(&mut self, version: Option<String>) -> Result<Arc<FullCsrOntology>, FactoryError> {
        let onto_ref = ResourceRef::new(KnownResourcePrefixes::MONDO, version);
        self.build_ontology(&onto_ref, None)
    }
    /// Loads the bidirectional dictionary for the Mondo Disease Ontology (MONDO).
    ///
    /// Convenience method for loading the MONDO bidict.
    ///
    /// # Arguments
    ///
    /// * `version` - Optional version string. If `None`, uses the latest version.
    ///
    /// # Returns
    ///
    /// Returns an `Arc<OntologyBiDict>` for the MONDO ontology.
    ///
    /// # Errors
    ///
    /// Returns `OntologyFactoryError` if the ontology or bidict cannot be created.
    pub fn mondo_bi_dict(
        &mut self,
        version: Option<String>,
    ) -> Result<Arc<OntologyBiDict>, FactoryError> {
        let onto_ref = ResourceRef::new(KnownResourcePrefixes::MONDO, version);
        self.build_bidict(&onto_ref, None)
    }

    fn init_ontolius(ontology_path: impl Read) -> Result<Arc<FullCsrOntology>, anyhow::Error> {
    fn init_ontolius(ontology_path: impl Read) -> Result<Arc<FullCsrOntology>, anyhow::Error> {
        let loader = OntologyLoaderBuilder::new().obographs_parser().build();

        let ontolius = loader.load_from_read(ontology_path)?;
        let ontolius = loader.load_from_read(ontology_path)?;
        Ok(Arc::new(ontolius))
    }

    fn cant_build_err_wrap<E: Display>(err: E, ontology: &ResourceRef) -> FactoryError {
        FactoryError::CantBuild {
            reason: format!("for ontology '{}' '{}'", ontology, err),
        }
    }
}

impl Default for CachedOntologyFactory<OntologyRegistry> {
impl Default for CachedOntologyFactory<OntologyRegistry> {
    fn default() -> Self {
        CachedOntologyFactory::new(FileSystemOntologyRegistry::new(
            default_cache_dir().expect("Cannot get cache dir"),
            BioRegistryMetadataProvider::default(),
            OboLibraryProvider::default(),
        ))
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_suite::mocks::MockOntologyRegistry;
    use rstest::rstest;

    #[rstest]
    fn test_build_ontology_success() -> Result<(), FactoryError> {
        let ontology = ResourceRef::new("geno", Some("2025-07-25".to_string()));

        let mut factory = CachedOntologyFactory::new(MockOntologyRegistry::default());
        let mut factory = CachedOntologyFactory::new(MockOntologyRegistry::default());
        let result = factory.build_ontology(&ontology, None)?;

        assert!(Arc::strong_count(&result) >= 1);

        assert!(factory.cache.contains_key(&CacheKey {
            ontology: ontology.clone(),
            file_name: None,
        }));

        Ok(())
    }

    #[rstest]
    fn test_build_bidict() -> Result<(), FactoryError> {
        let ontology = ResourceRef::new("geno", Some("2025-07-25".to_string()));

        let mut factory = CachedOntologyFactory::new(MockOntologyRegistry::default());
        let mut factory = CachedOntologyFactory::new(MockOntologyRegistry::default());
        let result = factory.build_bidict(&ontology, None)?;

        assert!(Arc::strong_count(&result) >= 1);

        assert!(factory.cache.contains_key(&CacheKey {
            ontology: ontology.clone(),
            file_name: None,
        }));

        Ok(())
    }

    #[rstest]
    fn test_build_bidict_other() -> Result<(), FactoryError> {
        let ontology = ResourceRef::from("ro").with_latest();

        let mut factory = CachedOntologyFactory::new(MockOntologyRegistry::default());
        let mut factory = CachedOntologyFactory::new(MockOntologyRegistry::default());
        let result = factory.build_bidict(&ontology, None)?;

        assert!(Arc::strong_count(&result) >= 1);

        assert!(factory.cache.contains_key(&CacheKey {
            ontology: ontology.clone(),
            file_name: None,
        }));

        Ok(())
    }
}
