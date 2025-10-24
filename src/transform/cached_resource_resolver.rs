use crate::ontology::BioRegistryClient;
use crate::ontology::traits::{HasPrefixId, HasVersion};
use log::{debug, warn};
use phenopackets::schema::v2::core::Resource;
use std::collections::HashMap;

/// A cached resolver for biological ontology resources.
///
/// This resolver fetches resource metadata from the BioRegistry API and caches
/// the results to avoid repeated network requests. It maintains a cache of resolved
/// resources and allows specifying known versions for resources before resolution.
#[derive(Clone, Default, Debug)]
#[allow(dead_code)]
pub struct CachedResourceResolver {
    cache: HashMap<String, Resource>,
    bio_reg_client: BioRegistryClient,
}

impl CachedResourceResolver {
    /// Resolves a resource by its ID, returning cached data if available or fetching
    /// from BioRegistry if not.
    ///
    /// The resolution process:
    /// 1. Checks the cache for an existing resource
    /// 2. If not cached, fetches from BioRegistry API
    /// 3. Prioritizes known versions over BioRegistry versions
    /// 4. Selects the first available download format (JSON, OWL, OBO, RDF) or homepage
    /// 5. Caches successful resolutions for future use
    ///
    /// # Arguments
    ///
    /// * `id` - The resource identifier (case-insensitive)
    ///
    /// # Returns
    ///
    /// * `Some(Resource)` if the resource was successfully resolved with all required fields
    /// * `None` if the resource couldn't be found or is missing required fields
    #[allow(dead_code)]
    pub fn resolve(&mut self, resource_ref: &(impl HasPrefixId + HasVersion)) -> Option<Resource> {
        let prefix_id = resource_ref.prefix_id().to_lowercase();
        debug!("Resolve id: {}", prefix_id);
        self.cache.get(&prefix_id).cloned().or_else(|| {
            debug!("Cache not hit");
            let response = self.bio_reg_client.get_resource(&prefix_id);

            response.ok().and_then(|bio_reg_resource| {
                let resolved_version: Option<String> = match resource_ref.version() {
                    "latest" => bio_reg_resource.version.unwrap_or("-".to_string()).into(),
                    version => version.to_string().into(),
                };
                let resolved_url = bio_reg_resource
                    .download_json
                    .or(bio_reg_resource.download_owl)
                    .or(bio_reg_resource.download_obo)
                    .or(bio_reg_resource.download_rdf)
                    .or(bio_reg_resource.homepage);

                CachedResourceResolver::log_missing_fields(
                    &prefix_id,
                    &resolved_version,
                    &resolved_url,
                    &bio_reg_resource.name,
                    &bio_reg_resource.preferred_prefix,
                    &bio_reg_resource.uri_format,
                );

                let resource = Resource {
                    id: bio_reg_resource.prefix,
                    name: bio_reg_resource.name?,
                    url: resolved_url?,
                    version: resolved_version?,
                    namespace_prefix: bio_reg_resource.preferred_prefix?,
                    iri_prefix: bio_reg_resource.uri_format?,
                };
                debug!("Cached resource: {}", prefix_id);
                self.cache.insert(prefix_id, resource.clone());

                Some(resource)
            })
        })
    }

    fn log_missing_fields(
        id: &str,
        version: &Option<String>,
        url: &Option<String>,
        name: &Option<String>,
        preferred_prefix: &Option<String>,
        uri_format: &Option<String>,
    ) {
        let mut missing_fields: Vec<&str> = Vec::new();

        if version.is_none() {
            missing_fields.push("version");
        }
        if url.is_none() {
            missing_fields.push("url");
        }
        if name.is_none() {
            missing_fields.push("name");
        }
        if preferred_prefix.is_none() {
            missing_fields.push("namespace_prefix");
        }
        if uri_format.is_none() {
            missing_fields.push("iri_prefix");
        }

        if !missing_fields.is_empty() {
            warn!(
                "Could construct resource for resource id {id}. Missing fields {:?}",
                missing_fields
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::ontology::resource_references::ResourceRef;
    use crate::ontology::traits::HasPrefixId;
    use crate::transform::cached_resource_resolver::CachedResourceResolver;
    use rstest::rstest;

    #[rstest]
    fn test_resolve() {
        let resource_id = ResourceRef::new("hp".to_string(), "".to_string());
        let mut resolver = CachedResourceResolver::default();
        let hpo_metadata = resolver.resolve(&resource_id).unwrap();

        assert_eq!(hpo_metadata.id, resource_id.prefix_id());
        assert_eq!(hpo_metadata.name, "Human Phenotype Ontology");
        assert_eq!(hpo_metadata.url, "http://purl.obolibrary.org/obo/hp.json");
        assert_eq!(hpo_metadata.namespace_prefix, "HP");
        assert_eq!(
            hpo_metadata.iri_prefix,
            "http://purl.obolibrary.org/obo/HP_$1"
        );
    }

    #[rstest]
    fn test_resolve_versionless_resource() {
        let mut resolver = CachedResourceResolver::default();
        let resource_id = ResourceRef::new("hgnc".to_string(), "".to_string());
        let hgnc_metadata = resolver.resolve(&resource_id).unwrap();

        assert_eq!(hgnc_metadata.id, "hgnc");
        assert_eq!(hgnc_metadata.version, "-");
    }
}
