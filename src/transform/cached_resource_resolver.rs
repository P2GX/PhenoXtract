use crate::ontology::BioRegistryClient;
use log::warn;
use phenopackets::schema::v2::core::Resource;
use std::collections::HashMap;

#[derive(Clone, Default, Debug)]
#[allow(dead_code)]
struct CachedResourceResolver {
    cache: HashMap<String, Resource>,
    known_versions: HashMap<String, String>,
    bio_reg_client: BioRegistryClient,
}

impl CachedResourceResolver {
    #[allow(dead_code)]
    pub fn new(
        cache: HashMap<String, Resource>,
        known_versions: HashMap<String, String>,
        bio_reg_client: BioRegistryClient,
    ) -> CachedResourceResolver {
        Self {
            cache,
            known_versions,
            bio_reg_client,
        }
    }

    #[allow(dead_code)]
    pub fn resolve(&mut self, id: &str) -> Option<Resource> {
        let id = id.to_lowercase();

        self.cache.get(&id).cloned().or_else(|| {
            let response = self.bio_reg_client.get_resource(&id);

            response.ok().and_then(|bio_reg_resource| {
                let resolved_version = self
                    .known_versions
                    .get(&id)
                    .cloned()
                    .or(bio_reg_resource.version)
                    .or(Some("-".to_string()));

                let resolved_url = bio_reg_resource
                    .download_json
                    .or(bio_reg_resource.download_owl)
                    .or(bio_reg_resource.download_obo)
                    .or(bio_reg_resource.download_rdf)
                    .or(bio_reg_resource.homepage);

                CachedResourceResolver::log_missing_fields(
                    &id,
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

                self.cache.insert(id, resource.clone());
                Some(resource)
            })
        })
    }

    #[allow(dead_code)]
    pub fn add_known_version(&mut self, id: &str, version: &str) {
        self.known_versions
            .insert(id.to_string(), version.to_string());
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
    use crate::transform::cached_resource_resolver::CachedResourceResolver;
    use rstest::rstest;

    #[rstest]
    fn test_resolve() {
        let resource_id = "hp";
        let mut f = CachedResourceResolver::default();
        let hpo_metadata = f.resolve(resource_id).unwrap();

        assert_eq!(hpo_metadata.id, resource_id);
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
        let mut f = CachedResourceResolver::default();

        let hgnc_metadata = f.resolve("hgnc").unwrap();

        assert_eq!(hgnc_metadata.id, "hgnc");
        assert_eq!(hgnc_metadata.version, "-");
    }

    #[rstest]
    fn test_resolve_known_version() {
        let know_version = "1.2.3.4";
        let mut f = CachedResourceResolver::default();
        f.add_known_version("hgnc", know_version);
        let hgnc_metadata = f.resolve("hgnc").unwrap();

        assert_eq!(hgnc_metadata.id, "hgnc");
        assert_eq!(hgnc_metadata.version, know_version);
    }
}
