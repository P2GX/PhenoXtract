use ontology_registry::blocking::bio_registry_metadata_provider::BioRegistryMetadataProvider;
use ontology_registry::blocking::file_system_ontology_registry::FileSystemOntologyRegistry;
use ontology_registry::blocking::obolib_ontology_provider::OboLibraryProvider;

pub type OntologyRegistry =
    FileSystemOntologyRegistry<BioRegistryMetadataProvider, OboLibraryProvider>;
