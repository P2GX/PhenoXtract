use crate::ontology::error::BiDictError;
use crate::ontology::ontology_factory::Ontology;
use crate::ontology::resource_references::ResourceRef;
use enum_dispatch::enum_dispatch;
use fastobo::ast::OboDoc;
use ontolius::ontology::csr::FullCsrOntology;
use std::fmt::Debug;
use std::sync::Arc;

pub trait HasPrefixId {
    fn prefix_id(&self) -> &str;
}

pub trait HasVersion {
    fn version(&self) -> &str;
}

pub trait BiDict: Send + Sync + Debug {
    fn get(&self, id_or_label: &str) -> Result<&str, BiDictError>;
    fn get_label(&self, id: &str) -> Result<&str, BiDictError>;
    fn get_id(&self, term: &str) -> Result<&str, BiDictError>;

    fn reference(&self) -> &ResourceRef;
}

pub trait OntologyTermLike {
    fn prefix(&self) -> String;
    fn ontology_id(&self) -> String;
    fn current(&self) -> bool;
    fn label(&self) -> &str;
    fn iter_synonyms<'a>(&'a self) -> Box<dyn Iterator<Item = &'a dyn SynonymLike> + 'a>;
}

pub trait SynonymLike {
    fn syn_name(&self) -> &str;
}

#[enum_dispatch]
pub trait OntologyLike {
    /// The amount of CURRENT terms with the specified prefix.
    fn ontology_len(&self, ontology_prefix: String) -> usize {
        self.iter_ontology_terms(ontology_prefix).count()
    }

    /// Should iterate over the CURRENT terms of the ontology, and only those with the specified prefix.
    fn iter_ontology_terms<'a>(
        &'a self,
        ontology_prefix: String,
    ) -> Box<dyn Iterator<Item = &'a dyn OntologyTermLike> + 'a>;
}
