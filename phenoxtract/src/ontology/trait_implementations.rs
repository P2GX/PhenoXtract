use crate::ontology::traits::{OntologyLike, OntologyTermLike, SynonymLike};
use fastobo::ast::{Ident, OboDoc, Synonym as FastOboSynonym, TermClause, TermFrame};
use ontolius::Identified;
use ontolius::ontology::OntologyTerms;
use ontolius::ontology::csr::FullCsrOntology;
use ontolius::term::simple::SimpleTerm;
use ontolius::term::{MinimalTerm, Synonym as OntoliusSynonym, Synonymous};
use std::sync::Arc;

impl OntologyLike for Arc<FullCsrOntology> {
    fn iter_ontology_terms<'a>(
        &'a self,
        ontology_prefix: String,
    ) -> Box<dyn Iterator<Item = &'a dyn OntologyTermLike> + 'a> {
        Box::new(
            self.iter_terms()
                .map(|t| t as &dyn OntologyTermLike)
                .filter(|t| t.current())
                .filter(move |t| t.prefix().eq_ignore_ascii_case(&ontology_prefix)),
        )
    }
}

impl OntologyTermLike for SimpleTerm {
    fn prefix(&self) -> String {
        self.identifier().prefix().to_string()
    }

    fn ontology_id(&self) -> String {
        self.identifier().to_string()
    }

    fn current(&self) -> bool {
        self.is_current()
    }

    fn label(&self) -> &str {
        self.name()
    }

    fn iter_synonyms<'a>(&'a self) -> Box<dyn Iterator<Item = &'a dyn SynonymLike> + 'a> {
        Box::new(self.synonyms().iter().map(|s| s as &dyn SynonymLike))
    }
}

impl SynonymLike for OntoliusSynonym {
    fn syn_name(&self) -> &str {
        self.name.as_str()
    }
}

impl OntologyLike for OboDoc {
    fn iter_ontology_terms<'a>(
        &'a self,
        ontology_prefix: String,
    ) -> Box<dyn Iterator<Item = &'a dyn OntologyTermLike> + 'a> {
        Box::new(
            self.entities()
                .iter()
                .filter_map(|e| e.as_term())
                .filter(|t| t.current())
                .filter(move |t| t.prefix().eq_ignore_ascii_case(&ontology_prefix))
                .map(|t| t as &dyn OntologyTermLike),
        )
    }
}

impl OntologyTermLike for TermFrame {
    fn prefix(&self) -> String {
        match self.id().as_inner().as_id() {
            Ident::Prefixed(p) => p.prefix().to_string(),
            Ident::Unprefixed(_) => String::new(),
            Ident::Url(u) => u.to_string(),
        }
    }

    fn ontology_id(&self) -> String {
        self.id().to_string().trim().to_string()
    }

    fn current(&self) -> bool {
        self.clauses()
            .iter()
            .all(|line| !matches!(line.as_inner(), TermClause::IsObsolete(true)))
    }

    fn label(&self) -> &str {
        self.name().map(|n| n.as_str()).unwrap_or("")
    }

    fn iter_synonyms<'a>(&'a self) -> Box<dyn Iterator<Item = &'a dyn SynonymLike> + 'a> {
        Box::new(self.clauses().iter().filter_map(|line| {
            if let TermClause::Synonym(syn) = line.as_inner() {
                Some(syn.as_ref() as &dyn SynonymLike)
            } else {
                None
            }
        }))
    }
}

impl SynonymLike for FastOboSynonym {
    fn syn_name(&self) -> &str {
        self.description().as_str()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ontology::CachedOntologyFactory;
    use crate::ontology::resource_references::ResourceRef;
    use crate::test_suite::mocks::MockOntologyRegistry;
    use crate::test_suite::phenopacket_component_generation::default_pato_qual_measurement;
    use ontolius::TermId;
    use rstest::{fixture, rstest};

    #[fixture]
    fn number_of_pato_terms() -> usize {
        1887
    }

    #[fixture]
    fn pato_obodoc() -> OboDoc {
        let ontology = ResourceRef::new("pato", Some("2025-05-14".to_string()));
        let mut factory = CachedOntologyFactory::new(MockOntologyRegistry::default());
        factory.build_obodoc_ontology(&ontology, None).unwrap()
    }

    #[fixture]
    fn pato_ontolius() -> Arc<FullCsrOntology> {
        let ontology = ResourceRef::new("pato", Some("2025-05-14".to_string()));
        let mut factory = CachedOntologyFactory::new(MockOntologyRegistry::default());
        factory.build_ontolius_ontology(&ontology, None).unwrap()
    }

    fn assert_ontology_len(ontology: &impl OntologyLike, expected: usize) {
        assert_eq!(ontology.ontology_len("PATO".to_string()), expected);
    }

    #[rstest]
    fn test_obodoc_ontology_len(pato_obodoc: OboDoc, number_of_pato_terms: usize) {
        assert_ontology_len(&pato_obodoc, number_of_pato_terms);
    }

    #[rstest]
    fn test_ontolius_ontology_len(
        pato_ontolius: Arc<FullCsrOntology>,
        number_of_pato_terms: usize,
    ) {
        assert_ontology_len(&pato_ontolius, number_of_pato_terms);
    }

    fn assert_iter_ontology_terms(ontology: &impl OntologyLike, expected: usize) {
        assert_eq!(
            ontology.iter_ontology_terms("PATO".to_string()).count(),
            expected
        );
    }

    #[rstest]
    fn test_obodoc_iter_ontology_terms(pato_obodoc: OboDoc, number_of_pato_terms: usize) {
        assert_iter_ontology_terms(&pato_obodoc, number_of_pato_terms);
    }

    #[rstest]
    fn test_ontolius_iter_ontology_terms(
        pato_ontolius: Arc<FullCsrOntology>,
        number_of_pato_terms: usize,
    ) {
        assert_iter_ontology_terms(&pato_ontolius, number_of_pato_terms);
    }

    #[fixture]
    fn present_id() -> String {
        default_pato_qual_measurement().id
    }

    #[fixture]
    fn present_from_obodoc(pato_obodoc: OboDoc, present_id: String) -> TermFrame {
        pato_obodoc
            .entities()
            .iter()
            .filter_map(|e| e.as_term())
            .find(|t| t.id().to_string().trim() == present_id)
            .unwrap()
            .clone()
    }

    #[fixture]
    fn present_from_ontolius(
        pato_ontolius: Arc<FullCsrOntology>,
        present_id: String,
    ) -> SimpleTerm {
        let term_id: TermId = present_id.parse().unwrap();
        pato_ontolius.term_by_id(&term_id).unwrap().clone()
    }

    fn assert_prefix(term: &impl OntologyTermLike, expected: &str) {
        assert_eq!(term.prefix(), expected);
    }

    #[rstest]
    fn test_obodoc_prefix(present_from_obodoc: TermFrame) {
        assert_prefix(&present_from_obodoc, "PATO");
    }

    #[rstest]
    fn test_ontolius_prefix(present_from_ontolius: SimpleTerm) {
        assert_prefix(&present_from_ontolius, "PATO");
    }
}
