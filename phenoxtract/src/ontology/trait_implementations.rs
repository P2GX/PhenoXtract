use crate::ontology::ontology_factory::Ontology;
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

impl OntologyLike for Arc<OboDoc> {
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

impl OntologyLike for Ontology {
    fn iter_ontology_terms<'a>(
        &'a self,
        ontology_prefix: String,
    ) -> Box<dyn Iterator<Item = &'a dyn OntologyTermLike> + 'a> {
        match self {
            Ontology::Ontolius(inner) => inner.iter_ontology_terms(ontology_prefix),
            Ontology::OboDoc(inner) => inner.iter_ontology_terms(ontology_prefix),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ontology::CachedOntologyFactory;
    use crate::ontology::resource_references::ResourceRef;
    use crate::test_suite::mocks::MockOntologyRegistry;
    use crate::test_suite::phenopacket_component_generation::default_pato_qual_measurement;
    use fastobo::ast::SynonymScope;
    use ontolius::TermId;
    use rstest::{fixture, rstest};

    #[fixture]
    fn number_of_pato_terms() -> usize {
        1887
    }

    #[fixture]
    fn pato_obodoc() -> Arc<OboDoc> {
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
    fn test_obodoc_ontology_len(pato_obodoc: Arc<OboDoc>, number_of_pato_terms: usize) {
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
    fn test_obodoc_iter_ontology_terms(pato_obodoc: Arc<OboDoc>, number_of_pato_terms: usize) {
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
    fn present_label() -> String {
        default_pato_qual_measurement().label
    }

    #[fixture]
    fn increased_amount_id() -> String {
        "PATO:0000470".to_string()
    }

    #[fixture]
    fn ring_shaped_obsolete_id() -> String {
        "PATO:0040001".to_string()
    }

    fn obodoc_term_from_id(obodoc: Arc<OboDoc>, id: String) -> TermFrame {
        obodoc
            .entities()
            .iter()
            .filter_map(|e| e.as_term())
            .find(|t| t.id().to_string().trim() == id)
            .unwrap()
            .clone()
    }

    fn ontolius_term_from_id(ontolius_ontology: Arc<FullCsrOntology>, id: String) -> SimpleTerm {
        let term_id: TermId = id.parse().unwrap();
        ontolius_ontology.term_by_id(&term_id).unwrap().clone()
    }

    #[fixture]
    fn present_from_obodoc(pato_obodoc: Arc<OboDoc>, present_id: String) -> TermFrame {
        obodoc_term_from_id(pato_obodoc, present_id)
    }

    #[fixture]
    fn present_from_ontolius(
        pato_ontolius: Arc<FullCsrOntology>,
        present_id: String,
    ) -> SimpleTerm {
        ontolius_term_from_id(pato_ontolius, present_id)
    }

    #[fixture]
    fn increased_amount_from_obodoc(
        pato_obodoc: Arc<OboDoc>,
        increased_amount_id: String,
    ) -> TermFrame {
        obodoc_term_from_id(pato_obodoc, increased_amount_id)
    }

    #[fixture]
    fn increased_amount_from_ontolius(
        pato_ontolius: Arc<FullCsrOntology>,
        increased_amount_id: String,
    ) -> SimpleTerm {
        ontolius_term_from_id(pato_ontolius, increased_amount_id)
    }

    #[fixture]
    fn ring_shaped_from_obodoc(
        pato_obodoc: Arc<OboDoc>,
        ring_shaped_obsolete_id: String,
    ) -> TermFrame {
        obodoc_term_from_id(pato_obodoc, ring_shaped_obsolete_id)
    }

    #[fixture]
    fn ring_shaped_from_ontolius(ring_shaped_obsolete_id: String) -> SimpleTerm {
        SimpleTerm::new(
            ring_shaped_obsolete_id.parse().unwrap(),
            "obsolete ring-shaped",
            vec![],
            true,
            None,
            None,
            vec![],
            vec![],
        )
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

    fn assert_ontology_id(term: &impl OntologyTermLike, expected: &str) {
        assert_eq!(term.ontology_id(), expected);
    }

    #[rstest]
    fn test_obodoc_id(present_from_obodoc: TermFrame, present_id: String) {
        assert_ontology_id(&present_from_obodoc, &present_id);
    }

    #[rstest]
    fn test_ontolius_id(present_from_ontolius: SimpleTerm, present_id: String) {
        assert_ontology_id(&present_from_ontolius, &present_id);
    }

    fn assert_current(term: &impl OntologyTermLike, expected: bool) {
        assert_eq!(term.current(), expected);
    }

    #[rstest]
    fn test_obodoc_current(present_from_obodoc: TermFrame) {
        assert_current(&present_from_obodoc, true);
    }

    #[rstest]
    fn test_ontolius_current(present_from_ontolius: SimpleTerm) {
        assert_current(&present_from_ontolius, true);
    }

    #[rstest]
    fn test_obodoc_obsolete(ring_shaped_from_obodoc: TermFrame) {
        assert_current(&ring_shaped_from_obodoc, false);
    }

    #[rstest]
    fn test_ontolius_obsolete(ring_shaped_from_ontolius: SimpleTerm) {
        assert_current(&ring_shaped_from_ontolius, false);
    }

    fn assert_label(term: &impl OntologyTermLike, expected: &str) {
        assert_eq!(term.label(), expected);
    }

    #[rstest]
    fn test_obodoc_label(present_from_obodoc: TermFrame, present_label: String) {
        assert_label(&present_from_obodoc, &present_label);
    }

    #[rstest]
    fn test_ontolius_label(present_from_ontolius: SimpleTerm, present_label: String) {
        assert_label(&present_from_ontolius, &present_label);
    }

    fn assert_syn_number(term: &impl OntologyTermLike, expected_no_syns: usize) {
        assert_eq!(term.iter_synonyms().count(), expected_no_syns);
    }

    fn assert_syn_name(syn: &dyn SynonymLike, expected: &str) {
        assert_eq!(syn.syn_name(), expected);
    }

    #[rstest]
    fn test_obodoc_iter_synonyms_length(increased_amount_from_obodoc: TermFrame) {
        assert_syn_number(&increased_amount_from_obodoc, 5);
    }

    #[rstest]
    fn test_ontolius_iter_synonyms_length(increased_amount_from_ontolius: SimpleTerm) {
        assert_syn_number(&increased_amount_from_ontolius, 5);
    }

    #[rstest]
    fn test_obodoc_iter_synonyms_names(present_from_obodoc: TermFrame) {
        let present_in_organism_syn = present_from_obodoc.iter_synonyms().next().unwrap();
        assert_syn_name(present_in_organism_syn, "present in organism");
    }

    #[rstest]
    fn test_ontolius_iter_synonyms_names(present_from_ontolius: SimpleTerm) {
        let present_in_organism_syn = present_from_ontolius.iter_synonyms().next().unwrap();
        assert_syn_name(present_in_organism_syn, "present in organism");
    }

    #[rstest]
    fn test_obodoc_syn_name() {
        let present_in_organism_syn =
            FastOboSynonym::new("present in organism", SynonymScope::Related);
        assert_syn_name(&present_in_organism_syn, "present in organism");
    }

    #[rstest]
    fn test_ontolius_syn_name(present_from_ontolius: SimpleTerm) {
        let present_in_organism_syn = present_from_ontolius.synonyms().first().unwrap();
        assert_syn_name(present_in_organism_syn, "present in organism");
    }
}
