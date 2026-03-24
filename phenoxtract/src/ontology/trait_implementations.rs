use crate::ontology::traits::{OntologyLike, OntologyTermLike, SynonymLike};
use fastobo::ast::{Ident, OboDoc, Synonym as FastOboSynonym, TermClause, TermFrame};
use ontolius::Identified;
use ontolius::ontology::OntologyTerms;
use ontolius::ontology::csr::FullCsrOntology;
use ontolius::term::simple::SimpleTerm;
use ontolius::term::{MinimalTerm, Synonym as OntoliusSynonym, Synonymous};
use std::sync::Arc;

impl OntologyLike for FullCsrOntology {
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

impl OntologyLike for Arc<FullCsrOntology> {
    fn iter_ontology_terms<'a>(
        &'a self,
        ontology_prefix: String,
    ) -> Box<dyn Iterator<Item = &'a dyn OntologyTermLike> + 'a> {
        self.as_ref().iter_ontology_terms(ontology_prefix)
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

impl OntologyLike for Arc<OboDoc> {
    fn iter_ontology_terms<'a>(
        &'a self,
        ontology_prefix: String,
    ) -> Box<dyn Iterator<Item = &'a dyn OntologyTermLike> + 'a> {
        self.as_ref().iter_ontology_terms(ontology_prefix)
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
    use crate::ontology::traits::{HasPrefixId, HasVersion};
    use crate::test_suite::mocks::MockOntologyRegistry;
    use crate::test_suite::phenopacket_component_generation::default_unit_oc;
    use crate::test_suite::resource_references::UO_REF;
    use fastobo::ast::SynonymScope;
    use ontolius::TermId;
    use ontolius::io::OntologyLoaderBuilder;
    use ontolius::term::Definition;
    use ontology_registry::{FileType, OntologyRegistration, Version};
    use rstest::{fixture, rstest};
    use std::io::BufReader;

    fn number_of_uo_terms() -> usize {
        573
    }

    fn uo_obodoc() -> Arc<OboDoc> {
        let registry = MockOntologyRegistry::default();
        let ontology_path = registry
            .register(
                UO_REF.prefix_id().to_lowercase(),
                Version::Declared(UO_REF.version().to_string()),
                FileType::Obo,
            )
            .unwrap();
        let mut reader = BufReader::new(ontology_path);
        Arc::new(fastobo::from_reader(&mut reader).unwrap())
    }

    fn uo_ontolius() -> Arc<FullCsrOntology> {
        let registry = MockOntologyRegistry::default();
        let ontology_path = registry
            .register(
                UO_REF.prefix_id().to_lowercase(),
                Version::Declared(UO_REF.version().to_string()),
                FileType::Json,
            )
            .unwrap();
        let loader = OntologyLoaderBuilder::new().obographs_parser().build();
        let ontolius = loader.load_from_read(ontology_path).unwrap();
        Arc::new(ontolius)
    }

    fn assert_ontology_len(ontology: &impl OntologyLike, expected: usize) {
        assert_eq!(ontology.ontology_len("UO".to_string()), expected);
    }

    #[rstest]
    fn test_obodoc_ontology_len() {
        assert_ontology_len(&uo_obodoc(), number_of_uo_terms());
    }

    #[rstest]
    fn test_ontolius_ontology_len() {
        assert_ontology_len(&uo_ontolius(), number_of_uo_terms());
    }

    fn assert_iter_ontology_terms(ontology: &impl OntologyLike, expected: usize) {
        assert_eq!(
            ontology.iter_ontology_terms("UO".to_string()).count(),
            expected
        );
    }

    #[rstest]
    fn test_obodoc_iter_ontology_terms() {
        assert_iter_ontology_terms(&uo_obodoc(), number_of_uo_terms());
    }

    #[rstest]
    fn test_ontolius_iter_ontology_terms() {
        assert_iter_ontology_terms(&uo_ontolius(), number_of_uo_terms());
    }

    fn centimeter_id() -> String {
        default_unit_oc().id
    }

    fn centimeter_label() -> String {
        default_unit_oc().label
    }

    fn micromole_obsolete_id() -> String {
        "UO:0010048".to_string()
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
    fn centimeter_obodoc() -> TermFrame {
        obodoc_term_from_id(uo_obodoc(), centimeter_id())
    }

    #[fixture]
    fn centimeter_ontolius() -> SimpleTerm {
        ontolius_term_from_id(uo_ontolius(), centimeter_id())
    }

    #[fixture]
    fn micromole_obodoc() -> TermFrame {
        obodoc_term_from_id(uo_obodoc(), micromole_obsolete_id())
    }

    #[fixture]
    fn micromole_ontolius() -> SimpleTerm {
        SimpleTerm::new(
            micromole_obsolete_id().parse().unwrap(),
            "micromole",
            vec![],
            true,
            Some(Definition { val: "DEPRECATED: Duplicate of http://purl.obolibrary.org/obo/UO_0000039. A substance unit which is equal to one millionth of a mole.".to_string(), xrefs: vec!["UOB:LTS".to_string()] }),
            None,
            vec![],
            vec![],
        )
    }

    fn assert_prefix(term: &impl OntologyTermLike, expected: &str) {
        assert_eq!(term.prefix(), expected);
    }

    #[rstest]
    fn test_obodoc_prefix() {
        assert_prefix(&centimeter_obodoc(), "UO");
    }

    #[rstest]
    fn test_ontolius_prefix() {
        assert_prefix(&centimeter_ontolius(), "UO");
    }

    fn assert_ontology_id(term: &impl OntologyTermLike, expected: &str) {
        assert_eq!(term.ontology_id(), expected);
    }

    #[rstest]
    fn test_obodoc_id() {
        assert_ontology_id(&centimeter_obodoc(), &centimeter_id());
    }

    #[rstest]
    fn test_ontolius_id() {
        assert_ontology_id(&centimeter_ontolius(), &centimeter_id());
    }

    fn assert_current(term: &impl OntologyTermLike, expected: bool) {
        assert_eq!(term.current(), expected);
    }

    #[rstest]
    fn test_obodoc_current() {
        assert_current(&centimeter_obodoc(), true);
    }

    #[rstest]
    fn test_ontolius_current() {
        assert_current(&centimeter_ontolius(), true);
    }

    #[rstest]
    fn test_obodoc_obsolete() {
        assert_current(&micromole_obodoc(), false);
    }

    #[rstest]
    fn test_ontolius_obsolete() {
        assert_current(&micromole_ontolius(), false);
    }

    fn assert_label(term: &impl OntologyTermLike, expected: &str) {
        assert_eq!(term.label(), expected);
    }

    #[rstest]
    fn test_obodoc_label() {
        assert_label(&centimeter_obodoc(), &centimeter_label());
    }

    #[rstest]
    fn test_ontolius_label() {
        assert_label(&centimeter_ontolius(), &centimeter_label());
    }

    fn assert_syns(term: &impl OntologyTermLike, expected_syn_names: Vec<&str>) {
        assert_eq!(
            term.iter_synonyms()
                .map(|s| s.syn_name())
                .collect::<Vec<&str>>(),
            expected_syn_names
        );
    }

    #[rstest]
    fn test_obodoc_iter_synonyms() {
        assert_syns(&centimeter_obodoc(), vec!["centimetre", "cm"])
    }

    #[rstest]
    fn test_ontolius_iter_synonyms() {
        assert_syns(&centimeter_ontolius(), vec!["centimetre", "cm"])
    }

    fn assert_syn_name(syn: &dyn SynonymLike, expected: &str) {
        assert_eq!(syn.syn_name(), expected);
    }

    #[rstest]
    fn test_obodoc_syn_name() {
        let centimetre_syn = FastOboSynonym::new("centimetre", SynonymScope::Exact);
        assert_syn_name(&centimetre_syn, "centimetre");
    }

    #[rstest]
    fn test_ontolius_syn_name() {
        let centimetre_syn = centimeter_ontolius().synonyms().first().unwrap().clone();
        assert_syn_name(&centimetre_syn, "centimetre");
    }
}
