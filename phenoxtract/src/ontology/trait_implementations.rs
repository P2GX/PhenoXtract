use crate::ontology::traits::{OntologyLike, OntologyTermLike, SynonymLike};
use fastobo::ast::{Ident, OboDoc, Synonym as FastOboSynonym, TermClause, TermFrame};
use ontolius::Identified;
use ontolius::ontology::OntologyTerms;
use ontolius::ontology::csr::FullCsrOntology;
use ontolius::term::simple::SimpleTerm;
use ontolius::term::{MinimalTerm, Synonym as OntoliusSynonym, Synonymous};
use std::sync::Arc;

impl OntologyLike for Arc<FullCsrOntology> {
    fn ontology_len(&self) -> usize {
        self.len()
    }

    fn iter_ontology_terms<'a>(
        &'a self,
    ) -> Box<dyn Iterator<Item = &'a dyn OntologyTermLike> + 'a> {
        Box::new(self.iter_terms().map(|t| t as &dyn OntologyTermLike))
    }
}

impl OntologyTermLike for SimpleTerm {
    fn prefix(&self) -> String {
        self.identifier().prefix().to_string().to_lowercase()
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
    fn ontology_len(&self) -> usize {
        self.entities().len()
    }

    fn iter_ontology_terms<'a>(
        &'a self,
    ) -> Box<dyn Iterator<Item = &'a dyn OntologyTermLike> + 'a> {
        Box::new(
            self.entities()
                .iter()
                .filter_map(|e| e.as_term())
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
        self.id().to_string()
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
