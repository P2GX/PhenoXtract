#[derive(Clone)]
pub enum GeneQuery<'a> {
    Symbol(&'a str),
    HgncId(&'a str),
}

impl<'a> GeneQuery<'a> {
    pub fn inner(&self) -> &'a str {
        match self {
            GeneQuery::Symbol(s) => s,
            GeneQuery::HgncId(s) => s,
        }
    }
}
