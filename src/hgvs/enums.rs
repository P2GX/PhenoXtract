#![allow(clippy::upper_case_acronyms)]
#[derive(Debug)]
pub enum ChromosomalSex {
    X,
    XX,
    XXX,
    XXY,
    XYY,
    XY,
    Unknown,
}

#[derive(Debug)]
pub enum AlleleCount {
    Single,
    Double,
}