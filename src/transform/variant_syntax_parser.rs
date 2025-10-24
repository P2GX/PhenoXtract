use crate::transform::error::PhenopacketBuilderError;
use hgvs;
use hgvs::parser::HgvsVariant;
use std::str::FromStr;

#[allow(unused)]
#[derive(Debug, Default)]
pub struct VariantParser;

impl VariantParser {
    /// this function will try to parse var_string as a variant in different formats (currently only HGVS accepted)
    /// and then return the appropriate variant syntax
    /// if all attempts to parse var_string fail, then an error will be thrown
    #[allow(unused)]
    pub fn try_parse_syntax(var_string: &str) -> Result<&str, PhenopacketBuilderError> {
        let hgvs = HgvsVariant::from_str(var_string).map_err(|e| {
            PhenopacketBuilderError::ParsingError {
                what: "HGVS Variant".to_string(),
                value: "var_string".to_string(),
            }
        })?;

        match hgvs {
            HgvsVariant::CdsVariant { .. } => Ok("hgvs.c"),
            HgvsVariant::GenomeVariant { .. } => Ok("hgvs.g"),
            HgvsVariant::MtVariant { .. } => Ok("hgvs.m"),
            HgvsVariant::TxVariant { .. } => Ok("hgvs.n"),
            HgvsVariant::ProtVariant { .. } => Ok("hgvs.p"),
            HgvsVariant::RnaVariant { .. } => Ok("hgvs.r"),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::transform::variant_syntax_parser::VariantParser;
    use rstest::rstest;

    #[rstest]
    fn test_get_syntax_from_str() {
        //coding SNP
        assert_eq!(
            VariantParser::try_parse_syntax("NM_004006.2:c.4375C>T").unwrap(),
            "hgvs.c"
        );
        //coding dup
        assert_eq!(
            VariantParser::try_parse_syntax("NM_000138.4:c.7642_7644dup").unwrap(),
            "hgvs.c"
        );
        //coding del
        assert_eq!(
            VariantParser::try_parse_syntax("NM_000501.3:c.1949del").unwrap(),
            "hgvs.c"
        );
        //coding del explicit
        assert_eq!(
            VariantParser::try_parse_syntax("NM_001110792.2:c.844delC").unwrap(),
            "hgvs.c"
        );
        //coding del multi
        assert_eq!(
            VariantParser::try_parse_syntax("NM_015120.4:c.6960_6963delACAG").unwrap(),
            "hgvs.c"
        );
        //genomic SNP
        assert_eq!(
            VariantParser::try_parse_syntax("NC_000023.11:g.32389644G>A").unwrap(),
            "hgvs.g"
        );
        //mitochondrial SNP
        assert_eq!(
            VariantParser::try_parse_syntax("NM_00333.2:m.789C>T").unwrap(),
            "hgvs.m"
        );
        //non-coding SNP
        assert_eq!(
            VariantParser::try_parse_syntax("NM_00444.2:n.1011C>T").unwrap(),
            "hgvs.n"
        );
        //amino acid substitution
        assert_eq!(
            VariantParser::try_parse_syntax("NP_003997.1:p.Trp24Cys").unwrap(),
            "hgvs.p"
        );
        //amino acid deletion
        assert_eq!(
            VariantParser::try_parse_syntax("NP_00555.2:p.Leu1213del").unwrap(),
            "hgvs.p"
        );
        //RNA SNP
        assert_eq!(
            VariantParser::try_parse_syntax("NM_00666.2:r.1416C>T").unwrap(),
            "hgvs.r"
        );

        //invalid accession is allowed
        VariantParser::try_parse_syntax("abdef:g.32389644G>A").unwrap();
        //no checks are made against the stated reference sequence
        VariantParser::try_parse_syntax("NM_004006.2:c.4375A>T").unwrap();
        VariantParser::try_parse_syntax("NM_004006.2:c.4375C>T").unwrap();
        VariantParser::try_parse_syntax("NM_004006.2:c.4375G>T").unwrap();
        //meaningless mutations are allowed
        VariantParser::try_parse_syntax("NM_004006.2:c.4375T>T").unwrap();
    }

    #[rstest]
    fn test_get_syntax_from_str_fail() {
        //null
        assert!(VariantParser::try_parse_syntax("").is_err());
        //invalid formatting
        assert!(VariantParser::try_parse_syntax("NM_004006.2#c.4375C>T").is_err());
        //invalid type
        assert!(VariantParser::try_parse_syntax("NM_004006.2:q.4375C>T").is_err());
        //invalid variant notation
        assert!(VariantParser::try_parse_syntax("NM_004006.2:c.4375C-T").is_err());
        //invalid negative genomic location
        assert!(VariantParser::try_parse_syntax("NM_004006.2:g.-4375C>T").is_err());
        //no location
        assert!(VariantParser::try_parse_syntax("NM_004006.2:c.C>T").is_err());
    }
}
