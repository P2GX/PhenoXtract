/// An enum which carries gene and variant information.
/// The collector should collect gene and variant data which can be interpreted
/// as one of the enum's branches.
#[derive(PartialEq, Debug)]
pub(crate) enum PathogenicGeneVariantData {
    None,
    CausativeGene(String),
    HeterozygousVariant {
        gene: Option<String>,
        var: String,
    },
    HomozygousVariant {
        gene: Option<String>,
        var: String,
    },
    CompoundHeterozygousVariantPair {
        gene: Option<String>,
        var1: String,
        var2: String,
    },
}

impl PathogenicGeneVariantData {
    /// Constructs a [`PathogenicGeneVariantData`] value from lists of genes and variants.
    ///
    /// # Valid Configurations
    ///
    /// - **No genes or variants** → `None`
    /// - **A single gene with no variants** → `CausativeGene`
    /// - **A single heterozygous variant, possibly alongside a gene** → `HeterozygousVariant`
    /// - **A pair of identical variants, possibly alongside a gene** → `HomozygousVariant`
    /// - **A pair of distinct, heterozygous variants, possibly alongside a gene** → `CompoundHeterozygousVariantPair`
    ///
    /// All other configurations are considered **invalid**.
    ///
    /// # Errors
    ///
    /// Returns an `Err` containing a descriptive message if the provided genes and variants
    /// do not match any valid configuration.
    pub fn from_genes_and_variants(
        genes: Vec<&str>,
        variants: Vec<&str>,
    ) -> Result<PathogenicGeneVariantData, String> {
        match (genes.len(), variants.len()) {
            (0, 0) => Ok(PathogenicGeneVariantData::None),
            (1, 0) => Ok(PathogenicGeneVariantData::CausativeGene(
                genes[0].to_string(),
            )),
            (0, 1) | (1, 1) => Ok(PathogenicGeneVariantData::HeterozygousVariant {
                gene: genes.first().map(|s| s.to_string()),
                var: variants[0].to_string(),
            }),
            (0, 2) | (1, 2) => {
                if variants[0] == variants[1] {
                    Ok(PathogenicGeneVariantData::HomozygousVariant {
                        gene: genes.first().map(|s| s.to_string()),
                        var: variants[0].to_string(),
                    })
                } else {
                    Ok(PathogenicGeneVariantData::CompoundHeterozygousVariantPair {
                        gene: genes.first().map(|s| s.to_string()),
                        var1: variants[0].to_string(),
                        var2: variants[1].to_string(),
                    })
                }
            }
            _ => Err(format!(
                "Invalid quantity of genes {} and variants {}. Could not interpret as PathogenicGeneVariantData.",
                genes.len(),
                variants.len()
            )),
        }
    }

    pub fn get_allelic_count(&self) -> usize {
        match self {
            PathogenicGeneVariantData::None => 0,
            PathogenicGeneVariantData::CausativeGene(_) => 0,
            PathogenicGeneVariantData::HeterozygousVariant { .. } => 1,
            PathogenicGeneVariantData::HomozygousVariant { .. } => 2,
            PathogenicGeneVariantData::CompoundHeterozygousVariantPair { .. } => 1,
        }
    }

    pub fn get_gene(&self) -> Option<&str> {
        match self {
            PathogenicGeneVariantData::None => None,
            PathogenicGeneVariantData::CausativeGene(gene) => Some(gene),
            PathogenicGeneVariantData::HeterozygousVariant { gene, .. }
            | PathogenicGeneVariantData::HomozygousVariant { gene, .. }
            | PathogenicGeneVariantData::CompoundHeterozygousVariantPair { gene, .. } => {
                gene.as_deref()
            }
        }
    }

    pub fn get_vars(&self) -> Vec<&str> {
        match self {
            PathogenicGeneVariantData::None | PathogenicGeneVariantData::CausativeGene(_) => vec![],
            PathogenicGeneVariantData::HomozygousVariant { var, .. }
            | PathogenicGeneVariantData::HeterozygousVariant { var, .. } => vec![var],
            PathogenicGeneVariantData::CompoundHeterozygousVariantPair { var1, var2, .. } => {
                vec![var1, var2]
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::transform::pathogenic_gene_variant_info::PathogenicGeneVariantData;
    use rstest::rstest;

    #[rstest]
    fn test_from_genes_and_variants() {
        assert!(matches!(
            PathogenicGeneVariantData::from_genes_and_variants(vec![], vec![]).unwrap(),
            PathogenicGeneVariantData::None
        ));
        assert!(matches!(
            PathogenicGeneVariantData::from_genes_and_variants(vec!["KIF21A"], vec![]).unwrap(),
            PathogenicGeneVariantData::CausativeGene(_)
        ));
        assert!(matches!(
            PathogenicGeneVariantData::from_genes_and_variants(
                vec!["KIF21A"],
                vec!["NM_001173464.1:c.2860C>T"]
            )
            .unwrap(),
            PathogenicGeneVariantData::HeterozygousVariant { .. }
        ));
        assert!(matches!(
            PathogenicGeneVariantData::from_genes_and_variants(
                vec!["KIF21A"],
                vec!["NM_001173464.1:c.2860C>T", "NM_001173464.1:c.2860C>T"]
            )
            .unwrap(),
            PathogenicGeneVariantData::HomozygousVariant { .. }
        ));
        assert!(matches!(
            PathogenicGeneVariantData::from_genes_and_variants(
                vec![],
                vec!["NM_001173464.1:c.2860C>T", "NM_015120.4:c.11031_11032delGA"]
            )
            .unwrap(),
            PathogenicGeneVariantData::CompoundHeterozygousVariantPair { .. }
        ));
    }

    #[rstest]
    fn test_from_genes_and_variants_invalid_configuration() {
        // multiple genes
        assert!(
            PathogenicGeneVariantData::from_genes_and_variants(
                vec!["KIF21A", "CLOCK"],
                vec!["NM_001173464.1:c.2860C>T"]
            )
            .is_err()
        );
        // too many variants
        assert!(
            PathogenicGeneVariantData::from_genes_and_variants(
                vec!["KIF21A"],
                vec![
                    "NM_001173464.1:c.2860C>T",
                    "NM_001173464.1:c.2860C>T",
                    "NM_001173464.1:c.2860C>T"
                ]
            )
            .is_err()
        );
    }
}
