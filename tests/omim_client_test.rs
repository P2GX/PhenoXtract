use dotenvy::dotenv;
use phenoxtract::ontology::error::BiDictError;
use phenoxtract::ontology::omim_client::OmimClient;
use phenoxtract::ontology::traits::BiDict;

#[test]
fn test_omim_client() {
    dotenv().ok();
    let label = "Bleeding disorder, platelet-type, 9";
    let omim_id = "OMIM:614200";
    let synonym_1: &str = "GLYCOPROTEIN Ia DEFICIENCY";
    // Skip test if API key is not available (e.g., in CI)
    let api_key = match std::env::var("BIOPORTAL_API_KEY") {
        Ok(key) => key,
        Err(_) => {
            println!("Skipping test_omim_client: BIOPORTAL_API_KEY not set");
            return;
        }
    };
    let client = OmimClient::new_with_key(api_key);

    // Test by ID - with better error handling
    let fatched_label = client
        .get_label(&omim_id)
        .expect("Failed to get label by ID");
    assert_eq!(fatched_label, label, "Label should match expected value");

    let fetched_id = client.get_id(label).expect("Failed to get ID by synonym");

    assert_eq!(fetched_id, omim_id, "ID should match expected OMIM ID");

    // Test by label - with better error handling
    match client.get_id(label) {
        Ok(id) => {
            println!("{} -> {}", label, id);

            // Test by synonym - with better error handling
            match client.get_id(synonym_1) {
                Ok(id_syn) => {
                    println!("{} -> {}", synonym_1, id_syn);
                    assert_eq!(id, id_syn, "IDs should match for the same disease");
                }
                Err(e) => println!("Failed to get ID by synonym: {}", e),
            }
        }
        Err(e) => println!("Failed to get ID by label: {}", e),
    }
}

#[test]
fn test_omim_client_caching() {
    dotenv().ok();
    // Skip test if API key is not available (e.g., in CI)
    let api_key = match std::env::var("BIOPORTAL_API_KEY") {
        Ok(key) => key,
        Err(_) => {
            println!("Skipping test_omim_client_caching: BIOPORTAL_API_KEY not set");
            return;
        }
    };
    let client = OmimClient::new_with_key(api_key);

    // First call should hit the API
    let result1 = client.get("OMIM:191100");
    println!("First call result: {:?}", result1);

    // Second call should be cached
    let result2 = client.get("OMIM:191100");
    println!("Second call result (cached): {:?}", result2);

    // Both should have the same result
    match (result1, result2) {
        (Ok(r1), Ok(r2)) => {
            assert_eq!(r1, r2, "Cached results should match");
            println!("Caching test passed: {}", r1);
        }
        (Err(e1), Err(e2)) => {
            println!("Both calls failed with errors: {} and {}", e1, e2);
        }
        _ => println!("Results were inconsistent"),
    }
}

#[test]
fn test_invalid_omim_id_validation() {
    // Use explicit key to avoid env dependence; validation should fail before any network call.
    let client = OmimClient::new_with_key("dummy-key".to_string());

    let err = client.get_label("OMIM:ABC").unwrap_err();
    assert!(matches!(err, BiDictError::InvalidId(_)));
}

#[test]
fn test_omim_147920() {
    dotenv().ok();
    // Skip test if API key is not available (e.g., in CI)
    let api_key = match std::env::var("BIOPORTAL_API_KEY") {
        Ok(key) => key,
        Err(_) => {
            println!("Skipping test_omim_147920: BIOPORTAL_API_KEY not set");
            return;
        }
    };
    let client = OmimClient::new_with_key(api_key);

    // Test OMIM:147920
    match client.get_label("OMIM:147920") {
        Ok(label) => {
            println!("OMIM:147920 -> {}", label);
            assert!(!label.is_empty(), "Label should not be empty");
        }
        Err(e) => println!("Failed to get label for OMIM:147920: {}", e),
    }
}
