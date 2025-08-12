mod config;
mod extract;
mod load;
mod pipeline;
mod transform;
fn main() {
    use phenopackets::schema::v2::Phenopacket;
    let a = Phenopacket::default();
    println!("Hello, world! ✌️");
}

#[test]
fn test_hallo_world() {
    main()
}
