use lightning_db::utils::integrity::calculate_checksum;

#[test]
fn test_simple_checksum() {
    let empty_data = &[];
    let empty_checksum = calculate_checksum(empty_data);
    assert_eq!(empty_checksum, 0); // CRC32 of empty data should be 0
    
    let test_data = b"Hello, World!";
    let test_checksum = calculate_checksum(test_data);
    
    // Same data should produce same checksum
    let duplicate_checksum = calculate_checksum(test_data);
    assert_eq!(test_checksum, duplicate_checksum);
}