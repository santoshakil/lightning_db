use std::fs;
use std::path::Path;
use tempfile::tempdir;

#[test]
fn test_page_manager_file_creation() {
    println!("Test: Creating a simple file");

    let dir = tempdir().unwrap();
    let file_path = dir.path().join("test.db");
    println!("File path: {:?}", file_path);

    // Just create a file
    let file = fs::File::create(&file_path).unwrap();
    println!("File created");

    // Set its length
    file.set_len(1024 * 1024).unwrap();
    println!("File length set to 1MB");

    // Drop the file
    drop(file);
    println!("File closed");

    // Verify it exists
    assert!(file_path.exists());
    println!("Test passed!");
}

#[test]
fn test_page_manager_mmap() {
    println!("Test: Creating mmap");

    let dir = tempdir().unwrap();
    let file_path = dir.path().join("test.db");

    // Create file with OpenOptions
    use std::fs::OpenOptions;
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(&file_path)
        .unwrap();
    println!("File created with OpenOptions");

    file.set_len(1024 * 1024).unwrap();
    println!("File length set");

    // Try to create mmap
    use memmap2::MmapOptions;
    let mmap = unsafe { MmapOptions::new().map_mut(&file) }.unwrap();
    println!("MmapMut created successfully, len: {}", mmap.len());

    drop(mmap);
    println!("Mmap dropped");

    drop(file);
    println!("File dropped");

    println!("Test passed!");
}
