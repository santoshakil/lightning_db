use memmap2::MmapOptions;
use std::fs::OpenOptions;
use tempfile::tempdir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Testing basic file operations...");

    let dir = tempdir()?;
    let path = dir.path().join("test.dat");

    println!("Creating file...");
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(&path)?;

    println!("Setting file size...");
    file.set_len(65536)?; // 64KB

    println!("Creating mmap...");
    let mut mmap = unsafe { MmapOptions::new().map_mut(&file)? };

    println!("Writing to mmap...");
    mmap[0..4].copy_from_slice(&[1, 2, 3, 4]);

    println!("Flushing mmap...");
    mmap.flush()?;

    println!("All basic operations completed successfully!");

    Ok(())
}
