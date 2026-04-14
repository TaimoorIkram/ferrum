use std::fs::File;
use std::io::Read;
use std::path::Path;

use crate::persistence::{DatabaseRegistry, DatabaseRegistryData};

pub fn load_registry(path: &Path) -> Result<DatabaseRegistry, String> {
    // Check if file exists
    if !path.exists() {
        return Err(format!("File does not exist: {:?}", path));
    }

    // Read file contents
    let mut file = File::open(path).map_err(|e| format!("Failed to open file: {}", e))?;

    let mut contents = String::new();
    file.read_to_string(&mut contents)
        .map_err(|e| format!("Failed to read file: {}", e))?;

    // Deserialize from JSON
    let data: DatabaseRegistryData =
        serde_json::from_str(&contents).map_err(|e| format!("Deserialization failed: {}", e))?;

    // Convert back to runtime format
    Ok(DatabaseRegistry::from_data(data))
}
