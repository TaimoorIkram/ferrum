use std::fs::File;
use std::io::Write;
use std::path::Path;

use crate::persistence::DatabaseRegistry;

pub fn save_registry(registry: &DatabaseRegistry, path: &Path) -> Result<(), String> {
    // Convert to serializable format
    let data = registry.to_data();

    // Serialize to JSON
    let json =
        serde_json::to_string_pretty(&data).map_err(|e| format!("Serialization failed: {}", e))?;

    // Write to file
    let mut file = File::create(path).map_err(|e| format!("Failed to create file: {}", e))?;

    file.write_all(json.as_bytes())
        .map_err(|e| format!("Failed to write to file: {}", e))?;

    Ok(())
}
