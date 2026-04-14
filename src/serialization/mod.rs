use std::path::Path;

pub(crate) mod deserializers;
pub(crate) mod serializers;

pub fn registry_exists(path: &Path) -> bool {
    path.exists()
}
