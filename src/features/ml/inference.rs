use std::sync::Arc;

pub struct Uinference {
    _placeholder: Arc<()>,
}

impl Uinference {
    pub fn new() -> Self {
        Self {
            _placeholder: Arc::new(()),
        }
    }
}
