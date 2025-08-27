use std::sync::Arc;

pub struct Upreprocessing {
    _placeholder: Arc<()>,
}

impl Upreprocessing {
    pub fn new() -> Self {
        Self {
            _placeholder: Arc::new(()),
        }
    }
}
