use std::sync::Arc;

pub struct Utraining {
    _placeholder: Arc<()>,
}

impl Utraining {
    pub fn new() -> Self {
        Self {
            _placeholder: Arc::new(()),
        }
    }
}
