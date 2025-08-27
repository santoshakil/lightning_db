use std::sync::Arc;

pub struct Uevaluation {
    _placeholder: Arc<()>,
}

impl Uevaluation {
    pub fn new() -> Self {
        Self {
            _placeholder: Arc::new(()),
        }
    }
}
