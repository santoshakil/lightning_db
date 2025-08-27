use std::sync::Arc;

pub struct UfeatureUstore {
    _placeholder: Arc<()>,
}

impl UfeatureUstore {
    pub fn new() -> Self {
        Self {
            _placeholder: Arc::new(()),
        }
    }
}
