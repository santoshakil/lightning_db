use std::sync::Arc;

pub struct UmodelUstore {
    _placeholder: Arc<()>,
}

impl UmodelUstore {
    pub fn new() -> Self {
        Self {
            _placeholder: Arc::new(()),
        }
    }
}
