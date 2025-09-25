use std::borrow::Cow;

#[inline(always)]
pub fn minimize_allocation<'a>(key: &'a [u8]) -> Cow<'a, [u8]> {
    Cow::Borrowed(key)
}

#[inline(always)]
pub fn inline_small_vec(data: &[u8]) -> Vec<u8> {
    if data.len() <= 32 {
        let mut vec = Vec::with_capacity(32);
        vec.extend_from_slice(data);
        vec
    } else {
        data.to_vec()
    }
}

pub struct KeyRef<'a> {
    data: &'a [u8],
}

impl<'a> KeyRef<'a> {
    #[inline(always)]
    pub fn new(data: &'a [u8]) -> Self {
        Self { data }
    }

    #[inline(always)]
    pub fn as_bytes(&self) -> &[u8] {
        self.data
    }

    #[inline(always)]
    pub fn to_owned(&self) -> Vec<u8> {
        inline_small_vec(self.data)
    }
}

impl<'a> AsRef<[u8]> for KeyRef<'a> {
    #[inline(always)]
    fn as_ref(&self) -> &[u8] {
        self.data
    }
}

#[inline(always)]
pub fn fast_clone<T: Clone>(value: &T) -> T {
    value.clone()
}

#[inline(never)]
pub fn cold_path_error<E>(err: E) -> E {
    err
}