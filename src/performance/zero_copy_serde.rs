use std::alloc::{alloc, dealloc, Layout};
use std::cell::RefCell;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::mem::{align_of, size_of, MaybeUninit};
use std::ops::{Deref, DerefMut};
use std::ptr::{self, NonNull};
use std::slice;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use parking_lot::{RwLock, Mutex};
use crate::core::error::{Error, Result};

pub unsafe trait ZeroCopy: Sized {
    fn write_bytes(&self, buf: &mut [u8]) -> Result<usize>;
    fn read_bytes(buf: &[u8]) -> Result<Self>;
    fn size_bytes(&self) -> usize;
    fn alignment() -> usize {
        align_of::<Self>()
    }
}

pub trait ZeroCopySerialize {
    fn serialize_into(&self, writer: &mut ZeroCopyWriter) -> Result<()>;
    fn serialized_size(&self) -> usize;
}

pub trait ZeroCopyDeserialize: Sized {
    fn deserialize_from(reader: &mut ZeroCopyReader) -> Result<Self>;
}

#[repr(C)]
pub struct ZeroCopyBuffer {
    data: NonNull<u8>,
    len: usize,
    capacity: usize,
    alignment: usize,
    owned: bool,
}

impl ZeroCopyBuffer {
    pub fn new(capacity: usize, alignment: usize) -> Result<Self> {
        if capacity == 0 {
            return Err(Error::InvalidArgument("Capacity must be > 0".to_string()));
        }
        
        let layout = Layout::from_size_align(capacity, alignment)
            .map_err(|e| Error::Generic(format!("Invalid layout: {}", e)))?;
        
        let data = unsafe {
            let ptr = alloc(layout);
            if ptr.is_null() {
                return Err(Error::OutOfMemory { 
                    requested: capacity,
                    available: 0,
                });
            }
            NonNull::new_unchecked(ptr)
        };
        
        Ok(Self {
            data,
            len: 0,
            capacity,
            alignment,
            owned: true,
        })
    }
    
    pub fn from_slice(slice: &[u8]) -> Result<Self> {
        let mut buffer = Self::new(slice.len(), 1)?;
        buffer.extend_from_slice(slice)?;
        Ok(buffer)
    }
    
    pub unsafe fn from_raw_parts(ptr: *mut u8, len: usize, capacity: usize) -> Self {
        Self {
            data: NonNull::new_unchecked(ptr),
            len,
            capacity,
            alignment: 1,
            owned: false,
        }
    }
    
    pub fn as_slice(&self) -> &[u8] {
        unsafe { slice::from_raw_parts(self.data.as_ptr(), self.len) }
    }
    
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe { slice::from_raw_parts_mut(self.data.as_ptr(), self.len) }
    }
    
    pub fn extend_from_slice(&mut self, slice: &[u8]) -> Result<()> {
        if self.len + slice.len() > self.capacity {
            return Err(Error::Generic("Buffer capacity exceeded".to_string()));
        }
        
        unsafe {
            ptr::copy_nonoverlapping(
                slice.as_ptr(),
                self.data.as_ptr().add(self.len),
                slice.len(),
            );
        }
        
        self.len += slice.len();
        Ok(())
    }
    
    pub fn clear(&mut self) {
        self.len = 0;
    }
    
    pub fn resize(&mut self, new_capacity: usize) -> Result<()> {
        if !self.owned {
            return Err(Error::Generic("Cannot resize non-owned buffer".to_string()));
        }
        
        if new_capacity < self.len {
            return Err(Error::InvalidArgument(
                "New capacity smaller than current length".to_string()
            ));
        }
        
        let new_layout = Layout::from_size_align(new_capacity, self.alignment)
            .map_err(|e| Error::Generic(format!("Invalid layout: {}", e)))?;
        
        let new_data = unsafe {
            let ptr = alloc(new_layout);
            if ptr.is_null() {
                return Err(Error::OutOfMemory {
                    requested: new_capacity,
                    available: 0,
                });
            }
            
            ptr::copy_nonoverlapping(self.data.as_ptr(), ptr, self.len);
            
            let old_layout = Layout::from_size_align_unchecked(self.capacity, self.alignment);
            dealloc(self.data.as_ptr(), old_layout);
            
            NonNull::new_unchecked(ptr)
        };
        
        self.data = new_data;
        self.capacity = new_capacity;
        Ok(())
    }
    
    pub fn len(&self) -> usize {
        self.len
    }
    
    pub fn capacity(&self) -> usize {
        self.capacity
    }
    
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
}

impl Drop for ZeroCopyBuffer {
    fn drop(&mut self) {
        if self.owned && self.capacity > 0 {
            unsafe {
                let layout = Layout::from_size_align_unchecked(self.capacity, self.alignment);
                dealloc(self.data.as_ptr(), layout);
            }
        }
    }
}

unsafe impl Send for ZeroCopyBuffer {}
unsafe impl Sync for ZeroCopyBuffer {}

pub struct ZeroCopyWriter {
    buffer: ZeroCopyBuffer,
    position: usize,
    endianness: Endianness,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Endianness {
    Little,
    Big,
    Native,
}

impl ZeroCopyWriter {
    pub fn new(capacity: usize) -> Result<Self> {
        Ok(Self {
            buffer: ZeroCopyBuffer::new(capacity, 8)?,
            position: 0,
            endianness: Endianness::Native,
        })
    }
    
    pub fn with_endianness(capacity: usize, endianness: Endianness) -> Result<Self> {
        Ok(Self {
            buffer: ZeroCopyBuffer::new(capacity, 8)?,
            position: 0,
            endianness,
        })
    }
    
    pub fn write_raw(&mut self, data: &[u8]) -> Result<()> {
        let end_pos = self.position + data.len();
        
        if end_pos > self.buffer.capacity {
            let new_capacity = (end_pos * 3) / 2;
            self.buffer.resize(new_capacity)?;
        }
        
        unsafe {
            ptr::copy_nonoverlapping(
                data.as_ptr(),
                self.buffer.data.as_ptr().add(self.position),
                data.len(),
            );
        }
        
        self.position = end_pos;
        if self.position > self.buffer.len {
            self.buffer.len = self.position;
        }
        
        Ok(())
    }
    
    pub fn write_u8(&mut self, val: u8) -> Result<()> {
        self.write_raw(&[val])
    }
    
    pub fn write_u16(&mut self, val: u16) -> Result<()> {
        let bytes = match self.endianness {
            Endianness::Little | Endianness::Native if cfg!(target_endian = "little") => {
                val.to_le_bytes()
            }
            _ => val.to_be_bytes(),
        };
        self.write_raw(&bytes)
    }
    
    pub fn write_u32(&mut self, val: u32) -> Result<()> {
        let bytes = match self.endianness {
            Endianness::Little | Endianness::Native if cfg!(target_endian = "little") => {
                val.to_le_bytes()
            }
            _ => val.to_be_bytes(),
        };
        self.write_raw(&bytes)
    }
    
    pub fn write_u64(&mut self, val: u64) -> Result<()> {
        let bytes = match self.endianness {
            Endianness::Little | Endianness::Native if cfg!(target_endian = "little") => {
                val.to_le_bytes()
            }
            _ => val.to_be_bytes(),
        };
        self.write_raw(&bytes)
    }
    
    pub fn write_i8(&mut self, val: i8) -> Result<()> {
        self.write_u8(val as u8)
    }
    
    pub fn write_i16(&mut self, val: i16) -> Result<()> {
        self.write_u16(val as u16)
    }
    
    pub fn write_i32(&mut self, val: i32) -> Result<()> {
        self.write_u32(val as u32)
    }
    
    pub fn write_i64(&mut self, val: i64) -> Result<()> {
        self.write_u64(val as u64)
    }
    
    pub fn write_f32(&mut self, val: f32) -> Result<()> {
        self.write_u32(val.to_bits())
    }
    
    pub fn write_f64(&mut self, val: f64) -> Result<()> {
        self.write_u64(val.to_bits())
    }
    
    pub fn write_bool(&mut self, val: bool) -> Result<()> {
        self.write_u8(if val { 1 } else { 0 })
    }
    
    pub fn write_string(&mut self, s: &str) -> Result<()> {
        let bytes = s.as_bytes();
        self.write_u32(bytes.len() as u32)?;
        self.write_raw(bytes)
    }
    
    pub fn write_bytes(&mut self, bytes: &[u8]) -> Result<()> {
        self.write_u32(bytes.len() as u32)?;
        self.write_raw(bytes)
    }
    
    pub fn align_to(&mut self, alignment: usize) -> Result<()> {
        let remainder = self.position % alignment;
        if remainder != 0 {
            let padding = alignment - remainder;
            for _ in 0..padding {
                self.write_u8(0)?;
            }
        }
        Ok(())
    }
    
    pub fn position(&self) -> usize {
        self.position
    }
    
    pub fn into_buffer(self) -> ZeroCopyBuffer {
        self.buffer
    }
    
    pub fn as_slice(&self) -> &[u8] {
        &self.buffer.as_slice()[..self.position]
    }
}

pub struct ZeroCopyReader<'a> {
    data: &'a [u8],
    position: usize,
    endianness: Endianness,
}

impl<'a> ZeroCopyReader<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        Self {
            data,
            position: 0,
            endianness: Endianness::Native,
        }
    }
    
    pub fn with_endianness(data: &'a [u8], endianness: Endianness) -> Self {
        Self {
            data,
            position: 0,
            endianness,
        }
    }
    
    pub fn read_raw(&mut self, len: usize) -> Result<&'a [u8]> {
        if self.position + len > self.data.len() {
            return Err(Error::Generic("Buffer underflow".to_string()));
        }
        
        let slice = &self.data[self.position..self.position + len];
        self.position += len;
        Ok(slice)
    }
    
    pub fn read_u8(&mut self) -> Result<u8> {
        let bytes = self.read_raw(1)?;
        Ok(bytes[0])
    }
    
    pub fn read_u16(&mut self) -> Result<u16> {
        let bytes = self.read_raw(2)?;
        let array: [u8; 2] = bytes.try_into().unwrap();
        
        Ok(match self.endianness {
            Endianness::Little | Endianness::Native if cfg!(target_endian = "little") => {
                u16::from_le_bytes(array)
            }
            _ => u16::from_be_bytes(array),
        })
    }
    
    pub fn read_u32(&mut self) -> Result<u32> {
        let bytes = self.read_raw(4)?;
        let array: [u8; 4] = bytes.try_into().unwrap();
        
        Ok(match self.endianness {
            Endianness::Little | Endianness::Native if cfg!(target_endian = "little") => {
                u32::from_le_bytes(array)
            }
            _ => u32::from_be_bytes(array),
        })
    }
    
    pub fn read_u64(&mut self) -> Result<u64> {
        let bytes = self.read_raw(8)?;
        let array: [u8; 8] = bytes.try_into().unwrap();
        
        Ok(match self.endianness {
            Endianness::Little | Endianness::Native if cfg!(target_endian = "little") => {
                u64::from_le_bytes(array)
            }
            _ => u64::from_be_bytes(array),
        })
    }
    
    pub fn read_i8(&mut self) -> Result<i8> {
        Ok(self.read_u8()? as i8)
    }
    
    pub fn read_i16(&mut self) -> Result<i16> {
        Ok(self.read_u16()? as i16)
    }
    
    pub fn read_i32(&mut self) -> Result<i32> {
        Ok(self.read_u32()? as i32)
    }
    
    pub fn read_i64(&mut self) -> Result<i64> {
        Ok(self.read_u64()? as i64)
    }
    
    pub fn read_f32(&mut self) -> Result<f32> {
        Ok(f32::from_bits(self.read_u32()?))
    }
    
    pub fn read_f64(&mut self) -> Result<f64> {
        Ok(f64::from_bits(self.read_u64()?))
    }
    
    pub fn read_bool(&mut self) -> Result<bool> {
        Ok(self.read_u8()? != 0)
    }
    
    pub fn read_string(&mut self) -> Result<String> {
        let len = self.read_u32()? as usize;
        let bytes = self.read_raw(len)?;
        String::from_utf8(bytes.to_vec())
            .map_err(|e| Error::Generic(format!("Invalid UTF-8: {}", e)))
    }
    
    pub fn read_bytes(&mut self) -> Result<Vec<u8>> {
        let len = self.read_u32()? as usize;
        Ok(self.read_raw(len)?.to_vec())
    }
    
    pub fn skip(&mut self, bytes: usize) -> Result<()> {
        if self.position + bytes > self.data.len() {
            return Err(Error::Generic("Buffer underflow".to_string()));
        }
        self.position += bytes;
        Ok(())
    }
    
    pub fn align_to(&mut self, alignment: usize) -> Result<()> {
        let remainder = self.position % alignment;
        if remainder != 0 {
            self.skip(alignment - remainder)?;
        }
        Ok(())
    }
    
    pub fn position(&self) -> usize {
        self.position
    }
    
    pub fn remaining(&self) -> usize {
        self.data.len() - self.position
    }
    
    pub fn is_empty(&self) -> bool {
        self.position >= self.data.len()
    }
}

pub struct ZeroCopyArena {
    chunks: Vec<ZeroCopyBuffer>,
    current_chunk: usize,
    chunk_size: usize,
    total_allocated: AtomicUsize,
}

impl ZeroCopyArena {
    pub fn new(chunk_size: usize) -> Self {
        Self {
            chunks: vec![],
            current_chunk: 0,
            chunk_size,
            total_allocated: AtomicUsize::new(0),
        }
    }
    
    pub fn allocate(&mut self, size: usize, alignment: usize) -> Result<*mut u8> {
        let aligned_size = (size + alignment - 1) & !(alignment - 1);
        
        if self.chunks.is_empty() || 
           self.chunks[self.current_chunk].len + aligned_size > self.chunks[self.current_chunk].capacity {
            let new_chunk_size = self.chunk_size.max(aligned_size);
            let new_chunk = ZeroCopyBuffer::new(new_chunk_size, alignment)?;
            self.chunks.push(new_chunk);
            self.current_chunk = self.chunks.len() - 1;
        }
        
        let chunk = &mut self.chunks[self.current_chunk];
        let ptr = unsafe { chunk.data.as_ptr().add(chunk.len) };
        chunk.len += aligned_size;
        
        self.total_allocated.fetch_add(aligned_size, Ordering::Relaxed);
        
        Ok(ptr)
    }
    
    pub fn allocate_slice<T>(&mut self, count: usize) -> Result<&mut [T]> {
        let size = size_of::<T>() * count;
        let alignment = align_of::<T>();
        let ptr = self.allocate(size, alignment)? as *mut T;
        
        unsafe {
            let slice = slice::from_raw_parts_mut(ptr, count);
            for i in 0..count {
                ptr::write(ptr.add(i), MaybeUninit::zeroed().assume_init());
            }
            Ok(slice)
        }
    }
    
    pub fn allocate_object<T>(&mut self, value: T) -> Result<&mut T> {
        let size = size_of::<T>();
        let alignment = align_of::<T>();
        let ptr = self.allocate(size, alignment)? as *mut T;
        
        unsafe {
            ptr::write(ptr, value);
            Ok(&mut *ptr)
        }
    }
    
    pub fn reset(&mut self) {
        for chunk in &mut self.chunks {
            chunk.len = 0;
        }
        self.current_chunk = 0;
        self.total_allocated.store(0, Ordering::Relaxed);
    }
    
    pub fn total_allocated(&self) -> usize {
        self.total_allocated.load(Ordering::Relaxed)
    }
}

#[repr(C)]
pub struct ZeroCopyString {
    ptr: NonNull<u8>,
    len: u32,
}

impl ZeroCopyString {
    pub fn from_str(s: &str, arena: &mut ZeroCopyArena) -> Result<Self> {
        let bytes = s.as_bytes();
        let ptr = arena.allocate(bytes.len(), 1)?;
        
        unsafe {
            ptr::copy_nonoverlapping(bytes.as_ptr(), ptr, bytes.len());
        }
        
        Ok(Self {
            ptr: unsafe { NonNull::new_unchecked(ptr) },
            len: bytes.len() as u32,
        })
    }
    
    pub fn as_str(&self) -> &str {
        unsafe {
            let slice = slice::from_raw_parts(self.ptr.as_ptr(), self.len as usize);
            std::str::from_utf8_unchecked(slice)
        }
    }
    
    pub fn len(&self) -> usize {
        self.len as usize
    }
}

#[repr(C)]
pub struct ZeroCopyVec<T> {
    ptr: NonNull<T>,
    len: u32,
    _phantom: PhantomData<T>,
}

impl<T> ZeroCopyVec<T> {
    pub fn from_slice(slice: &[T], arena: &mut ZeroCopyArena) -> Result<Self>
    where
        T: Clone,
    {
        let ptr = arena.allocate(
            size_of::<T>() * slice.len(),
            align_of::<T>(),
        )? as *mut T;
        
        unsafe {
            for (i, item) in slice.iter().enumerate() {
                ptr::write(ptr.add(i), item.clone());
            }
        }
        
        Ok(Self {
            ptr: unsafe { NonNull::new_unchecked(ptr) },
            len: slice.len() as u32,
            _phantom: PhantomData,
        })
    }
    
    pub fn as_slice(&self) -> &[T] {
        unsafe {
            slice::from_raw_parts(self.ptr.as_ptr(), self.len as usize)
        }
    }
    
    pub fn as_mut_slice(&mut self) -> &mut [T] {
        unsafe {
            slice::from_raw_parts_mut(self.ptr.as_ptr(), self.len as usize)
        }
    }
    
    pub fn len(&self) -> usize {
        self.len as usize
    }
    
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
}

macro_rules! impl_zero_copy_primitive {
    ($($t:ty),*) => {
        $(
            unsafe impl ZeroCopy for $t {
                fn write_bytes(&self, buf: &mut [u8]) -> Result<usize> {
                    let size = size_of::<Self>();
                    if buf.len() < size {
                        return Err(Error::Generic("Buffer too small".to_string()));
                    }
                    
                    unsafe {
                        ptr::copy_nonoverlapping(
                            self as *const Self as *const u8,
                            buf.as_mut_ptr(),
                            size,
                        );
                    }
                    
                    Ok(size)
                }
                
                fn read_bytes(buf: &[u8]) -> Result<Self> {
                    let size = size_of::<Self>();
                    if buf.len() < size {
                        return Err(Error::Generic("Buffer too small".to_string()));
                    }
                    
                    let mut value = MaybeUninit::<Self>::uninit();
                    unsafe {
                        ptr::copy_nonoverlapping(
                            buf.as_ptr(),
                            value.as_mut_ptr() as *mut u8,
                            size,
                        );
                        Ok(value.assume_init())
                    }
                }
                
                fn size_bytes(&self) -> usize {
                    size_of::<Self>()
                }
            }
        )*
    };
}

impl_zero_copy_primitive!(u8, u16, u32, u64, u128, i8, i16, i32, i64, i128, f32, f64, bool);

pub struct SharedZeroCopyBuffer {
    inner: Arc<RwLock<ZeroCopyBuffer>>,
}

impl SharedZeroCopyBuffer {
    pub fn new(capacity: usize, alignment: usize) -> Result<Self> {
        Ok(Self {
            inner: Arc::new(RwLock::new(ZeroCopyBuffer::new(capacity, alignment)?)),
        })
    }
    
    pub fn read(&self) -> parking_lot::RwLockReadGuard<ZeroCopyBuffer> {
        self.inner.read()
    }
    
    pub fn write(&self) -> parking_lot::RwLockWriteGuard<ZeroCopyBuffer> {
        self.inner.write()
    }
    
    pub fn clone_handle(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

pub struct ZeroCopyPool {
    buffers: Mutex<Vec<ZeroCopyBuffer>>,
    buffer_size: usize,
    max_buffers: usize,
}

impl ZeroCopyPool {
    pub fn new(buffer_size: usize, max_buffers: usize) -> Self {
        Self {
            buffers: Mutex::new(Vec::with_capacity(max_buffers)),
            buffer_size,
            max_buffers,
        }
    }
    
    pub fn acquire(&self) -> Result<ZeroCopyBuffer> {
        let mut buffers = self.buffers.lock();
        
        if let Some(mut buffer) = buffers.pop() {
            buffer.clear();
            Ok(buffer)
        } else {
            ZeroCopyBuffer::new(self.buffer_size, 8)
        }
    }
    
    pub fn release(&self, buffer: ZeroCopyBuffer) {
        let mut buffers = self.buffers.lock();
        
        if buffers.len() < self.max_buffers {
            buffers.push(buffer);
        }
    }
}

#[derive(Clone)]
pub struct ZeroCopySchema {
    fields: Vec<FieldDescriptor>,
    size: usize,
    alignment: usize,
}

#[derive(Clone)]
struct FieldDescriptor {
    name: String,
    offset: usize,
    size: usize,
    field_type: FieldType,
}

#[derive(Clone)]
enum FieldType {
    U8, U16, U32, U64,
    I8, I16, I32, I64,
    F32, F64,
    Bool,
    String,
    Bytes,
}

impl ZeroCopySchema {
    pub fn builder() -> SchemaBuilder {
        SchemaBuilder::new()
    }
    
    pub fn write_record(&self, writer: &mut ZeroCopyWriter, values: &[FieldValue]) -> Result<()> {
        if values.len() != self.fields.len() {
            return Err(Error::InvalidArgument("Field count mismatch".to_string()));
        }
        
        for (field, value) in self.fields.iter().zip(values.iter()) {
            match (&field.field_type, value) {
                (FieldType::U8, FieldValue::U8(v)) => writer.write_u8(*v)?,
                (FieldType::U16, FieldValue::U16(v)) => writer.write_u16(*v)?,
                (FieldType::U32, FieldValue::U32(v)) => writer.write_u32(*v)?,
                (FieldType::U64, FieldValue::U64(v)) => writer.write_u64(*v)?,
                (FieldType::I8, FieldValue::I8(v)) => writer.write_i8(*v)?,
                (FieldType::I16, FieldValue::I16(v)) => writer.write_i16(*v)?,
                (FieldType::I32, FieldValue::I32(v)) => writer.write_i32(*v)?,
                (FieldType::I64, FieldValue::I64(v)) => writer.write_i64(*v)?,
                (FieldType::F32, FieldValue::F32(v)) => writer.write_f32(*v)?,
                (FieldType::F64, FieldValue::F64(v)) => writer.write_f64(*v)?,
                (FieldType::Bool, FieldValue::Bool(v)) => writer.write_bool(*v)?,
                (FieldType::String, FieldValue::String(v)) => writer.write_string(v)?,
                (FieldType::Bytes, FieldValue::Bytes(v)) => writer.write_bytes(v)?,
                _ => return Err(Error::Generic("Type mismatch".to_string())),
            }
        }
        
        Ok(())
    }
    
    pub fn read_record(&self, reader: &mut ZeroCopyReader) -> Result<Vec<FieldValue>> {
        let mut values = Vec::with_capacity(self.fields.len());
        
        for field in &self.fields {
            let value = match field.field_type {
                FieldType::U8 => FieldValue::U8(reader.read_u8()?),
                FieldType::U16 => FieldValue::U16(reader.read_u16()?),
                FieldType::U32 => FieldValue::U32(reader.read_u32()?),
                FieldType::U64 => FieldValue::U64(reader.read_u64()?),
                FieldType::I8 => FieldValue::I8(reader.read_i8()?),
                FieldType::I16 => FieldValue::I16(reader.read_i16()?),
                FieldType::I32 => FieldValue::I32(reader.read_i32()?),
                FieldType::I64 => FieldValue::I64(reader.read_i64()?),
                FieldType::F32 => FieldValue::F32(reader.read_f32()?),
                FieldType::F64 => FieldValue::F64(reader.read_f64()?),
                FieldType::Bool => FieldValue::Bool(reader.read_bool()?),
                FieldType::String => FieldValue::String(reader.read_string()?),
                FieldType::Bytes => FieldValue::Bytes(reader.read_bytes()?),
            };
            values.push(value);
        }
        
        Ok(values)
    }
}

pub struct SchemaBuilder {
    fields: Vec<FieldDescriptor>,
    current_offset: usize,
    alignment: usize,
}

impl SchemaBuilder {
    pub fn new() -> Self {
        Self {
            fields: Vec::new(),
            current_offset: 0,
            alignment: 1,
        }
    }
    
    pub fn add_u8(mut self, name: &str) -> Self {
        self.add_field(name, 1, FieldType::U8);
        self
    }
    
    pub fn add_u16(mut self, name: &str) -> Self {
        self.align_offset(2);
        self.add_field(name, 2, FieldType::U16);
        self
    }
    
    pub fn add_u32(mut self, name: &str) -> Self {
        self.align_offset(4);
        self.add_field(name, 4, FieldType::U32);
        self
    }
    
    pub fn add_u64(mut self, name: &str) -> Self {
        self.align_offset(8);
        self.add_field(name, 8, FieldType::U64);
        self
    }
    
    pub fn add_string(mut self, name: &str) -> Self {
        self.align_offset(4);
        self.add_field(name, 0, FieldType::String);
        self
    }
    
    pub fn add_bytes(mut self, name: &str) -> Self {
        self.align_offset(4);
        self.add_field(name, 0, FieldType::Bytes);
        self
    }
    
    fn add_field(&mut self, name: &str, size: usize, field_type: FieldType) {
        self.fields.push(FieldDescriptor {
            name: name.to_string(),
            offset: self.current_offset,
            size,
            field_type,
        });
        self.current_offset += size;
    }
    
    fn align_offset(&mut self, alignment: usize) {
        let remainder = self.current_offset % alignment;
        if remainder != 0 {
            self.current_offset += alignment - remainder;
        }
        self.alignment = self.alignment.max(alignment);
    }
    
    pub fn build(self) -> ZeroCopySchema {
        ZeroCopySchema {
            fields: self.fields,
            size: self.current_offset,
            alignment: self.alignment,
        }
    }
}

#[derive(Debug, Clone)]
pub enum FieldValue {
    U8(u8), U16(u16), U32(u32), U64(u64),
    I8(i8), I16(i16), I32(i32), I64(i64),
    F32(f32), F64(f64),
    Bool(bool),
    String(String),
    Bytes(Vec<u8>),
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_zero_copy_buffer() {
        let mut buffer = ZeroCopyBuffer::new(1024, 8).unwrap();
        
        let data = b"Hello, World!";
        buffer.extend_from_slice(data).unwrap();
        
        assert_eq!(buffer.len(), data.len());
        assert_eq!(buffer.as_slice(), data);
    }
    
    #[test]
    fn test_zero_copy_writer_reader() {
        let mut writer = ZeroCopyWriter::new(1024).unwrap();
        
        writer.write_u32(42).unwrap();
        writer.write_string("test").unwrap();
        writer.write_f64(3.14159).unwrap();
        writer.write_bool(true).unwrap();
        
        let data = writer.as_slice();
        let mut reader = ZeroCopyReader::new(data);
        
        assert_eq!(reader.read_u32().unwrap(), 42);
        assert_eq!(reader.read_string().unwrap(), "test");
        assert_eq!(reader.read_f64().unwrap(), 3.14159);
        assert_eq!(reader.read_bool().unwrap(), true);
    }
    
    #[test]
    fn test_zero_copy_arena() {
        let mut arena = ZeroCopyArena::new(1024);
        
        let s1 = ZeroCopyString::from_str("Hello", &mut arena).unwrap();
        let s2 = ZeroCopyString::from_str("World", &mut arena).unwrap();
        
        assert_eq!(s1.as_str(), "Hello");
        assert_eq!(s2.as_str(), "World");
        
        let numbers = vec![1, 2, 3, 4, 5];
        let vec = ZeroCopyVec::from_slice(&numbers, &mut arena).unwrap();
        assert_eq!(vec.as_slice(), &numbers[..]);
    }
    
    #[test]
    fn test_zero_copy_primitives() {
        let val: u64 = 0x0123456789ABCDEF;
        let mut buf = [0u8; 8];
        
        val.write_bytes(&mut buf).unwrap();
        let restored = u64::read_bytes(&buf).unwrap();
        
        assert_eq!(val, restored);
    }
    
    #[test]
    fn test_endianness() {
        let mut writer_le = ZeroCopyWriter::with_endianness(1024, Endianness::Little).unwrap();
        let mut writer_be = ZeroCopyWriter::with_endianness(1024, Endianness::Big).unwrap();
        
        let value: u32 = 0x12345678;
        writer_le.write_u32(value).unwrap();
        writer_be.write_u32(value).unwrap();
        
        let data_le = writer_le.as_slice();
        let data_be = writer_be.as_slice();
        
        assert_eq!(data_le, &[0x78, 0x56, 0x34, 0x12]);
        assert_eq!(data_be, &[0x12, 0x34, 0x56, 0x78]);
    }
    
    #[test]
    fn test_schema() {
        let schema = ZeroCopySchema::builder()
            .add_u32("id")
            .add_string("name")
            .add_f64("score")
            .add_bool("active")
            .build();
        
        let mut writer = ZeroCopyWriter::new(1024).unwrap();
        
        let values = vec![
            FieldValue::U32(123),
            FieldValue::String("Alice".to_string()),
            FieldValue::F64(95.5),
            FieldValue::Bool(true),
        ];
        
        schema.write_record(&mut writer, &values).unwrap();
        
        let data = writer.as_slice();
        let mut reader = ZeroCopyReader::new(data);
        
        let read_values = schema.read_record(&mut reader).unwrap();
        
        match &read_values[0] {
            FieldValue::U32(v) => assert_eq!(*v, 123),
            _ => panic!("Wrong type"),
        }
        
        match &read_values[1] {
            FieldValue::String(v) => assert_eq!(v, "Alice"),
            _ => panic!("Wrong type"),
        }
    }
    
    #[test]
    fn test_buffer_pool() {
        let pool = ZeroCopyPool::new(1024, 10);
        
        let buffer1 = pool.acquire().unwrap();
        let buffer2 = pool.acquire().unwrap();
        
        assert_eq!(buffer1.capacity(), 1024);
        assert_eq!(buffer2.capacity(), 1024);
        
        pool.release(buffer1);
        
        let buffer3 = pool.acquire().unwrap();
        assert_eq!(buffer3.capacity(), 1024);
    }
}