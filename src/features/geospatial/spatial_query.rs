use serde::{Serialize, Deserialize};

#[derive(Debug, Clone)]
pub struct SpatialQuery {
    pub predicate: SpatialPredicate,
    pub limit: Option<usize>,
    pub properties: Vec<String>,
}

#[derive(Debug, Clone)]
pub enum SpatialPredicate {
    Within(super::geometry::Geometry),
    Intersects(super::geometry::Geometry),
    Contains(super::geometry::Geometry),
    Distance { center: super::geometry::Point, radius: f64 },
    BoundingBox { min: super::geometry::Point, max: super::geometry::Point },
    KNearest { center: super::geometry::Point, k: usize },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryResult {
    pub objects: Vec<super::spatial_index::SpatialObject>,
    pub total: usize,
    pub query_time_ms: u64,
}

impl SpatialQuery {
    pub fn within(geometry: super::geometry::Geometry) -> Self {
        Self {
            predicate: SpatialPredicate::Within(geometry),
            limit: None,
            properties: Vec::new(),
        }
    }
    
    pub fn near(center: super::geometry::Point, radius: f64) -> Self {
        Self {
            predicate: SpatialPredicate::Distance { center, radius },
            limit: None,
            properties: Vec::new(),
        }
    }
    
    pub fn k_nearest(center: super::geometry::Point, k: usize) -> Self {
        Self {
            predicate: SpatialPredicate::KNearest { center, k },
            limit: Some(k),
            properties: Vec::new(),
        }
    }
}