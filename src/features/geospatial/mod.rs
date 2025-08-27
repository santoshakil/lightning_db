pub mod spatial_index;
pub mod geometry;
pub mod rtree;
pub mod quadtree;
pub mod geohash;
pub mod spatial_query;
pub mod projections;
pub mod spatial_ops;

pub use spatial_index::{SpatialIndex, IndexType, SpatialIndexConfig};
pub use geometry::{Point, LineString, Polygon, MultiPolygon, Geometry};
pub use rtree::{RTree, RTreeNode, BoundingBox};
pub use quadtree::{QuadTree, QuadNode, Quadrant};
pub use geohash::{GeoHash, encode_geohash, decode_geohash};
pub use spatial_query::{SpatialQuery, SpatialPredicate, QueryResult};
pub use projections::{Projection, CoordinateSystem, transform_coordinates};
pub use spatial_ops::{distance, area, contains, intersects, buffer};