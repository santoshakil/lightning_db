use serde::{Serialize, Deserialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Geometry {
    Point(Point),
    LineString(LineString),
    Polygon(Polygon),
    MultiPoint(Vec<Point>),
    MultiLineString(Vec<LineString>),
    MultiPolygon(MultiPolygon),
    GeometryCollection(Vec<Geometry>),
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct Point {
    pub x: f64,
    pub y: f64,
    pub z: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LineString {
    pub points: Vec<Point>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Polygon {
    pub exterior: LineString,
    pub holes: Vec<LineString>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MultiPolygon {
    pub polygons: Vec<Polygon>,
}

impl Point {
    pub fn new(x: f64, y: f64) -> Self {
        Self { x, y, z: None }
    }
    
    pub fn new_3d(x: f64, y: f64, z: f64) -> Self {
        Self { x, y, z: Some(z) }
    }
}

impl Geometry {
    pub fn bounds(&self) -> (Point, Point) {
        match self {
            Geometry::Point(p) => (*p, *p),
            Geometry::LineString(ls) => ls.bounds(),
            Geometry::Polygon(p) => p.bounds(),
            _ => (Point::new(0.0, 0.0), Point::new(0.0, 0.0)),
        }
    }
}

impl LineString {
    pub fn bounds(&self) -> (Point, Point) {
        let mut min_x = f64::MAX;
        let mut min_y = f64::MAX;
        let mut max_x = f64::MIN;
        let mut max_y = f64::MIN;
        
        for point in &self.points {
            min_x = min_x.min(point.x);
            min_y = min_y.min(point.y);
            max_x = max_x.max(point.x);
            max_y = max_y.max(point.y);
        }
        
        (Point::new(min_x, min_y), Point::new(max_x, max_y))
    }
}

impl Polygon {
    pub fn bounds(&self) -> (Point, Point) {
        self.exterior.bounds()
    }
}