use super::geometry::{Point, LineString, Polygon, Geometry};

pub fn distance(p1: &Point, p2: &Point) -> f64 {
    let dx = p2.x - p1.x;
    let dy = p2.y - p1.y;
    let dz = match (p1.z, p2.z) {
        (Some(z1), Some(z2)) => (z2 - z1).powi(2),
        _ => 0.0,
    };
    (dx * dx + dy * dy + dz).sqrt()
}

pub fn haversine_distance(p1: &Point, p2: &Point) -> f64 {
    const EARTH_RADIUS_KM: f64 = 6371.0;
    
    let lat1 = p1.y.to_radians();
    let lat2 = p2.y.to_radians();
    let dlat = (p2.y - p1.y).to_radians();
    let dlon = (p2.x - p1.x).to_radians();
    
    let a = (dlat / 2.0).sin().powi(2) + 
            lat1.cos() * lat2.cos() * (dlon / 2.0).sin().powi(2);
    let c = 2.0 * a.sqrt().asin();
    
    EARTH_RADIUS_KM * c
}

pub fn area(polygon: &Polygon) -> f64 {
    shoelace_formula(&polygon.exterior.points)
}

fn shoelace_formula(points: &[Point]) -> f64 {
    if points.len() < 3 {
        return 0.0;
    }
    
    let mut area = 0.0;
    for i in 0..points.len() {
        let j = (i + 1) % points.len();
        area += points[i].x * points[j].y;
        area -= points[j].x * points[i].y;
    }
    
    area.abs() / 2.0
}

pub fn contains(polygon: &Polygon, point: &Point) -> bool {
    point_in_polygon(point, &polygon.exterior.points)
}

fn point_in_polygon(point: &Point, vertices: &[Point]) -> bool {
    let mut inside = false;
    let n = vertices.len();
    
    let mut p1 = vertices[0];
    for i in 1..=n {
        let p2 = vertices[i % n];
        
        if point.y > p1.y.min(p2.y) {
            if point.y <= p1.y.max(p2.y) {
                if point.x <= p1.x.max(p2.x) {
                    let xinters = if p1.y != p2.y {
                        (point.y - p1.y) * (p2.x - p1.x) / (p2.y - p1.y) + p1.x
                    } else {
                        point.x
                    };
                    
                    if p1.x == p2.x || point.x <= xinters {
                        inside = !inside;
                    }
                }
            }
        }
        
        p1 = p2;
    }
    
    inside
}

pub fn intersects(geom1: &Geometry, geom2: &Geometry) -> bool {
    match (geom1, geom2) {
        (Geometry::Point(p1), Geometry::Point(p2)) => p1.x == p2.x && p1.y == p2.y,
        (Geometry::LineString(ls1), Geometry::LineString(ls2)) => lines_intersect(ls1, ls2),
        _ => false,
    }
}

fn lines_intersect(ls1: &LineString, ls2: &LineString) -> bool {
    for i in 0..ls1.points.len() - 1 {
        for j in 0..ls2.points.len() - 1 {
            if segments_intersect(
                &ls1.points[i],
                &ls1.points[i + 1],
                &ls2.points[j],
                &ls2.points[j + 1],
            ) {
                return true;
            }
        }
    }
    false
}

fn segments_intersect(p1: &Point, p2: &Point, p3: &Point, p4: &Point) -> bool {
    let d = (p1.x - p2.x) * (p3.y - p4.y) - (p1.y - p2.y) * (p3.x - p4.x);
    
    if d.abs() < 1e-10 {
        return false;
    }
    
    let t = ((p1.x - p3.x) * (p3.y - p4.y) - (p1.y - p3.y) * (p3.x - p4.x)) / d;
    let u = -((p1.x - p2.x) * (p1.y - p3.y) - (p1.y - p2.y) * (p1.x - p3.x)) / d;
    
    t >= 0.0 && t <= 1.0 && u >= 0.0 && u <= 1.0
}

pub fn buffer(geometry: &Geometry, distance: f64) -> Geometry {
    match geometry {
        Geometry::Point(p) => {
            let mut points = Vec::new();
            let segments = 32;
            for i in 0..segments {
                let angle = 2.0 * std::f64::consts::PI * i as f64 / segments as f64;
                points.push(Point::new(
                    p.x + distance * angle.cos(),
                    p.y + distance * angle.sin(),
                ));
            }
            points.push(points[0]);
            Geometry::Polygon(Polygon {
                exterior: LineString { points },
                holes: Vec::new(),
            })
        }
        _ => geometry.clone(),
    }
}