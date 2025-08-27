use std::f64::consts::PI;

#[derive(Debug, Clone, Copy)]
pub enum Projection {
    WGS84,
    Mercator,
    UTM { zone: u8, hemisphere: Hemisphere },
    LambertConformalConic,
    AlbersEqualArea,
}

#[derive(Debug, Clone, Copy)]
pub enum CoordinateSystem {
    Geographic,
    Projected,
    Geocentric,
}

#[derive(Debug, Clone, Copy)]
pub enum Hemisphere {
    North,
    South,
}

pub fn transform_coordinates(
    x: f64,
    y: f64,
    from: Projection,
    to: Projection,
) -> (f64, f64) {
    match (from, to) {
        (Projection::WGS84, Projection::Mercator) => wgs84_to_mercator(x, y),
        (Projection::Mercator, Projection::WGS84) => mercator_to_wgs84(x, y),
        _ => (x, y),
    }
}

fn wgs84_to_mercator(lon: f64, lat: f64) -> (f64, f64) {
    let x = lon * PI / 180.0;
    let y = ((PI / 4.0 + lat * PI / 360.0).tan()).ln();
    (x, y)
}

fn mercator_to_wgs84(x: f64, y: f64) -> (f64, f64) {
    let lon = x * 180.0 / PI;
    let lat = (y.exp().atan() * 2.0 - PI / 2.0) * 180.0 / PI;
    (lon, lat)
}

pub fn degrees_to_radians(degrees: f64) -> f64 {
    degrees * PI / 180.0
}

pub fn radians_to_degrees(radians: f64) -> f64 {
    radians * 180.0 / PI
}