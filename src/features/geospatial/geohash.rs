pub struct GeoHash {
    hash: String,
    precision: usize,
}

const BASE32: &[u8] = b"0123456789bcdefghjkmnpqrstuvwxyz";

pub fn encode_geohash(lat: f64, lon: f64, precision: usize) -> String {
    let mut hash = String::new();
    let mut lat_range = (-90.0, 90.0);
    let mut lon_range = (-180.0, 180.0);
    let mut bits = 0u8;
    let mut bit = 0;
    
    while hash.len() < precision {
        if bit % 2 == 0 {
            let mid = (lon_range.0 + lon_range.1) / 2.0;
            if lon > mid {
                bits |= 1 << (4 - bit / 2);
                lon_range.0 = mid;
            } else {
                lon_range.1 = mid;
            }
        } else {
            let mid = (lat_range.0 + lat_range.1) / 2.0;
            if lat > mid {
                bits |= 1 << (4 - bit / 2);
                lat_range.0 = mid;
            } else {
                lat_range.1 = mid;
            }
        }
        
        bit += 1;
        if bit == 10 {
            hash.push(BASE32[bits as usize] as char);
            bits = 0;
            bit = 0;
        }
    }
    
    hash
}

pub fn decode_geohash(hash: &str) -> (f64, f64, f64, f64) {
    let mut lat_range = (-90.0, 90.0);
    let mut lon_range = (-180.0, 180.0);
    let mut is_lon = true;
    
    for c in hash.chars() {
        let idx = BASE32.iter().position(|&x| x as char == c).unwrap_or(0);
        
        for i in (0..5).rev() {
            let bit = (idx >> i) & 1;
            if is_lon {
                let mid = (lon_range.0 + lon_range.1) / 2.0;
                if bit == 1 {
                    lon_range.0 = mid;
                } else {
                    lon_range.1 = mid;
                }
            } else {
                let mid = (lat_range.0 + lat_range.1) / 2.0;
                if bit == 1 {
                    lat_range.0 = mid;
                } else {
                    lat_range.1 = mid;
                }
            }
            is_lon = !is_lon;
        }
    }
    
    (lat_range.0, lat_range.1, lon_range.0, lon_range.1)
}

impl GeoHash {
    pub fn new(lat: f64, lon: f64, precision: usize) -> Self {
        Self {
            hash: encode_geohash(lat, lon, precision),
            precision,
        }
    }
    
    pub fn neighbors(&self) -> Vec<String> {
        Vec::new()
    }
}