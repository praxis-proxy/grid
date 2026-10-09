//! Maps cloud region codes to approximate coordinates so a registry entry
//! with only a region still lands on the map.

/// A point on the map in degrees.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Coordinates {
    /// Degrees north of the equator.
    pub latitude: f64,
    /// Degrees east of the prime meridian.
    pub longitude: f64,
}

/// Region code → (latitude, longitude) of its principal data-center metro,
/// rounded to 0.01 degree.
const TABLE: &[(&str, f64, f64)] = &[
    // AWS
    ("us-east-1", 38.95, -77.45),
    ("us-east-2", 40.09, -82.75),
    ("us-west-1", 37.35, -121.96),
    ("us-west-2", 45.60, -122.68),
    ("ca-central-1", 45.50, -73.57),
    ("ca-west-1", 51.05, -114.07),
    ("sa-east-1", -23.55, -46.63),
    ("eu-west-1", 53.35, -6.26),
    ("eu-west-2", 51.51, -0.13),
    ("eu-west-3", 48.86, 2.35),
    ("eu-central-1", 50.11, 8.68),
    ("eu-central-2", 47.38, 8.54),
    ("eu-north-1", 59.33, 18.07),
    ("eu-south-1", 45.46, 9.19),
    ("eu-south-2", 40.42, -3.70),
    ("me-south-1", 26.07, 50.56),
    ("me-central-1", 24.45, 54.38),
    ("il-central-1", 32.08, 34.78),
    ("af-south-1", -33.92, 18.42),
    ("ap-south-1", 19.08, 72.88),
    ("ap-south-2", 17.39, 78.49),
    ("ap-southeast-1", 1.35, 103.82),
    ("ap-southeast-2", -33.87, 151.21),
    ("ap-southeast-3", -6.21, 106.85),
    ("ap-southeast-4", -37.81, 144.96),
    ("ap-northeast-1", 35.68, 139.69),
    ("ap-northeast-2", 37.57, 126.98),
    ("ap-northeast-3", 34.69, 135.50),
    ("ap-east-1", 22.32, 114.17),
    // GCP
    ("us-central1", 41.26, -95.94),
    ("us-east1", 33.20, -80.02),
    ("us-east4", 39.04, -77.49),
    ("us-east5", 39.96, -83.00),
    ("us-west1", 45.60, -121.18),
    ("us-west2", 34.05, -118.24),
    ("us-west3", 40.76, -111.89),
    ("us-west4", 36.17, -115.14),
    ("us-south1", 32.78, -96.80),
    ("northamerica-northeast1", 45.50, -73.57),
    ("northamerica-northeast2", 43.65, -79.38),
    ("southamerica-east1", -23.55, -46.63),
    ("europe-west1", 50.47, 3.87),
    ("europe-west2", 51.51, -0.13),
    ("europe-west3", 50.11, 8.68),
    ("europe-west4", 53.44, 6.83),
    ("europe-west6", 47.38, 8.54),
    ("europe-west8", 45.46, 9.19),
    ("europe-west9", 48.86, 2.35),
    ("europe-north1", 60.57, 27.19),
    ("europe-central2", 52.23, 21.01),
    ("europe-southwest1", 40.42, -3.70),
    ("asia-east1", 24.05, 120.52),
    ("asia-east2", 22.32, 114.17),
    ("asia-northeast1", 35.68, 139.69),
    ("asia-northeast2", 34.69, 135.50),
    ("asia-northeast3", 37.57, 126.98),
    ("asia-south1", 19.08, 72.88),
    ("asia-south2", 28.61, 77.21),
    ("asia-southeast1", 1.35, 103.82),
    ("asia-southeast2", -6.21, 106.85),
    ("australia-southeast1", -33.87, 151.21),
    ("australia-southeast2", -37.81, 144.96),
    ("me-west1", 32.08, 34.78),
    // Azure
    ("eastus", 37.37, -79.82),
    ("eastus2", 36.67, -78.39),
    ("centralus", 41.59, -93.62),
    ("northcentralus", 41.88, -87.63),
    ("southcentralus", 29.42, -98.49),
    ("westus", 37.78, -122.42),
    ("westus2", 47.23, -119.85),
    ("westus3", 33.45, -112.07),
    ("canadacentral", 43.65, -79.38),
    ("canadaeast", 46.81, -71.21),
    ("brazilsouth", -23.55, -46.63),
    ("northeurope", 53.35, -6.26),
    ("westeurope", 52.37, 4.90),
    ("uksouth", 51.51, -0.13),
    ("ukwest", 51.48, -3.18),
    ("francecentral", 48.86, 2.35),
    ("germanywestcentral", 50.11, 8.68),
    ("switzerlandnorth", 47.38, 8.54),
    ("norwayeast", 59.91, 10.75),
    ("swedencentral", 60.67, 17.14),
    ("polandcentral", 52.23, 21.01),
    ("italynorth", 45.46, 9.19),
    ("spaincentral", 40.42, -3.70),
    ("uaenorth", 25.20, 55.27),
    ("qatarcentral", 25.29, 51.53),
    ("southafricanorth", -26.20, 28.05),
    ("centralindia", 18.52, 73.86),
    ("southindia", 13.08, 80.27),
    ("japaneast", 35.68, 139.69),
    ("japanwest", 34.69, 135.50),
    ("koreacentral", 37.57, 126.98),
    ("eastasia", 22.32, 114.17),
    ("southeastasia", 1.35, 103.82),
    ("australiaeast", -33.87, 151.21),
    ("australiasoutheast", -37.81, 144.96),
];

/// Resolves a cloud region code, ignoring case and surrounding whitespace.
#[must_use]
pub fn lookup(region: &str) -> Option<Coordinates> {
    let wanted = region.trim().to_lowercase();
    TABLE
        .iter()
        .find(|(name, ..)| *name == wanted)
        .map(|&(_, latitude, longitude)| Coordinates { latitude, longitude })
}

#[cfg(test)]
mod tests {
    use super::{Coordinates, lookup};

    #[test]
    fn known_regions_resolve_case_insensitively_with_whitespace_trimmed() {
        let cases = [
            ("us-east-2", 40.09, -82.75),
            ("eu-west-2", 51.51, -0.13),
            ("  US-WEST-2 ", 45.60, -122.68),
            ("europe-west4", 53.44, 6.83),
            ("westeurope", 52.37, 4.90),
        ];
        for (region, latitude, longitude) in cases {
            assert_eq!(
                lookup(region),
                Some(Coordinates { latitude, longitude }),
                "region {region}"
            );
        }
    }

    #[test]
    fn unknown_and_empty_regions_resolve_to_none() {
        assert_eq!(lookup("moon-base-1"), None, "unknown region");
        assert_eq!(lookup(""), None, "empty region");
    }
}
