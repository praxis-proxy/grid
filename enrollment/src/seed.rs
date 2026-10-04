//! Reserved-name seeds: bootstrap vouches for a reserved site's key with the CA key,
//! so write access to the Secret that carries a seed grants nothing.

use std::path::Path;

use crate::store::{SeedRecord, Seeded};

/// First line of a seed, versioning its form.
const SEED_V1: &str = "grid-reserved-seed-v1";

/// File suffix of a seed's text in the mounted Secret.
pub const SEED_SUFFIX: &str = ".seed";

/// File suffix of a seed's signature, lowercase hex.
pub const SIGNATURE_SUFFIX: &str = ".sig";

/// Why a seed was not accepted.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum SeedError {
    /// The text is not a seed.
    #[error("malformed seed")]
    Malformed,
    /// The signature is not the current CA's over this text.
    #[error("seed signature does not verify against the grid CA")]
    BadSignature,
    /// The seed names a site that is not reserved.
    #[error("seed names {0}, which is not a reserved site")]
    NotReserved(String),
    /// The seed could not be read.
    #[error("reading the seed: {0}")]
    Read(String),
}

/// The signed text of `seed`.
#[must_use]
pub fn text(seed: &SeedRecord) -> String {
    format!(
        "{SEED_V1}\nsite={}\nkey={}\ngeneration={}\nissued={}\n",
        seed.site_name,
        seed.key_sha256,
        seed.generation,
        seed.issued_at.unix_timestamp()
    )
}

/// Sign `seed` with the CA key, returning the text and its hex signature.
///
/// # Errors
///
/// Returns a message when signing fails.
pub fn sign(seed: &SeedRecord, ca: &certs::CaCert) -> Result<(String, String), String> {
    let body = text(seed);
    let signature = certs::sign_with_ca(ca, body.as_bytes()).map_err(|err| err.to_string())?;
    Ok((body, hex(&signature)))
}

/// The seed in `body`, if `signature_hex` is the CA's signature over exactly that text.
///
/// # Errors
///
/// Returns [`SeedError`] when the signature does not verify or the text is not a seed.
pub fn verified(body: &str, signature_hex: &str, ca_cert_pem: &str) -> Result<SeedRecord, SeedError> {
    let signature = unhex(signature_hex.trim()).ok_or(SeedError::BadSignature)?;
    certs::verify_ca_signature(ca_cert_pem, body.as_bytes(), &signature).map_err(|_bad| SeedError::BadSignature)?;
    parse(body)
}

/// Parse the exact form [`text`] writes.
#[expect(clippy::too_many_lines, reason = "one strict check per field")]
fn parse(body: &str) -> Result<SeedRecord, SeedError> {
    let lines: Vec<&str> = body
        .strip_suffix('\n')
        .ok_or(SeedError::Malformed)?
        .split('\n')
        .collect();
    let [header, site, key, generation, issued] = lines.as_slice() else {
        return Err(SeedError::Malformed);
    };
    let value = |line: &'_ str, name: &str| {
        line.strip_prefix(name)
            .and_then(|rest| rest.strip_prefix('='))
            .map(str::to_owned)
            .ok_or(SeedError::Malformed)
    };
    let seed = SeedRecord {
        site_name: value(site, "site")?,
        key_sha256: value(key, "key")?,
        generation: value(generation, "generation")?
            .parse()
            .map_err(|_bad| SeedError::Malformed)?,
        issued_at: value(issued, "issued")?
            .parse()
            .ok()
            .and_then(|secs| time::OffsetDateTime::from_unix_timestamp(secs).ok())
            .ok_or(SeedError::Malformed)?,
    };
    let key_is_digest = seed.key_sha256.len() == 64
        && seed
            .key_sha256
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte));
    if *header != SEED_V1 || !key_is_digest || certs::validate_site_name(&seed.site_name).is_err() {
        return Err(SeedError::Malformed);
    }
    // Only the exact form text writes, so no signed body reads two ways.
    if text(&seed) != body {
        return Err(SeedError::Malformed);
    }
    Ok(seed)
}

/// Every seed in `dir`, by the file stem it was read from, each verified against `ca_cert_pem`.
///
/// A missing directory holds no seeds.
#[must_use]
pub fn load_dir(dir: &Path, ca_cert_pem: &str) -> Vec<(String, Result<SeedRecord, SeedError>)> {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return Vec::new();
    };
    let mut seeds: Vec<_> = entries
        .filter_map(Result::ok)
        .filter_map(|entry| {
            let name = entry.file_name().into_string().ok()?;
            name.strip_suffix(SEED_SUFFIX).map(str::to_owned)
        })
        .map(|stem| {
            let read = |suffix: &str| {
                std::fs::read_to_string(dir.join(format!("{stem}{suffix}")))
                    .map_err(|err| SeedError::Read(err.to_string()))
            };
            let seed = read(SEED_SUFFIX)
                .and_then(|body| read(SIGNATURE_SUFFIX).map(|signature| (body, signature)))
                .and_then(|(body, signature)| verified(&body, &signature, ca_cert_pem));
            (stem, seed)
        })
        .collect();
    seeds.sort_by(|left, right| left.0.cmp(&right.0));
    seeds
}

/// Apply every seed in `dir` the current CA signed for a reserved name, warning once per
/// problem. Returns the problems newly warned about, by seed.
#[expect(
    clippy::too_many_lines,
    reason = "verify, gate on the reserved list, apply, and report read as one pass"
)]
pub async fn apply(
    state: &crate::AppState,
    dir: &Path,
    reported: &mut std::collections::HashMap<String, String>,
) -> Vec<(String, String)> {
    let ca_pem = state.ca.current().cert_pem.clone();
    let mut warned = Vec::new();
    for (stem, seed) in load_dir(dir, &ca_pem) {
        let applied = match seed {
            Ok(seed) if !state.reserved_sites.contains(&seed.site_name) => {
                Err(SeedError::NotReserved(seed.site_name).to_string())
            },
            Ok(seed) => match state.store.seed_reserved(&seed).await {
                Ok(Seeded::NotReserved) => Err(format!(
                    "an enrolled site that is not reserved holds {}",
                    seed.site_name
                )),
                Ok(Seeded::Older) => Err(format!(
                    "the mounted seed for {} is generation {}, older than the one applied: a rollback or a \
                     replay of the seeds Secret",
                    seed.site_name, seed.generation
                )),
                Ok(seeded) => {
                    log_seeded(&seed, &seeded);
                    Ok(())
                },
                Err(error) => Err(error.to_string()),
            },
            Err(error) => Err(error.to_string()),
        };
        match applied {
            Ok(()) => drop(reported.remove(&stem)),
            Err(problem) if reported.get(&stem) != Some(&problem) => {
                tracing::warn!(seed = %stem, %problem, "reserved site seed not applied");
                reported.insert(stem.clone(), problem.clone());
                warned.push((stem, problem));
            },
            Err(_) => {},
        }
    }
    warned
}

/// Record what a seed did to a reserved site's record.
#[expect(clippy::cognitive_complexity, reason = "one tracing call per outcome")]
fn log_seeded(seed: &SeedRecord, seeded: &Seeded) {
    let (site, new_key, generation) = (&seed.site_name, &seed.key_sha256, seed.generation);
    match seeded {
        Seeded::Registered => {
            tracing::info!(
                site,
                new_key,
                generation,
                "reserved site registered from its bootstrap seed"
            );
        },
        Seeded::Reset { replaced_key } => tracing::info!(
            site,
            old_key = %replaced_key,
            new_key,
            generation,
            "reserved site record reset by a newer bootstrap seed"
        ),
        Seeded::Acknowledged => {
            tracing::info!(
                site,
                generation,
                "reserved site seed names the key its record holds; generation recorded"
            );
        },
        Seeded::Unchanged | Seeded::Older | Seeded::NotReserved => {
            tracing::debug!(site, generation, "reserved site seed already applied");
        },
    }
}

/// Lowercase hex.
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// Bytes from even-length lowercase or uppercase hex.
fn unhex(text: &str) -> Option<Vec<u8>> {
    if !text.len().is_multiple_of(2) {
        return None;
    }
    (0..text.len())
        .step_by(2)
        .map(|at| {
            text.get(at..at.checked_add(2)?)
                .and_then(|pair| u8::from_str_radix(pair, 16).ok())
        })
        .collect()
}

#[cfg(test)]
#[expect(clippy::expect_used, reason = "tests")]
mod tests {
    use super::*;

    fn seed() -> SeedRecord {
        SeedRecord {
            site_name: "hub".to_owned(),
            key_sha256: "ab".repeat(32),
            generation: 7,
            issued_at: time::OffsetDateTime::from_unix_timestamp(1_700_000_000).expect("time"),
        }
    }

    #[test]
    fn a_signed_seed_verifies_and_round_trips() {
        let ca = certs::generate_ca("grid-ca").expect("ca");
        let (body, signature) = sign(&seed(), &ca).expect("sign");
        assert_eq!(verified(&body, &signature, &ca.cert_pem), Ok(seed()));
    }

    #[test]
    fn a_seed_is_trusted_only_under_the_ca_signature() {
        let ca = certs::generate_ca("grid-ca").expect("ca");
        let other = certs::generate_ca("grid-ca").expect("other");
        let (body, signature) = sign(&seed(), &ca).expect("sign");
        let forged = body.replace("generation=7", "generation=8");
        assert_eq!(
            verified(&forged, &signature, &ca.cert_pem),
            Err(SeedError::BadSignature),
            "an edited seed"
        );
        let (_, foreign) = sign(&seed(), &other).expect("sign");
        assert_eq!(
            verified(&body, &foreign, &ca.cert_pem),
            Err(SeedError::BadSignature),
            "another CA"
        );
        assert_eq!(
            verified(&body, "zz", &ca.cert_pem),
            Err(SeedError::BadSignature),
            "not hex"
        );
    }

    #[test]
    fn only_the_exact_form_parses() {
        assert_eq!(parse(&text(&seed())), Ok(seed()));
        for bad in [
            "grid-reserved-seed-v1\nsite=hub\nkey=ab\ngeneration=1\nissued=1\n".to_owned(),
            text(&seed()).replace("issued=1700000000", "issued=soon"),
            text(&seed()).replace("issued=1700000000", "issued=+1700000000"),
            text(&seed()).replace("generation=7", "generation=07"),
            text(&seed()).replace("site=hub", "site=Hub!"),
            text(&seed()).replace("generation=7", "generation=-1"),
            format!("{}extra=1\n", text(&seed())),
            text(&seed()).trim_end().to_owned(),
            text(&seed()).replace(SEED_V1, "grid-reserved-seed-v2"),
        ] {
            assert_eq!(parse(&bad), Err(SeedError::Malformed), "{bad:?}");
        }
    }

    #[test]
    fn seeds_load_from_a_directory_and_a_bad_one_is_reported() {
        let ca = certs::generate_ca("grid-ca").expect("ca");
        let dir = std::env::temp_dir().join(format!("seeds-{}", uuid::Uuid::new_v4().simple()));
        std::fs::create_dir_all(&dir).expect("dir");
        let (body, signature) = sign(&seed(), &ca).expect("sign");
        std::fs::write(dir.join("hub.seed"), &body).expect("seed");
        std::fs::write(dir.join("hub.sig"), &signature).expect("sig");
        std::fs::write(dir.join("east.seed"), &body).expect("seed");
        std::fs::write(dir.join("east.sig"), "00").expect("sig");
        let loaded = load_dir(&dir, &ca.cert_pem);
        assert_eq!(
            loaded,
            vec![
                ("east".to_owned(), Err(SeedError::BadSignature)),
                ("hub".to_owned(), Ok(seed())),
            ]
        );
        assert!(
            load_dir(&dir.join("absent"), &ca.cert_pem).is_empty(),
            "no directory, no seeds"
        );
    }
}
