# Test certificates

Provide disposable TLS identities and fixed interoperability fixtures.

## Ownership

| Responsibility | Component or value |
| --- | --- |
| Stored as | Generated test files, test-cluster Secrets, and checked-in certificate fixtures |
| Issuer | Test harness or fixture-generation code |
| Created / written by | Test harness or fixture maintainer |
| Consumers | Unit, integration, interoperability, and topology tests |
| Validity | General generators: CA 3,650 days, leaf 365 days. Individual tests can select other dates |
| Rotation owner | Test/environment maintainer |
| Renewal trigger | Explicit regeneration or test setup. No production renewal service |
| Consumer reload | Determined by the test, including rotation-specific tests |

## Lifecycle

- The environment harness reuses existing CA and matching identity files.
  Reusing files does not reset their validity or guarantee they remain
  unexpired.
- OpenSSL and rcgen interoperability fixtures use CA expiry 2036-09-27 and leaf
  expiry 2126-09-06. These fixed test dates are not deployment defaults.
- Negative TLS tests deliberately create expired or not-yet-valid certificates.

## References

[Environment harness] | [Generators] | [Fixtures]

[Environment harness]: ../../../crates/xtask/src/env/certs.rs
[Generators]: ../../../crates/certs/src/generate.rs
[Fixtures]: ../../../crates/certs/tests/fixtures/
