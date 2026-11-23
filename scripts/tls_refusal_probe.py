#!/usr/bin/env python3
"""Confirm a peer's client-authentication TLS alert without sending HTTP data."""

import argparse
import socket
import ssl
import sys

# RFC 8446 sections 4.4.2.4 and 6: missing or rejected client certificates.
# Generic handshake_failure is inconclusive: it also covers cipher negotiation.
AUTH_ALERTS = frozenset({
    "TLSV13_ALERT_CERTIFICATE_REQUIRED",
    "SSLV3_ALERT_BAD_CERTIFICATE",
    "SSLV3_ALERT_UNSUPPORTED_CERTIFICATE",
    "SSLV3_ALERT_CERTIFICATE_REVOKED",
    "SSLV3_ALERT_CERTIFICATE_EXPIRED",
    "SSLV3_ALERT_CERTIFICATE_UNKNOWN",
    "TLSV1_ALERT_UNKNOWN_CA",
    "TLSV1_ALERT_ACCESS_DENIED",
})


def probe(address, port, server_name, ca, cert=None, key=None, timeout=15):
    """Return confirmation only for an explicit peer client-authentication alert."""
    try:
        context = ssl.create_default_context(cafile=ca)
        if cert is not None:
            context.load_cert_chain(cert, key)
        with socket.create_connection((address, port), timeout=timeout) as raw:
            with context.wrap_socket(raw, server_hostname=server_name) as secured:
                # TLS 1.3 can finish on the client before the server rejects its
                # empty Certificate. Read the alert without racing an HTTP write.
                data = secured.recv(1)
                return False, "application data received" if data else "connection closed without a TLS alert"
    except ssl.SSLError as error:
        return getattr(error, "reason", None) in AUTH_ALERTS, str(error)
    except OSError as error:
        return False, str(error)


def main():
    """Run a certificate-validating probe with a connect/read timeout."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--address", required=True)
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--server-name", required=True)
    parser.add_argument("--ca", required=True)
    parser.add_argument("--cert")
    parser.add_argument("--key")
    args = parser.parse_args()
    if bool(args.cert) != bool(args.key):
        parser.error("--cert and --key must be supplied together")
    refused, detail = probe(args.address, args.port, args.server_name, args.ca, args.cert, args.key)
    print(detail, file=sys.stderr)
    return 0 if refused else 1


if __name__ == "__main__":
    sys.exit(main())
