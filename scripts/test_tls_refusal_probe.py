"""The negative mTLS assertion requires peer evidence, never just a reset."""

import contextlib
import socket
import subprocess
import sys
import tempfile
import threading
from pathlib import Path
import ssl
import unittest
from unittest import mock

from scripts import tls_refusal_probe as subject


class RefusalProbeTests(unittest.TestCase):
    def setUp(self):
        self.context = mock.MagicMock()
        self.connection = self.context.wrap_socket.return_value.__enter__.return_value
        self.factory = mock.patch.object(subject.ssl, "create_default_context", return_value=self.context)
        self.connect = mock.patch.object(subject.socket, "create_connection")
        self.factory.start()
        self.connect_mock = self.connect.start()
        self.addCleanup(self.factory.stop)
        self.addCleanup(self.connect.stop)

    def result(self):
        return subject.probe("127.0.0.1", 8443, "site.grid.internal", "ca.crt")

    def test_peer_authentication_alert_after_tls13_handshake_is_confirmed(self):
        error = ssl.SSLError(1, "tlsv13 alert certificate required")
        error.reason = "TLSV13_ALERT_CERTIFICATE_REQUIRED"
        self.connection.recv.side_effect = error
        self.assertTrue(self.result()[0])
        self.connection.send.assert_not_called()
        self.connect_mock.assert_called_once_with(("127.0.0.1", 8443), timeout=15)
        self.context.wrap_socket.assert_called_once()
        self.assertEqual(self.context.wrap_socket.call_args.kwargs["server_hostname"], "site.grid.internal")

    def test_generic_handshake_failure_is_not_authentication_evidence(self):
        error = ssl.SSLError(1, "handshake failure")
        error.reason = "SSLV3_ALERT_HANDSHAKE_FAILURE"
        self.context.wrap_socket.side_effect = error
        self.assertFalse(self.result()[0])

    def test_certificate_and_key_follow_the_identity_under_test(self):
        subject.probe("127.0.0.1", 8443, "site", "ca.crt", "rogue.crt", "rogue.key")
        self.context.load_cert_chain.assert_called_once_with("rogue.crt", "rogue.key")

    def test_resets_timeouts_eof_and_application_data_are_not_authentication_evidence(self):
        for result in [b"", b"H", ConnectionResetError("reset"), socket.timeout("read timeout")]:
            with self.subTest(result=result):
                self.connection.recv.side_effect = result if isinstance(result, Exception) else None
                self.connection.recv.return_value = result
                self.assertFalse(self.result()[0])

    def test_server_verification_protocol_errors_and_unexpected_eof_are_not_refusals(self):
        for reason in ["CERTIFICATE_VERIFY_FAILED", "TLSV1_ALERT_PROTOCOL_VERSION", "UNEXPECTED_EOF_WHILE_READING"]:
            with self.subTest(reason=reason):
                error = ssl.SSLError(1, reason)
                error.reason = reason
                self.context.wrap_socket.side_effect = error
                self.assertFalse(self.result()[0])

    def test_connection_and_configuration_failures_are_not_refusals(self):
        self.connect_mock.side_effect = ConnectionRefusedError("offline")
        self.assertFalse(self.result()[0])
        self.connect_mock.side_effect = None
        self.context.load_cert_chain.side_effect = FileNotFoundError("missing identity")
        self.assertFalse(subject.probe("127.0.0.1", 8443, "site", "ca.crt", "missing", "missing")[0])


class RealTlsTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.work = tempfile.TemporaryDirectory()
        cls.cert = str(Path(cls.work.name) / "cert.pem")
        cls.key = str(Path(cls.work.name) / "key.pem")
        subprocess.run([
            "openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1",
            "-subj", "/CN=localhost", "-addext", "subjectAltName=DNS:localhost",
            "-keyout", cls.key, "-out", cls.cert,
        ], check=True, capture_output=True)

    @classmethod
    def tearDownClass(cls):
        cls.work.cleanup()

    @contextlib.contextmanager
    def server(self, version):
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.minimum_version = version
        context.maximum_version = version
        context.load_cert_chain(self.cert, self.key)
        context.load_verify_locations(self.cert)
        context.verify_mode = ssl.CERT_REQUIRED
        failures = []
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", 0))
            listener.listen()
            listener.settimeout(3)
            def serve():
                try:
                    raw, _ = listener.accept()
                    with raw:
                        raw.settimeout(2)
                        try:
                            with context.wrap_socket(raw, server_side=True) as secured:
                                secured.sendall(b"H")
                        except ssl.SSLError:
                            pass
                except Exception as error:
                    failures.append(error)
            thread = threading.Thread(target=serve)
            thread.start()
            try:
                yield listener.getsockname()[1]
            finally:
                thread.join(4)
                self.assertFalse(thread.is_alive(), "TLS fixture must stop")
                self.assertEqual(failures, [])

    def test_real_tls13_missing_client_certificate_has_an_explicit_authentication_alert(self):
        with self.server(ssl.TLSVersion.TLSv1_3) as port:
            refused, detail = subject.probe("127.0.0.1", port, "localhost", self.cert, timeout=2)
            self.assertTrue(refused, detail)

    def test_real_tls12_generic_alert_is_inconclusive_for_the_independent_probe(self):
        with self.server(ssl.TLSVersion.TLSv1_2) as port:
            refused, detail = subject.probe("127.0.0.1", port, "localhost", self.cert, timeout=2)
            self.assertFalse(refused, detail)

    def test_real_cli_confirms_the_peer_alert(self):
        with self.server(ssl.TLSVersion.TLSv1_3) as port:
            result = subprocess.run([
                sys.executable, str(Path(subject.__file__)), "--address", "127.0.0.1",
                "--port", str(port), "--server-name", "localhost", "--ca", self.cert,
            ], timeout=5, capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn("certificate required", result.stderr.lower())

    def test_real_accepted_client_is_not_mistaken_for_a_refusal(self):
        with self.server(ssl.TLSVersion.TLSv1_3) as port:
            refused, detail = subject.probe("127.0.0.1", port, "localhost", self.cert, self.cert, self.key, timeout=2)
            self.assertFalse(refused, detail)
            self.assertEqual(detail, "application data received")

    def test_real_hostname_verification_failure_is_not_mistaken_for_peer_refusal(self):
        with self.server(ssl.TLSVersion.TLSv1_3) as port:
            refused, detail = subject.probe("127.0.0.1", port, "wrong-hostname", self.cert, timeout=2)
            self.assertFalse(refused, detail)


if __name__ == "__main__":
    unittest.main()
