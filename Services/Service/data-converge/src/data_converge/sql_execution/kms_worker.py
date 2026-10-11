"""One-shot platform SDK adapter; invoked only by the credential provider."""

import base64
import os
import sys

from data_converge.sql_execution.credentials import FRAME_PREFIX, MAX_SECRET_BYTES


def main():
    # Reserve a private protocol stream before suppressing SDK Python/native logs.
    # This process has no application work to resume after SDK initialization.
    try:
        with os.fdopen(os.dup(sys.stdout.fileno()), "wb") as protocol:
            with open(os.devnull, "wb") as sink:
                os.dup2(sink.fileno(), 1)
                os.dup2(sink.fileno(), 2)
                payload = sys.stdin.buffer.read(MAX_SECRET_BYTES + 1)
                if not 0 < len(payload) <= MAX_SECRET_BYTES:
                    return 1
                ciphertext = payload.decode("utf-8")

                # Supplied by the deployment platform, not a public PyPI package.
                from dsf.crypto.xms_crypto import xms_decrypt

                plaintext = xms_decrypt(ciphertext)
                if not isinstance(plaintext, str) or not 0 < len(plaintext) <= MAX_SECRET_BYTES:
                    return 1
                encoded = plaintext.encode("utf-8")
                if len(encoded) > MAX_SECRET_BYTES:
                    return 1
                protocol.write(FRAME_PREFIX + base64.b64encode(encoded) + b"\n")
        return 0
    except BaseException:
        # SDK exceptions may contain credential material. Parent reports a stable
        # failure code; never send exceptions or raw SDK diagnostics over stderr.
        return 1


if __name__ == "__main__":
    # Platform SDKs may create non-daemon threads; do not keep this helper alive.
    os._exit(main())
