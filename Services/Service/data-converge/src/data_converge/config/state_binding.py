"""Version-fence ledgers whose old execution facts have been retired."""

import hashlib


def fenced_binding(fingerprint: str) -> str:
    # Old binaries only calculate the original digest, so must reject this ledger.
    return hashlib.sha256(('legacy-retired-v1:' + fingerprint).encode('ascii')).hexdigest()


def ledger_binding(store, fingerprint: str) -> str:
    return fenced_binding(fingerprint) if store.get_meta('retirement_required') is not None else fingerprint
