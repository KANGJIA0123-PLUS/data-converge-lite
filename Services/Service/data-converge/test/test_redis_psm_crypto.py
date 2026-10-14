"""Redis PSM adapter contracts with synthetic keys, a test SDK and real AES-GCM.

The fixture models only the PSF interfaces shown in the company reference. Its
cipher header and XOR component are synthetic, not production format vectors.
"""

import builtins
import hashlib
import importlib
import os
import struct
import sys
import types
from enum import Enum

import pytest


def key_block(identity=202, material=b"synthetic work material", key_len=128, encrypted=False):
    block = bytearray(1024)
    struct.pack_into(">B3xIIII", block, 0, 1, identity, 1, 1000, key_len)
    block[20:52] = bytes(range(32))
    struct.pack_into(">I", block, 52, len(material))
    block[56] = int(encrypted)
    block[57:63] = b"AESGCM"
    block[key_len:key_len + len(material)] = material
    return bytes(block)


class Algorithm(Enum):
    AES_GCM_LONG = {"id": 4}


class Cipher:
    VERSION = 1
    CIPHER_HEADER_LEN = 21

    def __init__(self, raw):
        self.version = raw[0]
        self.key_id = int.from_bytes(raw[1:5], "big")
        self.i_vector = bytes(raw[5:21])
        self.cipher_text = bytearray(raw[21:])

    class Builder:
        def update_key_id(self, value):
            self.key_id = value
            return self

        def update_i_vector(self, value):
            self.i_vector = value
            return self

        def update_cipher_text(self, value):
            self.cipher_text = value
            return self

    @staticmethod
    def marshal(builder):
        return b"\x01" + builder.key_id.to_bytes(4, "big") + builder.i_vector + builder.cipher_text


class Decryptor:
    def __init__(self):
        self.is_init = False
        self.root_key_dict = {}
        self.shared_key_dict = {}

    def init_decryptor(self, root_path, work_path):
        for path, target in ((root_path, self.root_key_dict), (work_path, self.shared_key_dict)):
            with open(path, "rb") as stream:
                self.load_key_bytes(target, stream.read())
        self.is_init = True

    def get_shared_key(self, identity):
        return self.shared_key_dict[identity]

    def get_root_key(self, identity):
        return self.root_key_dict[identity]

    def get_first_shared_key_id(self):
        return next(iter(self.shared_key_dict))

    @staticmethod
    def get_hex_string(value):
        return bytes(value).hex()

    @staticmethod
    def parse_hex_string(value):
        return bytearray.fromhex(value)

    @staticmethod
    def copy_of_range(value, start, end):
        return bytearray(value[start:end])

    @staticmethod
    def get_xor_hex_string(value, length):
        return bytes(byte ^ 0xA5 for byte in value[:length]).hex()

    @staticmethod
    def generate_pbkdf2_hashed_bytes(material, salt, key_len, iteration, kdf_type):
        if kdf_type != 1:
            raise ValueError("synthetic private SDK exception")
        return hashlib.pbkdf2_hmac("sha256", material.encode(), salt, iteration, key_len // 8)

    def pbkdf2_and_decrypt(self, material, key_len, data, nonce, structure, algorithm):
        key = self.generate_pbkdf2_hashed_bytes(material, structure.get_salt(), key_len,
                                              structure.iteration, structure.kdf_type)
        return self.decrypt_cipher(key, nonce, data, algorithm)


class Encryptor:
    I_VECTOR_SIZE = 16

    def init_encryptor(self, root_path, work_path):
        self.decryptor.init_decryptor(root_path, work_path)


@pytest.fixture
def sdk(monkeypatch):
    AES = pytest.importorskip("Crypto.Cipher.AES")

    exports = {
        "psf.crypto.psm.psm_key_decryptor": {"PsmKeyDecryptor": Decryptor},
        "psf.crypto.psm.psm_key_encryptor": {"PsmKeyEncryptor": Encryptor},
        "psf.crypto.psm.structure.cipher_structure_v1": {"CipherStructureV1": Cipher},
        "psf.crypto.psm.constant.enum_algo_type": {"EnumAlgoType": Algorithm},
        "psf.security_api.random_security": {"gen_random_bytes": os.urandom},
    }
    for module_name, values in exports.items():
        pieces = module_name.split(".")
        for count in range(1, len(pieces) + 1):
            name = ".".join(pieces[:count])
            if name not in sys.modules:
                module = types.ModuleType(name)
                module.__path__ = []
                monkeypatch.setitem(sys.modules, name, module)
        for name, value in values.items():
            monkeypatch.setattr(sys.modules[module_name], name, value, raising=False)
    return types.SimpleNamespace(AES=AES, Decryptor=Decryptor, Cipher=Cipher)


@pytest.fixture
def keys(tmp_path):
    root = tmp_path / "synthetic-root.key"
    work = tmp_path / "synthetic-work.key"
    root.write_bytes(key_block(101, b"synthetic root material"))
    work.write_bytes(key_block())
    return root, work


def create(keys):
    module = importlib.import_module("data_converge.infrastructure.redis.crypto")
    return module.create_psm_codec(*(str(path) for path in keys))


def external_cipher(sdk, plaintext, identity=202, material=b"synthetic work material", nonce=b"n" * 16):
    # Independent test peer: bypass all production adapter methods.
    key = hashlib.pbkdf2_hmac("sha256", material.hex().encode(), bytes(range(32)), 1000, 16)
    encrypted, tag = sdk.AES.new(key, sdk.AES.MODE_GCM, nonce=nonce).encrypt_and_digest(plaintext)
    return (b"\x01" + identity.to_bytes(4, "big") + nonce + encrypted + tag).hex()


def assert_safe_failure(call, code):
    with pytest.raises(ValueError) as caught:
        call()
    assert str(caught.value) == code
    assert caught.value.__suppress_context__
    assert caught.value.__cause__ is None


def test_module_import_does_not_load_optional_crypto_dependencies(monkeypatch):
    original = builtins.__import__

    def restricted(name, *args, **kwargs):
        if name.split(".")[0] in {"psf", "Crypto"}:
            raise AssertionError("optional dependency imported before use")
        return original(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", restricted)
    sys.modules.pop("data_converge.infrastructure.redis.crypto", None)
    module = importlib.import_module("data_converge.infrastructure.redis.crypto")
    assert callable(module.create_psm_codec)


def test_codec_preserves_utf8_whitespace_and_uses_fresh_authenticated_nonce(sdk, keys):
    codec = create(keys)
    message = " 客户端@服务端@密码 \n"
    first, second = codec.encrypt(message), codec.encrypt(message)
    assert first != second
    assert first == first.upper()
    raw = bytes.fromhex(first)
    assert raw[:5] == b"\x01\x00\x00\x00\xca"
    key = hashlib.pbkdf2_hmac("sha256", b"synthetic work material".hex().encode(), bytes(range(32)), 1000, 16)
    assert sdk.AES.new(key, sdk.AES.MODE_GCM, nonce=raw[5:21]).decrypt_and_verify(raw[21:-16], raw[-16:]) == message.encode()
    assert codec.decrypt(first) == message


@pytest.mark.parametrize("length", [1, 15, 16, 17, 31, 32])
def test_decrypts_server_zero_padding_at_aes_block_boundaries(sdk, keys, length):
    payload = b"x" * length
    padded = payload + b"\0" * ((-length) % 16)
    assert create(keys).decrypt(external_cipher(sdk, padded)) == payload.decode()


def test_unwraps_work_key_with_psf_root_xor_and_kdf(sdk, keys):
    root_material = b"synthetic root material"
    work_material = b"wrapped synthetic work material\0\0"
    key = hashlib.pbkdf2_hmac("sha256", bytes(byte ^ 0xA5 for byte in root_material).hex().encode(),
                              bytes(range(32)), 1000, 16)
    nonce = b"r" * 16
    ciphertext, tag = sdk.AES.new(key, sdk.AES.MODE_GCM, nonce=nonce).encrypt_and_digest(work_material)
    wrapped = b"\x01" + (101).to_bytes(4, "big") + nonce + ciphertext + tag
    keys[1].write_bytes(key_block(material=wrapped, encrypted=True))
    codec = create(keys)
    assert codec.decrypt(external_cipher(sdk, b"peer response", material=work_material)) == "peer response"
    assert codec.decrypt(codec.encrypt("client response")) == "client response"


@pytest.mark.parametrize("data", [b"", b"x" * 1023, b"x" * 1025, key_block() * 2, b"x" * (1024 * 1024 + 1)],
                         ids=["empty", "truncated", "partial-extra-block", "duplicate-id", "oversized"])
def test_rejects_empty_truncated_duplicate_or_oversized_key_files(sdk, keys, data):
    keys[1].write_bytes(data)
    assert_safe_failure(lambda: create(keys), "REDIS_PSM_INIT_FAILED")


@pytest.mark.parametrize("offset,data", [(16, (64).to_bytes(4, "big")), (52, b"\0" * 4),
                                           (52, (865).to_bytes(4, "big")), (56, b"\x02"),
                                           (57, b"AESCBC"), (12, b"\0" * 4)])
def test_rejects_invalid_redis_key_lengths_flags_algorithm_and_iterations(sdk, keys, offset, data):
    block = bytearray(key_block())
    block[offset:offset + len(data)] = data
    keys[1].write_bytes(block)
    assert_safe_failure(lambda: create(keys), "REDIS_PSM_INIT_FAILED")


@pytest.mark.parametrize("key_len,length", [(128, 864), (256, 736)])
def test_accepts_material_ending_at_checksum_boundary(sdk, keys, key_len, length):
    keys[1].write_bytes(key_block(material=b"m" * length, key_len=key_len))
    codec = create(keys)
    assert codec.decrypt(codec.encrypt("boundary")) == "boundary"


@pytest.mark.parametrize("mutation", ["version", "identity", "nonce", "ciphertext", "tag", "truncated"])
def test_rejects_unsupported_version_unknown_key_and_gcm_tampering(sdk, keys, mutation):
    raw = bytearray.fromhex(external_cipher(sdk, b"server@client"))
    if mutation == "truncated":
        raw = raw[:30]
    else:
        offset = {"version": 0, "identity": 4, "nonce": 5, "ciphertext": 21, "tag": -1}[mutation]
        raw[offset] ^= 1
    assert_safe_failure(lambda: create(keys).decrypt(raw.hex()), "REDIS_PSM_DECRYPT_FAILED")


@pytest.mark.parametrize("plaintext", ["", "contains\0nul", "x" * (1024 * 1024 + 1), None],
                         ids=["empty", "embedded-nul", "oversized", "not-text"])
def test_rejects_invalid_plaintext_before_encryption(sdk, keys, plaintext):
    assert_safe_failure(lambda: create(keys).encrypt(plaintext), "REDIS_PSM_ENCRYPT_FAILED")


def test_accepts_plaintext_at_utf8_byte_limit(sdk, keys):
    codec = create(keys)
    plaintext = "\u00e9" * (1024 * 512)
    assert codec.decrypt(codec.encrypt(plaintext)) == plaintext
    assert_safe_failure(lambda: codec.encrypt(plaintext + "x"), "REDIS_PSM_ENCRYPT_FAILED")


@pytest.mark.parametrize("ciphertext", ["", "abc", "not hex", "01 23", "00" * (1024 * 1024 + 100), None],
                         ids=["empty", "odd-hex", "not-hex", "embedded-space", "oversized", "not-text"])
def test_rejects_invalid_ciphertext_before_sdk_parsing(sdk, keys, ciphertext):
    assert_safe_failure(lambda: create(keys).decrypt(ciphertext), "REDIS_PSM_DECRYPT_FAILED")


def test_rejects_incorrect_nonce_length_from_random_provider(sdk, keys, monkeypatch):
    monkeypatch.setattr(sys.modules["psf.security_api.random_security"], "gen_random_bytes", lambda size: b"short")
    assert_safe_failure(lambda: create(keys).encrypt("client@server@password"), "REDIS_PSM_ENCRYPT_FAILED")


def test_redacts_sdk_initialization_exception(sdk, keys, monkeypatch):
    def fail(*args):
        raise RuntimeError("synthetic private SDK exception")

    monkeypatch.setattr(sdk.Decryptor, "init_decryptor", fail)
    assert_safe_failure(lambda: create(keys), "REDIS_PSM_INIT_FAILED")


@pytest.mark.parametrize("dependency", ["psf", "Crypto"])
def test_missing_optional_dependency_has_a_redacted_initialization_error(keys, monkeypatch, dependency):
    original = builtins.__import__

    def unavailable(name, *args, **kwargs):
        if name.split(".")[0] == dependency:
            raise ImportError("synthetic private import path")
        return original(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", unavailable)
    assert_safe_failure(lambda: create(keys), "REDIS_PSM_INIT_FAILED")


@pytest.mark.parametrize("wrapped", [b"\x02" + b"x" * 60, b"\x01" + b"x" * 20])
def test_rejects_bad_encrypted_work_key_version_and_length(sdk, keys, wrapped):
    keys[1].write_bytes(key_block(material=wrapped, encrypted=True))
    codec = create(keys)
    assert_safe_failure(lambda: codec.encrypt("client response"), "REDIS_PSM_ENCRYPT_FAILED")


def test_missing_key_file_does_not_echo_its_path(sdk, keys):
    keys[1].unlink()
    assert_safe_failure(lambda: create(keys), "REDIS_PSM_INIT_FAILED")
