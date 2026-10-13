"""Company PSM adapter for the Redis authentication exchange.

PSF owns key initialization, root-material XOR, PBKDF2 and the V1 wire format.
Only Redis's key layout and AES-GCM/zero-padding differences live here. The
deployment supplies ``psf`` and ``Crypto``; offline configuration needs neither.
"""

import re
from pathlib import Path

KEY_BLOCK_SIZE = 1024
MAX_KEY_FILE_BYTES = 1024 * 1024
MAX_TEXT_BYTES = 1024 * 1024
GCM_TAG_BYTES = 16


class RedisKeyStructure:
    """The Redis 1024-byte key record expected by the existing PSF methods."""

    def __init__(self, key_block):
        if len(key_block) != KEY_BLOCK_SIZE:
            raise ValueError("REDIS_PSM_KEY_INVALID") from None
        buf = memoryview(key_block)
        self.version = buf[0]
        self.timestamp = bytes(buf[1:4])
        self.identity = int.from_bytes(buf[4:8], "big")
        self.kdf_type = int.from_bytes(buf[8:12], "big")
        self.iteration = int.from_bytes(buf[12:16], "big")
        self.key_len = int.from_bytes(buf[16:20], "big")
        self.salt = bytes(buf[20:52])
        self.material_len = int.from_bytes(buf[52:56], "big")
        self.is_encrypt = buf[56] == 1
        # In the company Redis format key_len is also the material byte offset.
        if (self.key_len not in (128, 256) or self.iteration <= 0
                or self.material_len <= 0 or self.key_len + self.material_len > 992
                or buf[56] not in (0, 1) or bytes(buf[57:63]) != b"AESGCM"):
            raise ValueError("REDIS_PSM_KEY_INVALID") from None
        self.aes_type = "AESGCM"
        self.material = bytes(buf[self.key_len:self.key_len + self.material_len])
        self.checksum = bytes(buf[992:1024])

    def get_material_bytes(self):
        return bytearray(self.material)

    def get_material(self, index):
        return self.material[index]

    def get_salt(self):
        return self.salt

    def get_checksum(self):
        return self.checksum

    def get_kdf_type(self):
        return "PBKDF2WithHmacSHA256"

    def get_algo_type(self):
        from psf.crypto.psm.constant.enum_algo_type import EnumAlgoType

        return EnumAlgoType.AES_GCM_LONG


def _load_key_bytes(key_dict, key_bytes):
    if not 0 < len(key_bytes) <= MAX_KEY_FILE_BYTES or len(key_bytes) % KEY_BLOCK_SIZE:
        raise ValueError("REDIS_PSM_KEY_INVALID") from None
    for offset in range(0, len(key_bytes), KEY_BLOCK_SIZE):
        key = RedisKeyStructure(key_bytes[offset:offset + KEY_BLOCK_SIZE])
        if key.identity in key_dict:
            raise ValueError("REDIS_PSM_KEY_INVALID") from None
        key_dict[key.identity] = key


def _validate_key_file(path):
    # Bound the initial read before invoking the platform's normal initializer.
    # load_key_bytes also checks the data the SDK actually loads.
    with Path(path).open("rb") as stream:
        raw = stream.read(MAX_KEY_FILE_BYTES + 1)
    _load_key_bytes({}, raw)


def _parse_cipher(raw, cipher_structure, vector_size):
    if len(raw) < cipher_structure.CIPHER_HEADER_LEN + GCM_TAG_BYTES:
        raise ValueError("REDIS_PSM_CIPHER_INVALID") from None
    value = cipher_structure(raw)
    if (value.version != cipher_structure.VERSION
            or len(value.i_vector) != vector_size
            or len(value.cipher_text) < GCM_TAG_BYTES):
        raise ValueError("REDIS_PSM_CIPHER_INVALID") from None
    return value


def _make_decryptor_type(aes, psm_decryptor, psm_encryptor, cipher_structure):
    class RedisKeyDecryptor(psm_decryptor):
        load_key_bytes = staticmethod(_load_key_bytes)

        def get_raw_shared_key(self, key_id):
            shared_key = self.get_shared_key(key_id)
            if not shared_key.is_encrypt:
                return shared_key.get_material_bytes()
            value = _parse_cipher(shared_key.get_material_bytes(), cipher_structure, psm_encryptor.I_VECTOR_SIZE)
            root_key = self.get_root_key(value.key_id)
            root_material = self.get_xor_hex_string(root_key.get_material_bytes(), root_key.material_len)
            return self.pbkdf2_and_decrypt(
                root_material, root_key.key_len, bytes(value.cipher_text),
                value.i_vector, root_key, root_key.get_algo_type())

        @staticmethod
        def decrypt_cipher(key, i_vector, data, algo_type):
            if (algo_type.value["id"] != 4 or len(i_vector) != psm_encryptor.I_VECTOR_SIZE
                    or len(data) < GCM_TAG_BYTES):
                raise ValueError("REDIS_PSM_CIPHER_INVALID") from None
            cipher = aes.new(key, aes.MODE_GCM, nonce=bytes(i_vector))
            return cipher.decrypt_and_verify(bytes(data[:-GCM_TAG_BYTES]), bytes(data[-GCM_TAG_BYTES:]))

        def decrypt(self, cipher_text):
            if not self.is_init:
                raise ValueError("REDIS_PSM_NOT_INITIALIZED") from None
            value = _parse_cipher(self.parse_hex_string(cipher_text), cipher_structure, psm_encryptor.I_VECTOR_SIZE)
            shared_key = self.get_shared_key(value.key_id)
            material = self.get_hex_string(self.get_raw_shared_key(value.key_id))
            plaintext = self.pbkdf2_and_decrypt(
                material, shared_key.key_len, bytes(value.cipher_text),
                value.i_vector, shared_key, shared_key.get_algo_type())
            # The Redis peer pads its plaintext with zero bytes to 16 bytes.
            # Work-key material above remains binary and must not be stripped.
            return bytes(plaintext).rstrip(b"\0").decode("utf-8")

    return RedisKeyDecryptor


def _make_encryptor_type(aes, psm_decryptor, psm_encryptor, cipher_structure, random_bytes, redis_decryptor):
    class RedisKeyEncryptor(psm_encryptor):
        def __init__(self):
            super().__init__()
            self.decryptor = redis_decryptor()

        def encrypt(self, plaintext):
            key_id = self.decryptor.get_first_shared_key_id()
            shared_key = self.decryptor.get_shared_key(key_id)
            material = self.decryptor.get_hex_string(self.decryptor.get_raw_shared_key(key_id))
            nonce = random_bytes(self.I_VECTOR_SIZE)
            if len(nonce) != self.I_VECTOR_SIZE or shared_key.get_algo_type().value["id"] != 4:
                raise ValueError("REDIS_PSM_CIPHER_INVALID") from None
            key = self.decryptor.generate_pbkdf2_hashed_bytes(
                material, shared_key.get_salt(), shared_key.key_len,
                shared_key.iteration, shared_key.kdf_type)
            cipher = aes.new(key, aes.MODE_GCM, nonce=bytes(nonce))
            ciphertext, tag = cipher.encrypt_and_digest(plaintext.encode("utf-8"))
            value = (cipher_structure.Builder().update_key_id(key_id)
                     .update_i_vector(nonce).update_cipher_text(ciphertext + tag))
            return psm_decryptor.get_hex_string(cipher_structure.marshal(value)).upper()

    return RedisKeyEncryptor


class _Codec:
    def __init__(self, encryptor, cipher_structure):
        self._encryptor = encryptor
        self._cipher_structure = cipher_structure

    def encrypt(self, plaintext):
        try:
            if (not isinstance(plaintext, str) or "\0" in plaintext
                    or not 0 < len(plaintext.encode("utf-8")) <= MAX_TEXT_BYTES):
                raise ValueError("invalid plaintext")
            return self._encryptor.encrypt(plaintext)
        except Exception:
            raise ValueError("REDIS_PSM_ENCRYPT_FAILED") from None

    def decrypt(self, ciphertext):
        try:
            limit = 2 * (MAX_TEXT_BYTES + self._cipher_structure.CIPHER_HEADER_LEN + GCM_TAG_BYTES + 15)
            if not isinstance(ciphertext, str) or not 0 < len(ciphertext) <= limit:
                raise ValueError("invalid ciphertext")
            ciphertext = ciphertext.strip()
            if len(ciphertext) % 2 or re.fullmatch(r"[0-9a-fA-F]+", ciphertext) is None:
                raise ValueError("invalid ciphertext")
            plaintext = self._encryptor.decryptor.decrypt(ciphertext)
            if not plaintext or "\0" in plaintext or len(plaintext.encode("utf-8")) > MAX_TEXT_BYTES:
                raise ValueError("invalid plaintext")
            return plaintext
        except Exception:
            raise ValueError("REDIS_PSM_DECRYPT_FAILED") from None


def create_psm_codec(root_key_file: str, work_key_file: str):
    """Initialize once and return encrypt(str)->hex / decrypt(hex)->str methods.

    SDK errors never escape with their messages or displayed exception chains.
    PSF's own logging policy remains the deployment platform's responsibility.
    """
    try:
        _validate_key_file(root_key_file)
        _validate_key_file(work_key_file)

        from Crypto.Cipher import AES
        from psf.crypto.psm.psm_key_decryptor import PsmKeyDecryptor
        from psf.crypto.psm.psm_key_encryptor import PsmKeyEncryptor
        from psf.crypto.psm.structure.cipher_structure_v1 import CipherStructureV1
        from psf.security_api.random_security import gen_random_bytes

        decryptor_type = _make_decryptor_type(AES, PsmKeyDecryptor, PsmKeyEncryptor, CipherStructureV1)
        encryptor_type = _make_encryptor_type(
            AES, PsmKeyDecryptor, PsmKeyEncryptor, CipherStructureV1, gen_random_bytes, decryptor_type)
        encryptor = encryptor_type()
        encryptor.init_encryptor(root_key_file, work_key_file)
        return _Codec(encryptor, CipherStructureV1)
    except Exception:
        raise ValueError("REDIS_PSM_INIT_FAILED") from None
