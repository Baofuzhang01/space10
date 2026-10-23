from cryptography.hazmat.primitives import padding
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding as asymmetric_padding
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.backends import default_backend
import base64
from hashlib import md5
import os

from uuid import uuid1
import hashlib


PASSWORD_V2_PREFIX = "v2:rsa-oaep-sha256:"
PASSWORD_PRIVATE_KEY_ENV = "PASSWORD_DECRYPTION_PRIVATE_KEY_B64"


def AES_Encrypt(data):
    key = b"u2oh6Vu^HWe4_AES"  # Convert to bytes
    iv = b"u2oh6Vu^HWe4_AES"  # Convert to bytes
    padder = padding.PKCS7(128).padder()
    padded_data = padder.update(data.encode("utf-8")) + padder.finalize()
    cipher = Cipher(algorithms.AES(key), modes.CBC(iv), backend=default_backend())
    encryptor = cipher.encryptor()
    encrypted_data = encryptor.update(padded_data) + encryptor.finalize()
    enctext = base64.b64encode(encrypted_data).decode("utf-8")
    return enctext


def _legacy_aes_decrypt(ciphertext_b64):
    """Decrypt the legacy database format. New passwords must not use it."""
    key = b"u2oh6Vu^HWe4_AES"
    iv = b"u2oh6Vu^HWe4_AES"
    cipher = Cipher(algorithms.AES(key), modes.CBC(iv), backend=default_backend())
    decryptor = cipher.decryptor()
    padded_data = decryptor.update(base64.b64decode(ciphertext_b64)) + decryptor.finalize()
    unpadder = padding.PKCS7(128).unpadder()
    plaintext = unpadder.update(padded_data) + unpadder.finalize()
    return plaintext.decode("utf-8")


def _load_password_private_key():
    encoded_key = os.environ.get(PASSWORD_PRIVATE_KEY_ENV, "").strip()
    if not encoded_key:
        raise RuntimeError(
            f"{PASSWORD_PRIVATE_KEY_ENV} is required to decrypt v2 passwords"
        )
    try:
        private_key_pem = base64.b64decode(encoded_key, validate=True)
        return serialization.load_pem_private_key(private_key_pem, password=None)
    except (ValueError, TypeError) as exc:
        raise RuntimeError(f"{PASSWORD_PRIVATE_KEY_ENV} is invalid") from exc


def encrypt_password_v2(plaintext, public_key_pem):
    """Encrypt a newly entered password with the website's public key."""
    public_key = serialization.load_pem_public_key(public_key_pem)
    ciphertext = public_key.encrypt(
        str(plaintext).encode("utf-8"),
        asymmetric_padding.OAEP(
            mgf=asymmetric_padding.MGF1(algorithm=hashes.SHA256()),
            algorithm=hashes.SHA256(),
            label=None,
        ),
    )
    return PASSWORD_V2_PREFIX + base64.b64encode(ciphertext).decode("ascii")


def _decrypt_password_v2(ciphertext):
    encoded_ciphertext = ciphertext[len(PASSWORD_V2_PREFIX):]
    try:
        encrypted_bytes = base64.b64decode(encoded_ciphertext, validate=True)
        plaintext = _load_password_private_key().decrypt(
            encrypted_bytes,
            asymmetric_padding.OAEP(
                mgf=asymmetric_padding.MGF1(algorithm=hashes.SHA256()),
                algorithm=hashes.SHA256(),
                label=None,
            ),
        )
    except RuntimeError:
        raise
    except (ValueError, TypeError) as exc:
        raise ValueError("invalid v2 password ciphertext") from exc
    return plaintext.decode("utf-8")


def AES_Decrypt(ciphertext_b64):
    """Decrypt stored passwords while keeping the legacy format compatible.

    Values prefixed with ``v2:rsa-oaep-sha256:`` use the private key supplied
    at runtime. Unprefixed values keep using the existing AES-CBC path so old
    users continue working until they update their password.
    """
    ciphertext = str(ciphertext_b64 or "")
    if ciphertext.startswith(PASSWORD_V2_PREFIX):
        return _decrypt_password_v2(ciphertext)
    return _legacy_aes_decrypt(ciphertext)


def resort(submit_info):
    return {key: submit_info[key] for key in sorted(submit_info.keys())}


def enc(submit_info):
    add = lambda x, y: x + y
    processed_info = resort(submit_info)
    needed = [
        add(add("[", key), "=" + value) + "]" for key, value in processed_info.items()
    ]
    pattern = "%sd`~7^/>N4!Q#){''"
    needed.append(add("[", pattern) + "]")
    seq = "".join(needed)
    return md5(seq.encode("utf-8")).hexdigest()


def generate_captcha_key(timestamp: int, captcha_type: str = "slide"):
    captcha_key = md5((str(timestamp) + str(uuid1())).encode("utf-8")).hexdigest()
    encoded_timestamp = (
        md5(
            (
                str(timestamp)
                + "42sxgHoTPTKbt0uZxPJ7ssOvtXr3ZgZ1"
                + captcha_type
                + captcha_key
            ).encode("utf-8")
        ).hexdigest()
        + ":"
        + str(int(timestamp) + 0x493E0)
    )
    return [captcha_key, encoded_timestamp]


def sort_dict_by_keys(dictionary):
    """将字典按键排序并返回新字典"""
    sorted_keys = sorted(dictionary.keys())
    sorted_dict = {key: dictionary[key] for key in sorted_keys}
    return sorted_dict


def verify_param(params, algorithm_value):
    """
    生成参数的MD5验证哈希值

    参数:
        params: 要验证的参数字典
        algorithm_value: 对应JavaScript中id为'algorithm'的元素值

    返回:
        计算得到的MD5哈希字符串
    """
    # 对参数字典按键排序
    sorted_params = sort_dict_by_keys(params)

    # 构建哈希字符串列表
    hash_list = []

    # 遍历排序后的参数，构建格式为 [key=value] 的字符串
    for key, value in sorted_params.items():
        # 确保值转换为字符串，与JavaScript行为一致
        hash_list.append(f"[{key}={str(value)}]")

    # 添加algorithm值
    hash_list.append(f"[{algorithm_value}]")

    # 连接所有元素形成最终字符串
    hash_string = "".join(hash_list)

    # 计算MD5哈希值（注意：Python的hashlib返回bytes，需要转换为十六进制字符串）
    md5_hash = hashlib.md5(hash_string.encode("utf-8")).hexdigest()

    return md5_hash
