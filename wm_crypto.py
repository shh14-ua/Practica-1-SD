#!/usr/bin/env python3
"""
wm_crypto.py: Utilidades criptográficas para la práctica Water Management (Release 2).
Proporciona:
  - Generación de credenciales seguras de registro (tokens).
  - Generación de claves de cifrado simétrico únicas por estación.
  - Cifrado y descifrado simétrico de mensajes (con soporte para Fernet/AES y fallback puro).
  - Generación automática de certificados SSL autofirmados para el canal seguro HTTPS de WM_Registry.
  - Generación del certificado TLS (con nombres SAN) del canal seguro WM_Central <-> WM_WS_M.
"""

import base64
import hashlib
import hmac
import os
import secrets
import ssl
from typing import Optional, Tuple

# Intentamos importar cryptography si está disponible
try:
    from cryptography.fernet import Fernet
    _HAS_CRYPTOGRAPHY = True
except ImportError:
    Fernet = None
    _HAS_CRYPTOGRAPHY = False


# ==============================================================================
# 1. GENERACIÓN DE CREDENCIALES Y CLAVES
# ==============================================================================

def generate_credential(prefix: str = "CRED") -> str:
    """Genera una credencial aleatoria segura para el registro de una WS."""
    random_part = secrets.token_hex(12).upper()
    return f"{prefix}-{random_part}"


def generate_symmetric_key() -> str:
    """
    Genera una clave de cifrado simétrico única (32 bytes en formato base64 URL-safe).
    Compatible con Fernet y con el cifrado simétrico integrado.
    """
    key_bytes = secrets.token_bytes(32)
    return base64.urlsafe_b64encode(key_bytes).decode("ascii")


# ==============================================================================
# 2. CIFRADO SIMÉTRICO (AES / Fernet con fallback a HMAC-SHA256 CTR)
# ==============================================================================

def _derive_keys(raw_key: bytes) -> Tuple[bytes, bytes]:
    """Deriva dos claves de 32 bytes (cifrado y autenticación) usando SHA-256."""
    enc_key = hashlib.sha256(raw_key + b":enc").digest()
    auth_key = hashlib.sha256(raw_key + b":auth").digest()
    return enc_key, auth_key


def _keystream_block(enc_key: bytes, nonce: bytes, counter: int) -> bytes:
    """Genera un bloque de flujo pseudoaleatorio usando HMAC-SHA256."""
    counter_bytes = counter.to_bytes(8, byteorder="big")
    return hmac.new(enc_key, nonce + counter_bytes, hashlib.sha256).digest()


def _xor_crypt(data: bytes, enc_key: bytes, nonce: bytes) -> bytes:
    """Cifra o descifra datos mediante XOR con un keystream basado en HMAC-SHA256 CTR."""
    output = bytearray(len(data))
    block_size = 32
    for offset in range(0, len(data), block_size):
        counter = offset // block_size
        stream = _keystream_block(enc_key, nonce, counter)
        chunk = data[offset:offset + block_size]
        for i in range(len(chunk)):
            output[offset + i] = chunk[i] ^ stream[i]
    return bytes(output)


def encrypt_message(key_str: str, plaintext: str) -> str:
    """
    Cifra un mensaje de texto plano con la clave simétrica especificada.
    Devuelve una cadena prefijada con 'ENC:' y el contenido en base64.
    """
    if not plaintext:
        return ""

    raw_data = plaintext.encode("utf-8")

    if _HAS_CRYPTOGRAPHY and Fernet is not None:
        try:
            f = Fernet(key_str.encode("ascii"))
            token = f.encrypt(raw_data)
            return "ENC:" + token.decode("ascii")
        except Exception:
            pass

    # Fallback criptográfico estándar (HMAC-SHA256 CTR + Tag de integridad)
    raw_key = base64.urlsafe_b64decode(key_str.encode("ascii"))
    enc_key, auth_key = _derive_keys(raw_key)

    nonce = secrets.token_bytes(16)
    ciphertext = _xor_crypt(raw_data, enc_key, nonce)
    tag = hmac.new(auth_key, nonce + ciphertext, hashlib.sha256).digest()

    package = nonce + tag + ciphertext
    b64 = base64.urlsafe_b64encode(package).decode("ascii")
    return f"ENC:{b64}"


def decrypt_message(key_str: str, cipher_str: str) -> Optional[str]:
    """
    Descifra un mensaje previamente cifrado con encrypt_message.
    Devuelve el texto plano original o None si la clave o el mensaje son inválidos.
    """
    if not cipher_str:
        return ""

    if not cipher_str.startswith("ENC:"):
        return None  # No es un mensaje cifrado reconocido

    payload = cipher_str[4:]

    if _HAS_CRYPTOGRAPHY and Fernet is not None:
        try:
            f = Fernet(key_str.encode("ascii"))
            raw = f.decrypt(payload.encode("ascii"), ttl=None)
            return raw.decode("utf-8", errors="replace")
        except Exception:
            pass

    try:
        package = base64.urlsafe_b64decode(payload.encode("ascii"))
        if len(package) < 48:  # 16 nonce + 32 tag
            return None

        nonce = package[:16]
        expected_tag = package[16:48]
        ciphertext = package[48:]

        raw_key = base64.urlsafe_b64decode(key_str.encode("ascii"))
        enc_key, auth_key = _derive_keys(raw_key)

        actual_tag = hmac.new(auth_key, nonce + ciphertext, hashlib.sha256).digest()
        if not hmac.compare_digest(expected_tag, actual_tag):
            return None  # Mensaje manipulado o clave incorrecta

        plaintext_bytes = _xor_crypt(ciphertext, enc_key, nonce)
        return plaintext_bytes.decode("utf-8", errors="replace")
    except Exception:
        return None


def is_encrypted(message: str) -> bool:
    """Indica si una cadena tiene el formato de un mensaje cifrado."""
    return isinstance(message, str) and message.startswith("ENC:")


# ==============================================================================
# 3. GENERACIÓN DE CERTIFICADOS SSL AUTO-FIRMADOS PARA HTTPS
# ==============================================================================

def ensure_ssl_certificates(cert_dir: str) -> Tuple[str, str]:
    """
    Asegura la existencia de un par de certificado y clave privada SSL en el directorio cert_dir.
    Si no existen, los genera automáticamente para permitir conexiones HTTPS seguras.
    Devuelve (ruta_cert, ruta_key).
    """
    os.makedirs(cert_dir, exist_ok=True)
    cert_path = os.path.join(cert_dir, "registry_cert.pem")
    key_path = os.path.join(cert_dir, "registry_key.pem")

    if os.path.isfile(cert_path) and os.path.isfile(key_path):
        return cert_path, key_path

    # Intentamos generarlo con openssl si está instalado
    try:
        import subprocess
        cmd = [
            "openssl", "req", "-x509", "-newkey", "rsa:2048",
            "-keyout", key_path, "-out", cert_path,
            "-days", "365", "-nodes",
            "-subj", "/CN=WM_Registry/O=WaterManagement/C=ES"
        ]
        ret = subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        if ret.returncode == 0 and os.path.isfile(cert_path):
            return cert_path, key_path
    except Exception:
        pass

    # Si openssl no está en PATH o falló, podemos usar cryptography x509 si existe
    if _HAS_CRYPTOGRAPHY:
        try:
            import datetime
            from cryptography import x509
            from cryptography.x509.oid import NameOID
            from cryptography.hazmat.primitives import hashes, serialization
            from cryptography.hazmat.primitives.asymmetric import rsa

            key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
            subject = issuer = x509.Name([
                x509.NameAttribute(NameOID.COMMON_NAME, "WM_Registry"),
                x509.NameAttribute(NameOID.ORGANIZATION_NAME, "WaterManagement"),
            ])
            cert = (
                x509.CertificateBuilder()
                .subject_name(subject)
                .issuer_name(issuer)
                .public_key(key.public_key())
                .serial_number(x509.random_serial_number())
                .not_valid_before(datetime.datetime.now(datetime.timezone.utc))
                .not_valid_after(datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(days=365))
                .sign(key, hashes.SHA256())
            )
            with open(key_path, "wb") as f:
                f.write(key.private_bytes(
                    encoding=serialization.Encoding.PEM,
                    format=serialization.PrivateFormat.TraditionalOpenSSL,
                    encryption_algorithm=serialization.NoEncryption()
                ))
            with open(cert_path, "wb") as f:
                f.write(cert.public_bytes(serialization.Encoding.PEM))
            return cert_path, key_path
        except Exception:
            pass

    return cert_path, key_path


# ==============================================================================
# 4. CERTIFICADO TLS CON NOMBRES SAN (CANAL SEGURO WM_CENTRAL <-> WM_WS_M)
# ==============================================================================

def _is_ip_address(name: str) -> bool:
    import ipaddress
    try:
        ipaddress.ip_address(name)
        return True
    except ValueError:
        return False


def _certificate_covers_san(cert_path: str, san_names: list) -> bool:
    """Comprueba que el certificado incluye en su extensión SAN todos los nombres/IP indicados."""
    import ipaddress
    from cryptography import x509
    try:
        with open(cert_path, "rb") as f:
            cert = x509.load_pem_x509_certificate(f.read())
        ext = cert.extensions.get_extension_for_class(x509.SubjectAlternativeName).value
    except Exception:
        return False
    dns = set(ext.get_values_for_type(x509.DNSName))
    ips = {str(ip) for ip in ext.get_values_for_type(x509.IPAddress)}
    return all(
        (str(ipaddress.ip_address(n)) in ips) if _is_ip_address(n) else (n in dns)
        for n in san_names
    )


def ensure_tls_certificate(cert_path: str, key_path: str, common_name: str, san_names: list) -> bool:
    """
    Garantiza un certificado X.509 autofirmado (RSA-2048, SHA-256) y su clave privada para TLS.
    Lo (re)genera si falta alguno de los dos ficheros, si no forman pareja o si el certificado
    no cubre todos los nombres/IP de san_names (necesarios para que el cliente valide el host).
    La clave privada se guarda con permisos 0600.
    Devuelve True si se ha generado un par nuevo y False si se reutiliza el existente.
    Lanza RuntimeError si no se puede generar (p. ej. falta la librería 'cryptography').
    """
    san_names = list(dict.fromkeys(n.strip() for n in san_names if n and n.strip()))

    if os.path.isfile(cert_path) and os.path.isfile(key_path):
        try:
            ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER).load_cert_chain(cert_path, key_path)
            pair_ok = True
        except (ssl.SSLError, OSError):
            pair_ok = False
        if pair_ok and _HAS_CRYPTOGRAPHY and _certificate_covers_san(cert_path, san_names):
            return False

    if not _HAS_CRYPTOGRAPHY:
        raise RuntimeError(
            "Falta la librería 'cryptography' para generar el certificado TLS "
            "(instálela con: pip install -r requirements.txt)."
        )

    import datetime
    import ipaddress
    from cryptography import x509
    from cryptography.x509.oid import NameOID
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import rsa

    entries = [
        x509.IPAddress(ipaddress.ip_address(n)) if _is_ip_address(n) else x509.DNSName(n)
        for n in san_names
    ]
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    subject = issuer = x509.Name([
        x509.NameAttribute(NameOID.COUNTRY_NAME, "ES"),
        x509.NameAttribute(NameOID.ORGANIZATION_NAME, "WaterManagement SD"),
        x509.NameAttribute(NameOID.COMMON_NAME, common_name),
    ])
    now_utc = datetime.datetime.now(datetime.timezone.utc)
    cert = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(issuer)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now_utc - datetime.timedelta(days=1))
        .not_valid_after(now_utc + datetime.timedelta(days=3650))
        .add_extension(x509.SubjectAlternativeName(entries), critical=False)
        .sign(key, hashes.SHA256())
    )

    for path in (cert_path, key_path):
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    fd = os.open(key_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "wb") as f:
        f.write(key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.TraditionalOpenSSL,
            encryption_algorithm=serialization.NoEncryption()
        ))
    try:
        os.chmod(key_path, 0o600)
    except OSError:
        pass
    with open(cert_path, "wb") as f:
        f.write(cert.public_bytes(serialization.Encoding.PEM))
    return True
