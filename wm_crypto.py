#!/usr/bin/env python3
"""
wm_crypto.py: Utilidades criptográficas para la práctica Water Management (Release 2).
Proporciona:
  - Generación de credenciales seguras de registro (tokens).
  - Generación de claves de cifrado simétrico únicas por estación.
  - Cifrado simétrico autenticado de mensajes (exclusivamente Fernet, librería 'cryptography').
  - Mensajes Kafka cifrados entre WM_Central y WM_WS_E (formato, tipo, WS y número de secuencia).
  - Generación automática de certificados SSL autofirmados para el canal seguro HTTPS de WM_Registry.
  - Generación del certificado TLS (con nombres SAN) del canal seguro WM_Central <-> WM_WS_M.
"""

import base64
import hashlib
import json
import os
import secrets
import ssl
import time
from typing import Optional, Tuple

# 'cryptography' es obligatoria para cifrar (ver require_cryptography); la importación no falla
# aquí para que los módulos que no cifran (p. ej. la generación de credenciales) puedan cargarse.
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
    Formato de clave Fernet.
    """
    key_bytes = secrets.token_bytes(32)
    return base64.urlsafe_b64encode(key_bytes).decode("ascii")


# ==============================================================================
# 2. CIFRADO SIMÉTRICO AUTENTICADO (Fernet: AES-128-CBC + HMAC-SHA256)
# ==============================================================================
# Se usa exclusivamente la implementación Fernet de la librería 'cryptography'.
# Si no está instalada no se recurre a ningún algoritmo alternativo: se lanza un error.

class CryptoUnavailableError(RuntimeError):
    """Falta la librería 'cryptography', imprescindible para el cifrado simétrico."""


def require_cryptography():
    """Comprueba que el cifrado Fernet está disponible (llamar al arrancar cada módulo)."""
    if not _HAS_CRYPTOGRAPHY:
        raise CryptoUnavailableError(
            "Falta la librería 'cryptography' (cifrado Fernet). Instálela con: pip install -r requirements.txt"
        )


def is_valid_symmetric_key(key_str: Optional[str]) -> bool:
    """Indica si la cadena es una clave Fernet válida (32 bytes en base64 URL-safe)."""
    require_cryptography()
    try:
        Fernet(key_str.encode("ascii"))
        return True
    except Exception:
        return False


def encrypt_message(key_str: str, plaintext: str) -> str:
    """
    Cifra y autentica un mensaje con la clave simétrica (Fernet).
    Devuelve una cadena prefijada con 'ENC:' seguida del token Fernet.
    Lanza ValueError si la clave no es válida.
    """
    if not plaintext:
        return ""
    require_cryptography()
    token = Fernet(key_str.encode("ascii")).encrypt(plaintext.encode("utf-8"))
    return "ENC:" + token.decode("ascii")


def decrypt_message(key_str: str, cipher_str: str) -> Optional[str]:
    """
    Descifra un mensaje cifrado con encrypt_message verificando su integridad.
    Devuelve el texto original o None si la clave es incorrecta o el mensaje fue manipulado.
    """
    if not cipher_str:
        return ""
    if not cipher_str.startswith("ENC:"):
        return None  # No es un mensaje cifrado reconocido
    require_cryptography()
    try:
        raw = Fernet(key_str.encode("ascii")).decrypt(cipher_str[4:].encode("ascii"))
        return raw.decode("utf-8")
    except Exception:
        return None


def is_encrypted(message: str) -> bool:
    """Indica si una cadena tiene el formato de un mensaje cifrado."""
    return isinstance(message, str) and message.startswith("ENC:")


# ------------------------------------------------------------------------------
# Mensajes Kafka cifrados entre WM_Central y WM_WS_E (ws_commands / ws_events)
# ------------------------------------------------------------------------------
# Formato en Kafka: {"ws": "<WS>", "enc": "ENC:<token Fernet>"}. El identificador de la WS
# va visible solo para elegir la clave; dentro del cifrado viaja el mensaje completo con:
#   kind -> "cmd" (Central -> Engine) o "evt" (Engine -> Central): evita reinyectar un sentido en el otro.
#   ws   -> debe coincidir con el identificador visible.
#   seq  -> número creciente del emisor; el receptor rechaza los que no superen al último aceptado.
KAFKA_KIND_COMMAND = "cmd"
KAFKA_KIND_EVENT = "evt"
KAFKA_OUTER_FIELDS = frozenset({"ws", "enc"})  # únicos campos visibles admitidos en Kafka


def next_kafka_seq(last_seq: int) -> int:
    """Siguiente número de secuencia: creciente y basado en el reloj del propio emisor
    (el receptor solo compara con mensajes anteriores del mismo emisor, nunca entre relojes)."""
    return max(int(last_seq) + 1, time.time_ns())


def seal_kafka_message(key_str: str, kind: str, ws_id: str, payload: dict, seq: int) -> dict:
    """Construye el mensaje Kafka cifrado y autenticado."""
    inner = dict(payload)
    inner.update({"kind": kind, "ws": ws_id, "seq": int(seq)})
    return {"ws": ws_id, "enc": encrypt_message(key_str, json.dumps(inner, separators=(",", ":")))}


def open_kafka_message(key_str: str, message, kind: str, ws_id: str) -> Tuple[Optional[dict], str]:
    """
    Descifra y valida un mensaje Kafka. Devuelve (contenido, "") si es auténtico y va dirigido
    a ws_id con el tipo esperado; si no, (None, motivo). La comprobación de 'seq' frente al
    último aceptado corresponde al receptor.
    """
    if not isinstance(message, dict) or not is_encrypted(message.get("enc")):
        return None, "NO_CIFRADO"
    if set(message) != KAFKA_OUTER_FIELDS:
        return None, "CAMPOS_EXTRA"  # ningún dato adicional puede viajar en claro
    if str(message.get("ws", "")).strip().upper() != ws_id:
        return None, "OTRA_WS"
    plain = decrypt_message(key_str, message["enc"])
    if plain is None:
        return None, "CLAVE_O_INTEGRIDAD"
    try:
        inner = json.loads(plain)
    except ValueError:
        return None, "FORMATO"
    if not isinstance(inner, dict) or not isinstance(inner.get("seq"), int):
        return None, "FORMATO"
    if inner.get("kind") != kind:
        return None, "TIPO"
    if str(inner.get("ws", "")).strip().upper() != ws_id:
        return None, "OTRA_WS"
    return inner, ""


def key_fingerprint(key_str: str) -> str:
    """Huella SHA-256 de una clave (permite asociar datos a una clave sin almacenarla)."""
    return hashlib.sha256(key_str.encode("ascii")).hexdigest()


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
