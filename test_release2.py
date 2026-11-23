#!/usr/bin/env python3
"""
test_release2.py: Suite de pruebas de integración para la Release 2 (Seguridad y API REST).
Valida:
  1. wm_crypto: Cifrado y descifrado simétrico con integridad.
  2. WM_Registry: Servidor HTTPS, alta de WS y entrega de credenciales.
  3. WM_Central: Autenticación segura mediante credenciales del Registry.
  4. Entrega de clave simétrica única por WS y cifrado de mensajes en canal por socket.
  5. Denegación y auditoría ante intento de autenticación sin registro o con credencial falsa.
  6. Revocación de clave simétrica en Central: estación queda fuera de servicio y re-autenticación.
  7. API_Central y WM_W: Notificación de alerta meteorológica (<0ºC / lluvia), parada y restauración.
  8. Registro estructurado de eventos de auditoría (Audit Log).
"""

import json
import os
import socket
import ssl
import sys
import tempfile
import threading
import time
import urllib.request

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from common import client_handshake, request_socket
from wm_crypto import generate_symmetric_key, encrypt_message, decrypt_message, is_encrypted
from WM_Registry.WM_Registry import WMRegistry
from WM_Central.WM_Central import WMCentral
from WM_W.WM_W import WeatherControlOffice


def run_tests():
    print("=" * 70)
    print("INICIANDO PRUEBAS DE INTEGRACIÓN — RELEASE 2 (SEGURIDAD Y API REST)")
    print("=" * 70)

    # --------------------------------------------------------------------------
    # PRUEBA 1: Criptografía simétrica (wm_crypto)
    # --------------------------------------------------------------------------
    print("\n[TEST 1] Verificando cifrado y descifrado simétrico autenticado (Fernet)...")
    key = generate_symmetric_key()
    original_msg = "PING#WS-99#OK#SENSOR_OK"
    encrypted_msg = encrypt_message(key, original_msg)
    assert is_encrypted(encrypted_msg), "El mensaje no tiene el prefijo de cifrado 'ENC:'"
    assert encrypted_msg != original_msg, "El mensaje cifrado no debe coincidir con el texto plano"
    decrypted_msg = decrypt_message(key, encrypted_msg)
    assert decrypted_msg == original_msg, f"Fallo al descifrar: {decrypted_msg} != {original_msg}"

    # Verificación de integridad ante clave incorrecta
    fake_key = generate_symmetric_key()
    assert decrypt_message(fake_key, encrypted_msg) is None, "Descifrado con clave errónea debió fallar"
    print("  -> TEST 1 SUPERADO: Cifrado simétrico, integridad y descifrado OK.")

    # --------------------------------------------------------------------------
    # PRUEBA 2: WM_Registry HTTPS REST API
    # --------------------------------------------------------------------------
    print("\n[TEST 2] Levantando WM_Registry en puerto 5520 (HTTPS)...")
    reg_db = "test_release2_registry.db"
    if os.path.exists(reg_db):
        try: os.remove(reg_db)
        except: pass

    registry = WMRegistry(port=5520, db_path=reg_db)
    t_reg = threading.Thread(target=registry.run_server, daemon=True)
    t_reg.start()
    time.sleep(1.0)

    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE

    # Alta de estación WS-88 en WM_Registry
    req_data = json.dumps({"id": "WS-88", "location": "Pruebas Central"}).encode("utf-8")
    req = urllib.request.Request("https://127.0.0.1:5520/api/stations", data=req_data,
                                 headers={"Content-Type": "application/json"}, method="POST")
    with urllib.request.urlopen(req, context=ctx) as resp:
        res = json.loads(resp.read().decode("utf-8"))
        assert res["status"] == "ok", "Alta en Registry falló"
        ws88_cred = res["data"]["credential"]
        assert ws88_cred.startswith("CRED-"), "Formato de credencial incorrecto"
        print(f"  -> Estación WS-88 dada de alta en WM_Registry. Credencial: {ws88_cred}")

    # Verificar credencial por endpoint /api/verify
    v_data = json.dumps({"id": "WS-88", "credential": ws88_cred}).encode("utf-8")
    req_v = urllib.request.Request("https://127.0.0.1:5520/api/verify", data=v_data,
                                   headers={"Content-Type": "application/json"}, method="POST")
    with urllib.request.urlopen(req_v, context=ctx) as resp_v:
        res_v = json.loads(resp_v.read().decode("utf-8"))
        assert res_v["valid"] is True, "Credencial debió ser válida"
    print("  -> TEST 2 SUPERADO: WM_Registry HTTPS API REST operativo.")

    # --------------------------------------------------------------------------
    # PRUEBA 3: WM_Central y Autenticación con Credenciales
    # --------------------------------------------------------------------------
    print("\n[TEST 3] Levantando WM_Central (Sockets 5010, API 8090)...")
    central_db = "test_release2_central.db"
    if os.path.exists(central_db):
        try: os.remove(central_db)
        except: pass

    # Mock bootstrap de Kafka para la prueba
    # Certificado TLS de Central en un directorio temporal (canal cifrado con los monitores)
    tls_dir = tempfile.mkdtemp(prefix="wm_r2_tls_")
    central = WMCentral(socket_port=5010, kafka_bootstrap="localhost:9092",
                        web_port=8090, db_path=central_db,
                        tls_cert=os.path.join(tls_dir, "central_cert.pem"),
                        tls_key=os.path.join(tls_dir, "central_key.pem"))
    central.registry_url = "https://127.0.0.1:5520"
    central.registry_db_path = reg_db

    t_cent_sock = threading.Thread(target=central.run_socket_server, daemon=True)
    t_cent_sock.start()
    t_cent_web = threading.Thread(target=central.run_web_server, daemon=True)
    t_cent_web.start()
    time.sleep(1.0)

    # Clientes TLS que validan el certificado de Central (cadena y nombre de host)
    central_ctx = ssl.create_default_context(cafile=central.tls_cert)

    def tls_connect():
        raw = socket.create_connection(("127.0.0.1", 5010), timeout=3.0)
        return central_ctx.wrap_socket(raw, server_hostname="127.0.0.1")

    # 3.0 Un cliente sin TLS es rechazado por Central
    s_plain = socket.create_connection(("127.0.0.1", 5010), timeout=3.0)
    s_plain.sendall(b"\x05\x02AUTH#WS-88#CRED-X#Zona\x03\x00")
    try:
        plain_reply = s_plain.recv(16)
    except ConnectionResetError:
        plain_reply = b""
    assert plain_reply == b"", "Central no debe responder a un cliente sin TLS"
    s_plain.close()
    print("  -> Conexión sin TLS rechazada por Central.")

    # 3.1 Intento de autenticación sin registro en Registry -> Denegado
    s_fake = tls_connect()
    client_handshake(s_fake)
    resp_fake = request_socket(s_fake, "AUTH#WS-99#CRED-FALSA-1234#Zona Desconocida")
    assert "KO" in resp_fake, f"Autenticación con credencial falsa debió ser rechazada: {resp_fake}"
    s_fake.close()
    print("  -> Autenticación de estación no registrada rechazada con éxito (KO).")

    # 3.2 Autenticación con credencial legítima de Registry
    s_ws88 = tls_connect()
    client_handshake(s_ws88)
    resp_ok = request_socket(s_ws88, f"AUTH#WS-88#{ws88_cred}#Parque Central")
    assert resp_ok.startswith("STATUS#OK#"), f"Autenticación legítima falló: {resp_ok}"
    parts = resp_ok.split("#")
    ws88_sym_key = parts[2].strip()
    assert len(ws88_sym_key) >= 16, "No se recibió la clave de cifrado simétrico única"
    print(f"  -> Autenticación legítima aceptada (OK). Clave simétrica única recibida: {ws88_sym_key[:12]}...")
    print("  -> TEST 3 SUPERADO: Autenticación basada en Registry y entrega de claves OK.")

    # --------------------------------------------------------------------------
    # PRUEBA 4: Cifrado en canal por socket hacia Central
    # --------------------------------------------------------------------------
    print("\n[TEST 4] Enviando mensaje cifrado simétricamente a Central...")
    ping_plain = "PING#WS-88#OK#OK"
    ping_enc = encrypt_message(ws88_sym_key, ping_plain)
    resp_ping = request_socket(s_ws88, ping_enc)
    assert is_encrypted(resp_ping), "La respuesta de Central debe venir cifrada"
    resp_ping_dec = decrypt_message(ws88_sym_key, resp_ping)
    assert "OK" in resp_ping_dec, f"Respuesta de PING inesperada: {resp_ping_dec}"
    print(f"  -> PING cifrado transmitido y respondido cifrado con éxito: {resp_ping_dec}")
    print("  -> TEST 4 SUPERADO: Canal cifrado simétrico extremo a extremo OK.")

    # --------------------------------------------------------------------------
    # PRUEBA 5: Revocación de claves en Central y Fuera de Servicio
    # --------------------------------------------------------------------------
    print("\n[TEST 5] Revocando claves simétricas de WS-88 en Central por supuesta vulnerabilidad...")
    revoked = central.revoke_station_keys("WS-88")
    assert revoked is True, "Fallo al revocar claves"
    assert central.stations["WS-88"]["status"] == "FUERA DE SERVICIO", "La estación debe quedar FUERA DE SERVICIO"
    assert "WS-88" not in central.station_keys, "La clave simétrica debe haber sido borrada"

    # Central avisa de la revocación en la respuesta al siguiente mensaje de la estación
    resp_rev = request_socket(s_ws88, encrypt_message(ws88_sym_key, "PING#WS-88#OK#OK"))
    assert "REVOKED" in resp_rev, f"Se esperaba aviso REVOKED de Central: {resp_rev}"
    s_ws88.close()
    time.sleep(0.3)
    assert central.stations["WS-88"]["status"] == "FUERA DE SERVICIO", \
        "Tras desconectarse con claves revocadas debe seguir FUERA DE SERVICIO"
    print("  -> Central rechazó mensaje con claves revocadas. Estación queda fuera de servicio.")
    print("  -> TEST 5 SUPERADO: Revocación de claves y estado FUERA DE SERVICIO OK.")

    # --------------------------------------------------------------------------
    # PRUEBA 6: API_Central y Alerta Meteorológica de WM_W
    # --------------------------------------------------------------------------
    print("\n[TEST 6] Simulando alerta meteorológica de congelación (< 0ºC) desde WM_W vía API_Central...")
    # Re-autenticación de WS-88 (nuevas claves) y activación para la prueba de alerta climática
    s_ws88 = tls_connect()
    client_handshake(s_ws88)
    resp_reauth = request_socket(s_ws88, f"AUTH#WS-88#{ws88_cred}#Parque Central")
    assert resp_reauth.startswith("STATUS#OK#") and resp_reauth.split("#")[2] != ws88_sym_key, \
        f"La re-autenticación debe entregar una clave nueva: {resp_reauth}"
    central.activate_station("WS-88")

    alert_payload = json.dumps({
        "ws_id": "WS-88",
        "city": "Helsinki",
        "temp": -3.5,
        "condition": "Snow",
        "alert": True,
        "reason": "Temperatura de congelación (-3.5ºC)"
    }).encode("utf-8")

    req_alert = urllib.request.Request("http://127.0.0.1:8090/api/weather/alert", data=alert_payload,
                                       headers={"Content-Type": "application/json"}, method="POST")
    with urllib.request.urlopen(req_alert) as resp_al:
        assert resp_al.status == 200, "API_Central rechazó alerta"

    # Comprobamos que WS-88 pasó a FUERA DE SERVICIO por el clima adverso
    assert central.stations["WS-88"]["status"] == "FUERA DE SERVICIO", "WS-88 debió pasar a FUERA DE SERVICIO por helada"
    assert "WS-88" in central.weather_alerts, "Alerta meteorológica no registrada en Central"
    print("  -> Alerta climática procesada: WS-88 detenida y en FUERA DE SERVICIO.")

    # Cancelación de la alerta climática (vuelve a ser favorable)
    cancel_payload = json.dumps({
        "ws_id": "WS-88",
        "city": "Helsinki",
        "temp": 14.0,
        "condition": "Clear",
        "alert": False,
        "reason": "Clima normalizado"
    }).encode("utf-8")
    req_cancel = urllib.request.Request("http://127.0.0.1:8090/api/weather/alert", data=cancel_payload,
                                        headers={"Content-Type": "application/json"}, method="POST")
    with urllib.request.urlopen(req_cancel) as resp_canc:
        assert resp_canc.status == 200

    assert central.stations["WS-88"]["status"] == "DISPONIBLE", "WS-88 debió restaurarse a DISPONIBLE"
    assert "WS-88" not in central.weather_alerts, "Alerta debió ser eliminada"
    print("  -> Cancelación climática procesada: WS-88 restaurada a DISPONIBLE.")
    print("  -> TEST 6 SUPERADO: Consumo de API_Central por WM_W y gestión de alertas OK.")

    # --------------------------------------------------------------------------
    # PRUEBA 7: Registro estructurado de auditoría (Audit Log)
    # --------------------------------------------------------------------------
    print("\n[TEST 7] Verificando registro estructurado de auditoría...")
    req_audit = urllib.request.Request("http://127.0.0.1:8090/api/audit")
    with urllib.request.urlopen(req_audit) as resp_aud:
        audits = json.loads(resp_aud.read().decode("utf-8"))
        assert len(audits) >= 4, f"Se esperaban múltiples registros de auditoría, encontrados: {len(audits)}"
        actions = [a["action"] for a in audits]
        assert "AUTH_FAILED" in actions, "Falta evento AUTH_FAILED en auditoría"
        assert "AUTH_SUCCESS" in actions, "Falta evento AUTH_SUCCESS en auditoría"
        assert "KEY_REVOCATION" in actions, "Falta evento KEY_REVOCATION en auditoría"
        assert "WEATHER_ALERT" in actions, "Falta evento WEATHER_ALERT en auditoría"
        print(f"  -> Registros de auditoría encontrados ({len(audits)}): {actions}")
    print("  -> TEST 7 SUPERADO: Auditoría estructurada conforme a la especificación.")

    print("\n" + "=" * 70, flush=True)
    print("¡TODAS LAS PRUEBAS DE LA RELEASE 2 SE HAN SUPERADO CON ÉXITO! (100%)", flush=True)
    print("=" * 70, flush=True)


if __name__ == "__main__":
    run_tests()
    time.sleep(0.5)
    os._exit(0)
