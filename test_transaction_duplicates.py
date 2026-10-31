#!/usr/bin/env python3
"""
Pruebas automatizadas para el requisito 9.6:
  - Conservación de transacciones y prevención de duplicados (transaction_id + índice único).
  - Persistencia en SQLite antes de confirmar el procesamiento (offset Kafka) o notificar SUMMARY.
  - Recuperación ante fallos de persistencia, caída de Central o retraso de Engine tras LEAK.
  - Compatibilidad hacia atrás con bases de datos SQLite existentes sin columna transaction_id.
"""

import os
import sqlite3
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)
for subdir in ("WM_Central", "WM_WS", "WM_FO"):
    mod_path = os.path.join(ROOT_DIR, subdir)
    if mod_path not in sys.path:
        sys.path.insert(0, mod_path)

import common
import WM_Central as central_module
import WM_WS_E as engine_module
import WM_FO as fo_module


class TransactionDuplicateTests(unittest.TestCase):

    def test_same_finished_event_does_not_duplicate_history(self):
        """Dos eventos FINISHED con el mismo transaction_id producen 1 sola fila en SQLite y 1 solo SUMMARY."""
        with tempfile.TemporaryDirectory() as tmp:
            db_path = os.path.join(tmp, "central.db")
            sent_messages = []

            def fake_send_kafka(prod, topic, payload, **kw):
                sent_messages.append((topic, payload))
                return True

            with patch.object(central_module, "ensure_topics"), \
                 patch.object(central_module, "make_producer", return_value="FAKE_PROD"), \
                 patch.object(central_module, "send_kafka", side_effect=fake_send_kafka):

                central = central_module.WMCentral(
                    socket_port=0,
                    kafka_bootstrap="localhost:9092",
                    web_port=0,
                    db_path=db_path
                )

                central.stations["WS-01"]["operator"] = "FO-01"
                central.stations["WS-01"]["start_time"] = "2026-10-31T12:00:00"

                event = {
                    "type": "FINISHED",
                    "ws": "WS-01",
                    "op": "FO-01",
                    "vol": 0.52,
                    "dur": 5,
                    "reason": "TIME",
                    "transaction_id": "TEST-RIEGO-001"
                }

                central.handle_ws_event(event)
                central.handle_ws_event(event)

                with sqlite3.connect(db_path) as db:
                    rows = db.execute(
                        "SELECT transaction_id, total_vol, duration_sec FROM watering_history WHERE ws_id = 'WS-01'"
                    ).fetchall()

                self.assertEqual(
                    len(rows), 1,
                    f"El mismo evento FINISHED se registró {len(rows)} veces"
                )
                self.assertEqual(rows[0][0], "TEST-RIEGO-001")

                summaries = [
                    msg for topic, msg in sent_messages
                    if topic == common.TOPIC_REPLIES and msg.get("type") == "SUMMARY"
                ]
                self.assertEqual(
                    len(summaries), 1,
                    f"Se enviaron {len(summaries)} notificaciones SUMMARY para la misma transacción"
                )

    def test_persistence_failure_prevents_notification_and_allows_recovery(self):
        """Si SQLite falla al guardar FINISHED, no se envía SUMMARY ni se pierde el estado; al recuperar, guarda y notifica 1 vez."""
        with tempfile.TemporaryDirectory() as tmp:
            db_path = os.path.join(tmp, "central.db")
            sent_messages = []

            def fake_send_kafka(prod, topic, payload, **kw):
                sent_messages.append((topic, payload))
                return True

            with patch.object(central_module, "ensure_topics"), \
                 patch.object(central_module, "make_producer", return_value="FAKE_PROD"), \
                 patch.object(central_module, "send_kafka", side_effect=fake_send_kafka):

                central = central_module.WMCentral(
                    socket_port=0,
                    kafka_bootstrap="localhost:9092",
                    web_port=0,
                    db_path=db_path
                )

                central.stations["WS-01"]["status"] = common.STATUS_WATERING
                central.stations["WS-01"]["operator"] = "FO-01"
                central.stations["WS-01"]["start_time"] = "2026-10-31T12:05:00"
                central.stations["WS-01"]["transaction_id"] = "TX-PERSIST-FAIL-01"

                event = {
                    "type": "FINISHED",
                    "ws": "WS-01",
                    "op": "FO-01",
                    "vol": 0.85,
                    "dur": 8,
                    "reason": "TIME",
                    "transaction_id": "TX-PERSIST-FAIL-01"
                }

                # 1. Simular fallo de persistencia en SQLite
                with patch.object(
                    central, "_record_watering_history",
                    side_effect=sqlite3.OperationalError("disk I/O error")
                ):
                    with self.assertRaises(sqlite3.OperationalError):
                        central.handle_ws_event(event)

                # Verificar que NO se envió SUMMARY al operario cuando falló la BD
                summaries_after_fail = [
                    msg for topic, msg in sent_messages
                    if topic == common.TOPIC_REPLIES and msg.get("type") == "SUMMARY"
                ]
                self.assertEqual(
                    len(summaries_after_fail), 0,
                    "No debe enviarse SUMMARY si antes falló la persistencia en SQLite"
                )
                # El estado en memoria sigue conservando los datos para el reintento
                self.assertEqual(central.stations["WS-01"]["transaction_id"], "TX-PERSIST-FAIL-01")

                # 2. Reintentar cuando SQLite vuelve a estar operativa
                central.handle_ws_event(event)

                with sqlite3.connect(db_path) as db:
                    rows = db.execute(
                        "SELECT transaction_id, total_vol FROM watering_history WHERE ws_id = 'WS-01'"
                    ).fetchall()
                self.assertEqual(len(rows), 1)
                self.assertEqual(rows[0][0], "TX-PERSIST-FAIL-01")
                self.assertAlmostEqual(rows[0][1], 0.85, places=2)

                summaries_after_recovery = [
                    msg for topic, msg in sent_messages
                    if topic == common.TOPIC_REPLIES and msg.get("type") == "SUMMARY"
                ]
                self.assertEqual(len(summaries_after_recovery), 1)

    def test_kafka_events_consumer_commits_offset_only_after_persistence(self):
        """run_kafka_events_consumer no confirma el offset si falla handle_ws_event y reprocesa el evento al recuperar."""
        with tempfile.TemporaryDirectory() as tmp:
            db_path = os.path.join(tmp, "central.db")

            with patch.object(central_module, "ensure_topics"), \
                 patch.object(central_module, "make_producer", return_value="FAKE_PROD"), \
                 patch.object(central_module, "send_kafka", return_value=True):

                central = central_module.WMCentral(
                    socket_port=0,
                    kafka_bootstrap="localhost:9092",
                    web_port=0,
                    db_path=db_path
                )

                event = {
                    "type": "FINISHED",
                    "ws": "WS-01",
                    "op": "FO-01",
                    "vol": 1.10,
                    "dur": 10,
                    "reason": "TIME",
                    "transaction_id": "TX-KAFKA-COMMIT-01"
                }

                class ScriptedConsumer:
                    def __init__(self, should_stop_after_commit: bool):
                        self.committed = 0
                        self.should_stop_after_commit = should_stop_after_commit

                    def __iter__(self):
                        msg = type("Msg", (), {"value": event})()
                        yield msg

                    def commit(self):
                        self.committed += 1
                        if self.should_stop_after_commit:
                            central.running = False

                first_consumer = ScriptedConsumer(should_stop_after_commit=False)
                second_consumer = ScriptedConsumer(should_stop_after_commit=True)
                consumers = iter([first_consumer, second_consumer])

                real_record = central._record_watering_history
                attempts = {"count": 0}

                def flaky_record(*args, **kwargs):
                    attempts["count"] += 1
                    if attempts["count"] == 1:
                        raise sqlite3.OperationalError("database is locked")
                    return real_record(*args, **kwargs)

                with patch.object(central_module, "make_consumer", side_effect=lambda *a, **kw: next(consumers)), \
                     patch.object(central, "_record_watering_history", side_effect=flaky_record), \
                     patch("time.sleep", return_value=None):
                    central.run_kafka_events_consumer()

                self.assertEqual(
                    first_consumer.committed, 0,
                    "El primer consumidor NO debió confirmar el offset tras fallar SQLite"
                )
                self.assertEqual(
                    second_consumer.committed, 1,
                    "El segundo consumidor debió confirmar el offset tras persistir en SQLite"
                )

                with sqlite3.connect(db_path) as db:
                    count = db.execute(
                        "SELECT COUNT(*) FROM watering_history WHERE transaction_id = 'TX-KAFKA-COMMIT-01'"
                    ).fetchone()[0]
                self.assertEqual(count, 1)

    def test_interrupted_leak_consolidated_and_late_engine_finished_not_duplicated(self):
        """Si Central consolida un riego por LEAK y luego llega el FINISHED tardío del Engine con el mismo transaction_id,
        se actualiza el volumen final en la misma fila sin duplicar la transacción."""
        with tempfile.TemporaryDirectory() as tmp:
            db_path = os.path.join(tmp, "central.db")
            sent_messages = []

            def fake_send_kafka(prod, topic, payload, **kw):
                sent_messages.append((topic, payload))
                return True

            with patch.object(central_module, "ensure_topics"), \
                 patch.object(central_module, "make_producer", return_value="FAKE_PROD"), \
                 patch.object(central_module, "send_kafka", side_effect=fake_send_kafka):

                central = central_module.WMCentral(
                    socket_port=0,
                    kafka_bootstrap="localhost:9092",
                    web_port=0,
                    db_path=db_path
                )

                st = central.stations["WS-01"]
                st["status"] = common.STATUS_LEAK
                st["operator"] = "FO-01"
                st["vol"] = 0.30
                st["start_time"] = "2026-10-31T12:10:00"
                st["transaction_id"] = "TX-LEAK-RECOVERY-01"
                st["interrupted_watering"] = {
                    "op": "FO-01",
                    "vol": 0.30,
                    "start_time": "2026-10-31T12:10:00",
                    "transaction_id": "TX-LEAK-RECOVERY-01",
                    "ts": 100.0,
                }

                with patch("time.sleep", return_value=None):
                    central._finalize_interrupted_if_unreported("WS-01", 100.0)

                # Posteriormente el Engine reconecta y envía su FINISHED real con el mismo transaction_id
                late_finished = {
                    "type": "FINISHED",
                    "ws": "WS-01",
                    "op": "FO-01",
                    "vol": 0.48,
                    "dur": 4,
                    "reason": "LEAK",
                    "transaction_id": "TX-LEAK-RECOVERY-01",
                }
                central.handle_ws_event(late_finished)

                with sqlite3.connect(db_path) as db:
                    rows = db.execute(
                        "SELECT transaction_id, total_vol, duration_sec FROM watering_history WHERE ws_id = 'WS-01'"
                    ).fetchall()

                self.assertEqual(len(rows), 1, "No debe duplicarse la fila cuando llega el FINISHED tardío del Engine")
                self.assertEqual(rows[0][0], "TX-LEAK-RECOVERY-01")
                self.assertAlmostEqual(rows[0][1], 0.48, places=2)

                summaries = [
                    msg for topic, msg in sent_messages
                    if topic == common.TOPIC_REPLIES and msg.get("type") == "SUMMARY"
                ]
                self.assertEqual(len(summaries), 1)

    def test_engine_propagates_transaction_id_and_retries_pending_finished(self):
        """WSEngine propaga el mismo transaction_id en STARTED y FINISHED, y reintenta FINISHED si Kafka falla temporalmente."""
        sent_events = []
        kafka_available = {"ok": True}

        def fake_send_kafka(prod, topic, payload, **kw):
            if payload.get("type") == "FINISHED" and not kafka_available["ok"]:
                return False
            sent_events.append(payload)
            return True

        with patch.object(engine_module, "make_producer", return_value="FAKE_PROD"), \
             patch.object(engine_module, "send_kafka", side_effect=fake_send_kafka):

            engine = engine_module.WSEngine("localhost:9092", "127.0.0.1:6001")
            engine.ws_id = "WS-01"
            engine.monitor_connected = True
            engine.central_connected = True

            # Iniciar riego con transaction_id asignado por Central (duración=0 para control manual en el test)
            engine.start_watering("FO-01", 0, transaction_id="TX-ENGINE-001")
            engine.accumulated_vol = 0.64

            # Simular caída temporal de Kafka al finalizar el riego
            kafka_available["ok"] = False
            engine.finish_watering("MANUAL")

            self.assertEqual(len(engine.pending_finished_events), 1)
            self.assertEqual(engine.pending_finished_events[0]["transaction_id"], "TX-ENGINE-001")

            # Restablecer Kafka y vaciar cola pendiente
            kafka_available["ok"] = True
            flushed = engine.flush_pending_finished()
            self.assertEqual(flushed, 1)
            self.assertEqual(len(engine.pending_finished_events), 0)

            started_ev = [e for e in sent_events if e["type"] == "STARTED"]
            finished_ev = [e for e in sent_events if e["type"] == "FINISHED"]
            self.assertEqual(len(started_ev), 1)
            self.assertEqual(len(finished_ev), 1)
            self.assertEqual(started_ev[0]["transaction_id"], "TX-ENGINE-001")
            self.assertEqual(finished_ev[0]["transaction_id"], "TX-ENGINE-001")
            self.assertAlmostEqual(finished_ev[0]["vol"], 0.64, places=2)

    def test_backward_compatibility_with_legacy_sqlite_schema(self):
        """Una base de datos SQLite existente sin columna transaction_id se migra sin perder su historial previo."""
        with tempfile.TemporaryDirectory() as tmp:
            db_path = os.path.join(tmp, "legacy_central.db")

            # Crear esquema antiguo (sin transaction_id) e insertar dos registros históricos
            with sqlite3.connect(db_path) as db:
                db.execute("""
                    CREATE TABLE watering_history (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        ws_id TEXT NOT NULL,
                        operator_id TEXT NOT NULL,
                        start_time TEXT NOT NULL,
                        end_time TEXT NOT NULL,
                        total_vol REAL NOT NULL,
                        duration_sec INTEGER NOT NULL,
                        reason TEXT NOT NULL
                    )
                """)
                db.execute("""
                    INSERT INTO watering_history (ws_id, operator_id, start_time, end_time, total_vol, duration_sec, reason)
                    VALUES ('WS-01', 'FO-01', '2026-10-01T10:00:00', '2026-10-01T10:00:10', 1.5, 10, 'TIME'),
                           ('WS-02', 'FO-02', '2026-10-01T11:00:00', '2026-10-01T11:00:10', 1.8, 10, 'TIME')
                """)
                db.commit()

            with patch.object(central_module, "ensure_topics"), \
                 patch.object(central_module, "make_producer", return_value="FAKE_PROD"), \
                 patch.object(central_module, "send_kafka", return_value=True):

                central = central_module.WMCentral(
                    socket_port=0,
                    kafka_bootstrap="localhost:9092",
                    web_port=0,
                    db_path=db_path
                )

                ev = {
                    "type": "FINISHED",
                    "ws": "WS-01",
                    "op": "FO-01",
                    "vol": 0.95,
                    "dur": 6,
                    "reason": "TIME",
                    "transaction_id": "TX-NEW-AFTER-MIGRATION"
                }
                central.handle_ws_event(ev)
                central.handle_ws_event(ev)

                with sqlite3.connect(db_path) as db:
                    total_rows = db.execute("SELECT COUNT(*) FROM watering_history").fetchone()[0]
                    migrated_row = db.execute(
                        "SELECT total_vol FROM watering_history WHERE transaction_id = 'TX-NEW-AFTER-MIGRATION'"
                    ).fetchall()

                self.assertEqual(total_rows, 3, "Deben conservarse las 2 filas históricas más 1 nueva transacción")
                self.assertEqual(len(migrated_row), 1)


if __name__ == "__main__":
    unittest.main()
