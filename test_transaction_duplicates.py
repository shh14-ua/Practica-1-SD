import os
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

import WM_Central.WM_Central as central_module


class TransactionDuplicateTests(unittest.TestCase):

    def test_same_finished_event_does_not_duplicate_history(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = os.path.join(tmp, "central.db")

            with patch.object(central_module, "ensure_topics"), \
                 patch.object(central_module, "make_producer", return_value=None), \
                 patch.object(central_module, "send_kafka", return_value=True):

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
                    count = db.execute(
                        "SELECT COUNT(*) FROM watering_history WHERE ws_id = 'WS-01'"
                    ).fetchone()[0]

                self.assertEqual(
                    count, 1,
                    f"El mismo evento FINISHED se registró {count} veces"
                )


if __name__ == "__main__":
    unittest.main()
