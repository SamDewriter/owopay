from pathlib import Path
import unittest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
EXTRACTOR = PROJECT_ROOT / "ingestion" / "batch" / "extract_to_dw.py"


class TestExtractIdempotency(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.source = EXTRACTOR.read_text(encoding="utf-8")

    def test_unique_transaction_id_index_exists(self):
        self.assertIn("transactions_txn_id_uidx", self.source)

    def test_advisory_lock_exists(self):
        self.assertIn("pg_advisory_xact_lock", self.source)

    def test_daily_replacement_exists(self):
        self.assertIn(
            "delete from raw.transactions where _wat_day = %s",
            self.source,
        )


if __name__ == "__main__":
    unittest.main()