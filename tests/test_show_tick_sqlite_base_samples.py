r"""Проверяет выбор крайних сделок, пометку времени МСК и чтение активного WAL.

Запуск из корня проекта: .\.venv\Scripts\python.exe -m unittest discover -s tests -p test_show_tick_sqlite_base_samples.py -v
"""

from contextlib import closing
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import unittest

TEST_DIR = Path(__file__).resolve().parent
SCRIPT = TEST_DIR.parent / "show_tick_sqlite_base_samples.py"
SCHEMA = """
CREATE TABLE trades (
    contract_code TEXT NOT NULL,
    exchange_ts_us INTEGER NOT NULL,
    trade_num INTEGER NOT NULL,
    price REAL NOT NULL,
    qty INTEGER NOT NULL,
    flags INTEGER NOT NULL,
    PRIMARY KEY (contract_code, exchange_ts_us, trade_num)
) STRICT, WITHOUT ROWID
"""


class DatabasePreviewTests(unittest.TestCase):
    """Проверяет консольный просмотр SQLite через его обычную команду запуска."""

    def setUp(self):
        """Создаёт временный каталог для тестовых файлов SQLite."""
        folder = tempfile.TemporaryDirectory(dir=TEST_DIR)
        self.addCleanup(folder.cleanup)
        self.data_dir = Path(folder.name)

    def run_preview(self):
        """Запускает просмотр выбранного каталога и возвращает текст двух секций."""
        result = subprocess.run(
            [sys.executable, "-X", "utf8", str(SCRIPT), "--data-dir", str(self.data_dir)],
            capture_output=True, encoding="utf-8",
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        return result.stdout

    def write_week(self, name, start_us, numbers):
        """Создаёт файл name с метками от start_us для номеров сделок numbers."""
        with closing(sqlite3.connect(self.data_dir / name, autocommit=True)) as database:
            database.execute(SCHEMA)
            database.executemany("INSERT INTO trades VALUES (?, ?, ?, ?, ?, ?)", [
                ("MXZ6" if trade % 2 else "RIZ6", start_us + trade, trade, 106120, 2, 1025)
                for trade in numbers
            ])

    def trade_numbers(self, section):
        """Возвращает номера сделок в порядке отображения в секции section."""
        return [int(line.split("|")[3]) for line in section.splitlines() if line.startswith(("RIZ6", "MXZ6"))]

    def test_uses_first_and_last_weeks_in_exchange_time_order(self):
        """Проверяет выборку из крайних недель и пометку вычисляемой колонки МСК."""
        self.write_week("ticks_2026-11-23.sqlite", 1_795_381_200_000_000, [28, 27, 26, 25, 24, 23, 22, 21])
        self.write_week("ticks_2026-11-09.sqlite", 1_794_171_600_000_000, [8, 7, 6, 5, 4, 3, 2, 1])
        self.write_week("ticks_2026-11-16.sqlite", 1_794_776_400_000_000, [11, 12])
        (self.data_dir / "unrelated.sqlite").touch()

        first, last = self.run_preview().split("Последние сделки")
        self.assertIn("ticks_2026-11-09.sqlite", first)
        self.assertIn("ticks_2026-11-23.sqlite", last)
        self.assertNotIn("ticks_2026-11-16.sqlite", first + last)
        self.assertEqual(self.trade_numbers(first), [1, 2, 3, 4, 5])
        self.assertEqual(self.trade_numbers(last), [24, 25, 26, 27, 28])
        for section in (first, last):
            self.assertIn("Время МСК (отсутствует в БД)", section)
        self.assertIn("2026-11-23 00:00:00.000024", last)

    def test_reads_committed_wal_from_a_single_week(self):
        """Видит подтверждённые записи активного WAL и показывает обе секции одного файла."""
        path = self.data_dir / "ticks_2026-11-23.sqlite"
        with closing(sqlite3.connect(path, autocommit=True)) as database:
            database.execute("PRAGMA journal_mode=WAL")
            database.execute("PRAGMA wal_autocheckpoint=0")
            database.execute(SCHEMA)
            database.execute("INSERT INTO trades VALUES ('RIZ6', 1795381200000001, 1, 106120, 2, 1025)")
            database.execute("BEGIN")
            database.execute("INSERT INTO trades VALUES ('RIZ6', 1795381200000002, 2, 106120, 2, 1025)")

            first, last = self.run_preview().split("Последние сделки")
            for section in (first, last):
                self.assertIn(path.name, section)
                self.assertEqual(self.trade_numbers(section), [1])
