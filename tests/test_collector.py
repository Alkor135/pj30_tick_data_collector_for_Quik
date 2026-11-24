r"""Проверяет перенос пакетов в SQLite и выбор ближайших контрактов QLua.

Запуск из корня проекта: .\.venv\Scripts\python.exe -m unittest discover -s tests -v
Для проверки QLua задайте переменную QUIK_LUA_DLL с путём к lua54.dll терминала.
"""

import ctypes
from contextlib import closing
from datetime import UTC, datetime
import json
import msvcrt
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import unittest

TEST_DIR = Path(__file__).resolve().parent
ROOT = TEST_DIR.parent
TICK_COLUMNS = ["contract_code", "exchange_ts_us", "trade_num", "price", "qty", "flags"]
LEGACY_SCHEMA = """
CREATE TABLE trades (
    family TEXT NOT NULL,
    class_code TEXT NOT NULL,
    contract_code TEXT NOT NULL,
    exchange_ts TEXT NOT NULL,
    trade_num INTEGER NOT NULL,
    price REAL NOT NULL,
    qty INTEGER NOT NULL,
    flags INTEGER NOT NULL,
    PRIMARY KEY (contract_code, exchange_ts, trade_num)
) STRICT, WITHOUT ROWID
"""


def run_import(spool_dir, data_dir):
    """Импортирует пакеты из spool_dir в data_dir и возвращает результат процесса."""
    return subprocess.run(
        [sys.executable, str(ROOT / "collect_ticks.py"), "--once", "--spool-dir",
         str(spool_dir), "--data-dir", str(data_dir)],
        capture_output=True, encoding="utf-8", env={**os.environ, "PYTHONIOENCODING": "utf-8"},
    )


class CollectorTests(unittest.TestCase):
    """Проверки пакетного импорта через командную строку сборщика."""

    def setUp(self):
        """Создаёт отдельные каталоги пакетных файлов и тестовых баз."""
        self.temp = tempfile.TemporaryDirectory(dir=TEST_DIR)
        self.addCleanup(self.temp.cleanup)
        self.spool = Path(self.temp.name) / "spool"
        self.output = Path(self.temp.name) / "data"
        self.spool.mkdir()

    def test_routes_batch_by_week_and_replays_without_duplicates(self):
        """Проверяет границу недели, точность времени UTC, отчёт об импорте и защиту от дублей."""
        previous = "SPBFUT\tRIZ6\t2026-11-22T23:59:59.999999\t42\t106120\t3\t1025\n"
        following = "SPBFUT\tMXZ6\t2026-11-23T00:00:00.000008\t42\t276425\t2\t1\n"
        package = self.spool / "fixture.tsv"
        for attempt in range(2):
            package.write_text(previous + following + following, encoding="ascii")
            result = run_import(self.spool, self.output)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertFalse(package.exists())
            if attempt == 0:
                self.assertRegex(result.stdout,
                                 r"(?m)^\[\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}[+-]\d{2}:\d{2}\] Новых сделок: 2$")

        self.assertEqual(sorted(path.name for path in self.output.glob("*.sqlite")),
                         ["ticks_2026-11-16.sqlite", "ticks_2026-11-23.sqlite"])
        for week, expected in (
            ("2026-11-16", ("RIZ6", 1_795_381_199_999_999, 42, 106120, 3, 1025)),
            ("2026-11-23", ("MXZ6", 1_795_381_200_000_008, 42, 276425, 2, 1)),
        ):
            with closing(sqlite3.connect(self.output / f"ticks_{week}.sqlite")) as database:
                info = database.execute("PRAGMA table_info(trades)").fetchall()
                self.assertEqual([column[1] for column in info], TICK_COLUMNS)
                self.assertEqual(info[1][2], "INTEGER")
                self.assertEqual(database.execute("SELECT * FROM trades").fetchall(), [expected])

    def create_legacy_database(self, timestamp):
        """Создаёт недельную базу прежнего формата с меткой timestamp и возвращает её путь."""
        self.output.mkdir()
        path = self.output / "ticks_2026-11-16.sqlite"
        with closing(sqlite3.connect(path, autocommit=True)) as database:
            database.execute(LEGACY_SCHEMA)
            database.execute("INSERT INTO trades VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                             ("RTS", "SPBFUT", "RIZ6", timestamp, 42, 106120, 3, 1025))
        return path

    def test_migrates_existing_week_and_deduplicates_replayed_trade(self):
        """Проверяет, что старая сделка остаётся в базе и совпадает с повтором после перевода времени."""
        path = self.create_legacy_database("2026-11-20T10:00:00.123456")
        batch = self.spool / "fixture.tsv"
        batch.write_bytes(b"SPBFUT\tRIZ6\t2026-11-20T10:00:00.123456\t42\t106120\t3\t1025\n"
                          b"SPBFUT\tRIZ6\t2026-11-20T10:00:00.123456\t43\t106130\t1\t1026\n")
        result = run_import(self.spool, self.output)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertFalse(batch.exists())
        with closing(sqlite3.connect(path)) as database:
            self.assertEqual(database.execute("SELECT * FROM trades ORDER BY trade_num").fetchall(), [
                ("RIZ6", 1_795_158_000_123_456, 42, 106120, 3, 1025),
                ("RIZ6", 1_795_158_000_123_456, 43, 106130, 1, 1026),
            ])

    def test_preserves_old_database_if_conversion_fails(self):
        """Проверяет откат миграции и сохранение пакета при повреждённом времени старой сделки."""
        path = self.create_legacy_database("invalid")
        batch = self.spool / "fixture.tsv"
        batch.write_bytes(b"SPBFUT\tRIZ6\t2026-11-20T10:00:00.123456\t43\t106130\t1\t1026\n")
        result = run_import(self.spool, self.output)
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertTrue(batch.exists())
        with closing(sqlite3.connect(path)) as database:
            self.assertEqual(database.execute("SELECT family, exchange_ts FROM trades").fetchall(),
                             [("RTS", "invalid")])
            self.assertEqual(database.execute("SELECT name FROM sqlite_schema WHERE type = 'table'").fetchall(),
                             [("trades",)])

    def test_keeps_invalid_and_unpublished_batches(self):
        """Проверяет, что повреждённый пакет не импортируется частично."""
        valid = "SPBFUT\tRIZ6\t2026-11-20T18:00:00.123456\t42\t106120\t3\t1025\n"
        bad = self.spool / "bad.tsv"
        bad.write_text(valid + "обрыв", encoding="utf-8")
        unpublished = self.spool / "pending.part"
        unpublished.write_text(valid, encoding="ascii")
        result = run_import(self.spool, self.output)
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertIn("Пакет оставлен для проверки", result.stderr)
        self.assertTrue(bad.exists())
        self.assertTrue(unpublished.exists())
        self.assertEqual(list(self.output.glob("*.sqlite")), [])

    def test_rejects_second_reader_of_same_spool(self):
        """Проверяет, что общий каталог пакетов нельзя импортировать в разные базы одновременно."""
        batch = self.spool / "fixture.tsv"
        batch.write_bytes(b"SPBFUT\tRIZ6\t2026-11-20T18:00:00.123456\t42\t106120\t3\t1025\n")
        with (self.spool / ".import.lock").open("a+b") as lock:
            msvcrt.locking(lock.fileno(), msvcrt.LK_NBLCK, 1)
            result = run_import(self.spool, self.output)
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertIn(".import.lock", result.stderr)
        self.assertTrue(batch.exists())
        self.assertEqual(list(self.output.glob("*.sqlite")), [])


@unittest.skipUnless(os.environ.get("QUIK_LUA_DLL"), "Не задан путь к Lua 5.4 из QUIK")
class LuaTests(unittest.TestCase):
    """Запускает Lua терминала с подменой вызовов API QUIK."""

    def test_exports_only_front_contracts_and_rolls_at_expiry(self):
        """Проверяет независимость каталогов, смену серий и финальный пакет при остановке."""
        with tempfile.TemporaryDirectory(dir=TEST_DIR) as folder:
            spool = Path(folder) / "spool"
            spool.mkdir()
            start_time = int(datetime(2026, 12, 17, 8, tzinfo=UTC).timestamp())
            script = f"""
local now = {start_time}
local calls = 0
function os.time() return now end
function getScriptPath() return {json.dumps(Path(folder).as_posix())} end
function getClassSecurities(class)
    assert(class == 'SPBFUT')
    return 'RIH7,RIZ6,MXH7,MXZ6,RMZ6,MMZ6,RIU6,MXX6'
end
function getSecurityInfo(class, code)
    if calls < 2 and code == 'RIZ6' then
        return {{mat_date=0}}
    end
    local dates = {{RIH7=20270318, RIZ6=20261217, MXH7=20270318, MXZ6=20261217,
                    RIU6=20260917, MXX6=20261119}}
    return {{mat_date=dates[code]}}
end
local ticks = {{}}
function getNumberOf(name) assert(name == 'all_trades'); return #ticks end
function getItem(name, index) assert(name == 'all_trades'); return ticks[index + 1] end
function message(text, level) if level == 3 then error(text) end end
local function tick(code, id, hour, class)
    return {{class_code=class or 'SPBFUT', sec_code=code, trade_num=id,
              price=108125, qty=2, flags=1025,
              datetime={{year=2026, month=12, day=17, hour=hour, min=0, sec=0, ms=123, mcs=123456}}}}
end
ticks = {{tick('RIZ6', 99, 17)}}
function sleep(ms)
    calls = calls + 1
    if calls == 1 then
        now = now + 1
        ticks = {{ticks[1], tick('RIZ6', 1, 18), tick('RIH7', 2, 18), tick('MXZ6', 3, 18),
                  tick('MXH7', 4, 18), tick('RMZ6', 5, 18), tick('MMZ6', 6, 18),
                  tick('RIZ6', 7, 19), tick('RIH7', 8, 19), tick('MXZ6', 9, 19),
                  tick('MXH7', 10, 19), tick('MXH7', 11, 19, 'SPBOPT')}}
    elseif calls == 2 then
        now = now + 5
        ticks[#ticks + 1] = tick('MXH7', 12, 19)
        OnStop()
    else
        error('Ожидался выход после обработки последнего пакета')
    end
end
dofile({json.dumps((ROOT / 'export_ticks.lua').as_posix())})
main()
"""
            lua = ctypes.CDLL(os.environ["QUIK_LUA_DLL"])
            lua.luaL_newstate.restype = ctypes.c_void_p
            lua.luaL_openlibs.argtypes = [ctypes.c_void_p]
            lua.luaL_loadstring.argtypes = [ctypes.c_void_p, ctypes.c_char_p]
            lua.lua_pcallk.argtypes = [ctypes.c_void_p, ctypes.c_int, ctypes.c_int, ctypes.c_int,
                                       ctypes.c_ssize_t, ctypes.c_void_p]
            lua.lua_tolstring.argtypes = [ctypes.c_void_p, ctypes.c_int, ctypes.c_void_p]
            lua.lua_tolstring.restype = ctypes.c_char_p
            lua.lua_close.argtypes = [ctypes.c_void_p]
            state = lua.luaL_newstate()
            try:
                lua.luaL_openlibs(state)
                status = lua.luaL_loadstring(state, script.encode("utf-8"))
                if status == 0:
                    status = lua.lua_pcallk(state, 0, 0, 0, 0, None)
                self.assertEqual(status, 0, lua.lua_tolstring(state, -1, None) if status else None)
            finally:
                lua.lua_close(state)

            rows = [line.split("\t") for batch in sorted(spool.glob("*.tsv"))
                    for line in batch.read_text(encoding="ascii").splitlines()]
            self.assertEqual([(row[1], int(row[3])) for row in rows],
                             [("MXZ6", 3), ("MXH7", 10), ("RIZ6", 1), ("RIH7", 8), ("MXH7", 12)])
            self.assertEqual(rows[0][2], "2026-12-17T18:00:00.123456")

            output = Path(folder) / "data"
            result = run_import(spool, output)
            self.assertEqual(result.returncode, 0, result.stderr)
            database = sqlite3.connect(output / "ticks_2026-12-14.sqlite")
            try:
                self.assertEqual(database.execute(
                    "SELECT contract_code, exchange_ts_us, trade_num FROM trades ORDER BY trade_num"
                ).fetchall(), [("RIZ6", 1_797_519_600_123_456, 1), ("MXZ6", 1_797_519_600_123_456, 3),
                               ("RIH7", 1_797_523_200_123_456, 8), ("MXH7", 1_797_523_200_123_456, 10),
                               ("MXH7", 1_797_523_200_123_456, 12)])
            finally:
                database.close()

