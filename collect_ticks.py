r"""Сохраняет обезличенные сделки ближайших фьючерсов RTS и MIX из QUIK.

Читает готовые пакеты export_ticks.lua из папки spool и добавляет их в SQLite.
Каждая база содержит сделки за календарную неделю по биржевому времени МСК.
Время хранится целым числом микросекунд с начала эпохи Unix в UTC.
Недельная база старого формата обновляется при первой записи в неё.
Пакет удаляется только после фиксации записей во всех затронутых базах.
Сводка о новых сделках содержит локальное время обработки.

Постоянный сбор: .\.venv\Scripts\python.exe .\collect_ticks.py
Разовый импорт: .\.venv\Scripts\python.exe .\collect_ticks.py --once
Другой каталог баз: .\.venv\Scripts\python.exe .\collect_ticks.py --data-dir C:\data_quote\tick_data
"""

import argparse
from collections import defaultdict
from contextlib import contextmanager
from datetime import datetime, timedelta
import math
import msvcrt
from pathlib import Path
import re
import sqlite3
import sys
from time import sleep

DEFAULT_DATA_DIR = Path(r"C:\data_quote\tick_data")
DEFAULT_SPOOL_DIR = Path(__file__).resolve().parent / "spool"
CONTRACT_PATTERN = re.compile(r"(?:RI|MX)[FGHJKMNQUVXZ][0-9]")
UNIX_EPOCH_MSK = datetime(1970, 1, 1, 3)
MICROSECOND = timedelta(microseconds=1)
TICK_COLUMNS = ("contract_code", "exchange_ts_us", "trade_num", "price", "qty", "flags")
LEGACY_COLUMNS = ("family", "class_code", "contract_code", "exchange_ts", "trade_num", "price", "qty", "flags")
TABLE_SQL = """
CREATE TABLE {name} (
    contract_code TEXT NOT NULL,
    exchange_ts_us INTEGER NOT NULL,
    trade_num INTEGER NOT NULL,
    price REAL NOT NULL,
    qty INTEGER NOT NULL,
    flags INTEGER NOT NULL,
    PRIMARY KEY (contract_code, exchange_ts_us, trade_num)
) STRICT, WITHOUT ROWID
"""
INSERT_SQL = """
INSERT INTO trades (contract_code, exchange_ts_us, trade_num, price, qty, flags)
VALUES (?, ?, ?, ?, ?, ?)
ON CONFLICT (contract_code, exchange_ts_us, trade_num) DO NOTHING
"""


def parse_exchange_time(text):
    """Преобразует метку text из QUIK в биржевое время МСК без смещения в объекте."""
    timestamp = datetime.fromisoformat(text)
    if timestamp.tzinfo is not None or timestamp.isoformat(timespec="microseconds") != text:
        raise ValueError("ожидалось биржевое время в формате YYYY-MM-DDTHH:MM:SS.ffffff")
    return timestamp


def to_unix_microseconds(timestamp):
    """Переводит локальное время timestamp из МСК (UTC+3) в целые микросекунды Unix."""
    return (timestamp - UNIX_EPOCH_MSK) // MICROSECOND


def migrate_exchange_time(text):
    """Возвращает целочисленное время Unix для текстовой метки text из прежней SQLite."""
    return to_unix_microseconds(parse_exchange_time(text))


def prepare_trade_table(connection):
    """Создаёт таблицу сделок или переводит прежнюю схему в новый формат одной транзакцией."""
    columns = tuple(row[1] for row in connection.execute("PRAGMA table_info(trades)"))
    if not columns:
        connection.execute(TABLE_SQL.format(name="trades"))
        return
    if columns == TICK_COLUMNS:
        return
    if columns != LEGACY_COLUMNS:
        raise sqlite3.OperationalError("Неизвестная схема таблицы trades")

    connection.create_function("msk_to_unix_us", 1, migrate_exchange_time, deterministic=True)
    with connection:
        connection.execute("BEGIN IMMEDIATE")
        connection.execute(TABLE_SQL.format(name="trades_migrated"))
        connection.execute("""
            INSERT INTO trades_migrated (contract_code, exchange_ts_us, trade_num, price, qty, flags)
            SELECT contract_code, msk_to_unix_us(exchange_ts), trade_num, price, qty, flags
            FROM trades
        """)
        connection.execute("DROP TABLE trades")
        connection.execute("ALTER TABLE trades_migrated RENAME TO trades")


def read_batch(path):
    """Возвращает проверенные строки пакета path, сгруппированные по понедельникам ISO-недель."""
    by_week = defaultdict(list)
    with path.open("rb") as source:
        for number, line in enumerate(source, start=1):
            try:
                if not line.endswith(b"\n"):
                    raise ValueError("незавершённая строка")
                class_code, code, ts, trade, price, qty, flags = line.decode("ascii").rstrip("\r\n").split("\t")
                if class_code != "SPBFUT" or not CONTRACT_PATTERN.fullmatch(code):
                    raise ValueError("неизвестный класс или код контракта")
                timestamp = parse_exchange_time(ts)

                trade_num, quantity, flag_bits = int(trade), int(qty), int(flags)
                trade_price = float(price)
                if not (0 < trade_num < 2**63 and 0 < quantity < 2**63 and 0 <= flag_bits < 2**63):
                    raise ValueError("недопустимый номер, объём или флаги сделки")
                if not math.isfinite(trade_price):
                    raise ValueError("цена должна быть конечным числом")
            except ValueError as error:
                raise ValueError(f"{path.name}, строка {number}: {error}") from error

            day = timestamp.date()
            monday = day - timedelta(days=day.weekday())
            by_week[monday].append((code, to_unix_microseconds(timestamp), trade_num, trade_price, quantity, flag_bits))
    if not by_week:
        raise ValueError(f"{path.name}: пустой пакет")
    return by_week


@contextmanager
def lock_path(path):
    """Удерживает файловую блокировку path до выхода из блока."""
    with path.open("a+b") as lock:
        lock.seek(0)
        try:
            msvcrt.locking(lock.fileno(), msvcrt.LK_NBLCK, 1)
        except OSError as error:
            raise RuntimeError(f"Не удалось получить блокировку {path}. Проверьте запущенные сборщики.") from error
        try:
            yield
        finally:
            lock.seek(0)
            msvcrt.locking(lock.fileno(), msvcrt.LK_UNLCK, 1)


class TickStore:
    """Держит открытым одно соединение с недельной базой SQLite."""

    def __init__(self, data_dir):
        """Сохраняет каталог data_dir, в котором будут создаваться базы."""
        self.data_dir = data_dir
        self.week = None
        self.connection = None

    def get_connection(self, week):
        """Возвращает соединение с базой недели week, при необходимости создавая её."""
        if self.week == week:
            return self.connection

        self.close()
        path = self.data_dir / f"ticks_{week.isoformat()}.sqlite"
        connection = sqlite3.connect(path, timeout=10, isolation_level="IMMEDIATE")
        try:
            if connection.execute("PRAGMA journal_mode=WAL").fetchone()[0] != "wal":
                raise sqlite3.OperationalError(f"Не удалось включить WAL для {path}")
            connection.execute("PRAGMA synchronous=FULL")
            prepare_trade_table(connection)
        except BaseException:
            connection.close()
            raise

        self.week = week
        self.connection = connection
        return connection

    def import_batch(self, path):
        """Фиксирует сделки пакета path по неделям, удаляет пакет и возвращает число новых сделок."""
        by_week = read_batch(path)
        inserted = 0
        for week, rows in sorted(by_week.items()):
            connection = self.get_connection(week)
            before = connection.total_changes
            with connection:
                connection.executemany(INSERT_SQL, rows)
            inserted += connection.total_changes - before
        path.unlink()
        return inserted

    def close(self):
        """Закрывает текущее соединение с недельной базой."""
        if self.connection is not None:
            self.connection.close()
            self.connection = None
            self.week = None


def collect(args):
    """Обрабатывает пакеты по параметрам args, выводит сводку и возвращает код завершения."""
    args.spool_dir.mkdir(parents=True, exist_ok=True)
    args.data_dir.mkdir(parents=True, exist_ok=True)
    store = TickStore(args.data_dir)
    rejected = set()
    with lock_path(args.spool_dir / ".import.lock"), lock_path(args.data_dir / ".collector.lock"):
        try:
            print(f"Пакеты: {args.spool_dir}\nБазы: {args.data_dir}", flush=True)
            while True:
                imported = 0
                retry = False
                for path in sorted(args.spool_dir.glob("*.tsv")):
                    if path in rejected:
                        continue
                    try:
                        imported += store.import_batch(path)
                    except ValueError as error:
                        rejected.add(path)
                        print(f"Пакет оставлен для проверки: {error}", file=sys.stderr, flush=True)
                    except (OSError, sqlite3.Error) as error:
                        retry = True
                        print(f"Повтор импорта {path.name}: {error}", file=sys.stderr, flush=True)
                        break
                if imported:
                    processed_at = datetime.now().astimezone().isoformat(sep=" ", timespec="seconds")
                    print(f"[{processed_at}] Новых сделок: {imported}", flush=True)
                if args.once:
                    return int(bool(rejected) or retry)
                sleep(2 if retry else 0.25)
        finally:
            store.close()


def main():
    """Читает аргументы командной строки, запускает сборщик и возвращает код завершения."""
    parser = argparse.ArgumentParser(description="Запись сделок по ближайшим фьючерсам RTS и MIX из QUIK в SQLite.")
    parser.add_argument("--data-dir", type=Path, default=DEFAULT_DATA_DIR,
                        help=f"каталог недельных баз, по умолчанию {DEFAULT_DATA_DIR}")
    parser.add_argument("--spool-dir", type=Path, default=DEFAULT_SPOOL_DIR,
                        help="каталог готовых пакетов, по умолчанию spool в папке проекта")
    parser.add_argument("--once", action="store_true", help="импортировать готовые пакеты и завершить работу")
    try:
        return collect(parser.parse_args())
    except KeyboardInterrupt:
        print("Сборщик остановлен.", flush=True)
        return 0
    except (OSError, RuntimeError) as error:
        print(f"Ошибка сборщика: {error}", file=sys.stderr, flush=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
