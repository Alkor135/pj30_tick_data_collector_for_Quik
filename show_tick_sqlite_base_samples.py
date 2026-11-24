r"""Показывает по пять крайних сделок из недельных файлов SQLite.

Читает первые строки самой ранней базы и последние строки самой поздней базы.
Сортирует по биржевому времени и показывает исходное число микросекунд и время МСК.
Заголовок времени МСК помечает вычисляемую колонку как отсутствующую в БД.
Открывает базы только для чтения, в том числе во время работы сборщика.

Запуск из корня проекта: .\.venv\Scripts\python.exe .\show_tick_sqlite_base_samples.py
Другой каталог: .\.venv\Scripts\python.exe .\show_tick_sqlite_base_samples.py --data-dir C:\data_quote\tick_data
"""

import argparse
from contextlib import closing
from datetime import timedelta
from operator import itemgetter
from pathlib import Path
import re
import sqlite3
import sys

from collect_ticks import DEFAULT_DATA_DIR, UNIX_EPOCH_MSK, migrate_exchange_time

WEEKLY_FILENAME = re.compile(r"ticks_\d{4}-\d{2}-\d{2}\.sqlite")
SAMPLE_SIZE = 5
HEADERS = ("contract_code", "exchange_ts_us", "Время МСК (отсутствует в БД)", "trade_num", "price", "qty", "flags")


def contract_codes(database):
    """Возвращает коды контрактов из database, переходя к следующему ключу SQLite."""
    query = "SELECT min(contract_code) FROM trades"
    code = database.execute(query).fetchone()[0]
    while code is not None:
        yield code
        code = database.execute(f"{query} WHERE contract_code > ?", (code,)).fetchone()[0]


def select_edge(database, codes, time_column, *, latest):
    """Возвращает до пяти крайних сделок из database по указанным codes и полю time_column.

    Если latest истинно, выбирает самые поздние сделки. Результат отсортирован от ранних к поздним.
    """
    direction = "DESC" if latest else "ASC"
    query = f"""
        SELECT contract_code, {time_column}, trade_num, price, qty, flags
        FROM trades
        WHERE contract_code = ?
        ORDER BY {time_column} {direction}, trade_num {direction}
        LIMIT ?
    """
    # Первичный ключ начинается с кода контракта, затем идут время и номер сделки.
    candidates = [row for code in codes for row in database.execute(query, (code, SAMPLE_SIZE))]
    rows = sorted(candidates, key=itemgetter(1, 0, 2), reverse=latest)[:SAMPLE_SIZE]
    if latest:
        rows.reverse()
    return rows


def read_week(path, *, first=False, last=False):
    """Читает файл path и возвращает пару списков для запрошенных секций first и last."""
    uri = path.resolve().as_uri() + "?mode=ro"
    try:
        with closing(sqlite3.connect(uri, uri=True)) as database:
            database.execute("BEGIN")
            columns = {row[1] for row in database.execute("PRAGMA table_info(trades)")}
            if "exchange_ts_us" in columns:
                time_column = "exchange_ts_us"
            elif "exchange_ts" in columns:
                time_column = "exchange_ts"
            else:
                raise sqlite3.OperationalError("в таблице trades нет поля биржевого времени")
            codes = tuple(contract_codes(database))
            first_rows = select_edge(database, codes, time_column, latest=False) if first else []
            last_rows = select_edge(database, codes, time_column, latest=True) if last else []
            return first_rows, last_rows
    except sqlite3.Error as error:
        raise RuntimeError(f"{path.name} — {error}") from error


def print_table(title, path, rows):
    """Печатает заголовок title, имя базы path и таблицу сделок rows."""
    print(f"{title} — {path.name}")
    if not rows:
        print("Таблица trades пуста.")
        return

    table = [HEADERS]
    for code, timestamp, number, price, quantity, flags in rows:
        timestamp_us = migrate_exchange_time(timestamp) if isinstance(timestamp, str) else timestamp
        timestamp_msk = UNIX_EPOCH_MSK + timedelta(microseconds=timestamp_us)
        table.append((code, str(timestamp_us), timestamp_msk.isoformat(sep=" ", timespec="microseconds"),
                      str(number), str(price), str(quantity), str(flags)))

    widths = [max(map(len, column)) for column in zip(*table)]
    lines = [" | ".join(value.ljust(width) for value, width in zip(row, widths)).rstrip() for row in table]
    print(lines[0])
    print("-+-".join("-" * width for width in widths))
    print("\n".join(lines[1:]))


def main():
    """Читает каталог из аргументов, показывает обе выборки и возвращает код завершения."""
    parser = argparse.ArgumentParser(description="Просмотр первых и последних сделок в недельных базах SQLite.")
    parser.add_argument("--data-dir", type=Path, default=DEFAULT_DATA_DIR,
                        help=f"каталог недельных баз, по умолчанию {DEFAULT_DATA_DIR}")
    data_dir = parser.parse_args().data_dir

    try:
        files = sorted(path for path in data_dir.glob("ticks_*.sqlite")
                       if WEEKLY_FILENAME.fullmatch(path.name) and path.is_file())
        if not files:
            print(f"В каталоге {data_dir} нет недельных файлов ticks_ГГГГ-ММ-ДД.sqlite.")
            return 0

        oldest, newest = files[0], files[-1]
        first_rows, last_rows = read_week(oldest, first=True, last=oldest == newest)
        if oldest != newest:
            _, last_rows = read_week(newest, last=True)
        print_table("Первые сделки", oldest, first_rows)
        print()
        print_table("Последние сделки", newest, last_rows)
        return 0
    except (OSError, RuntimeError, ValueError, OverflowError) as error:
        print(f"Ошибка просмотра базы — {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
