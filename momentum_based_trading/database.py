import csv
import sqlite3
from pathlib import Path
from contextlib import closing


def prepare(cs: sqlite3.Cursor, conn: sqlite3.Connection):
    cs.execute(
        """
    create table if not exists prices (
    theday text primary key,
    price real
    )
    """
    )
    cs.execute("delete from prices")
    conn.commit()
    with closing(open(Path.cwd().parent / "data" / "SP500.csv")) as datafile:
        reader = csv.DictReader(datafile, fieldnames=["date", "price"], delimiter="\t")
        for row in reader:
            cs.execute(
                f"insert into prices values (\"{row['date']}\", {float(row['price'])})"
            )
    conn.commit()

    cs.execute(
        """
    create table if not exists positions (
    time_of_trade text,
    instrument text,
    quantity real,
    cash real,
    primary key (time_of_trade, instrument)
    )
    """
    )
    cs.execute(
        """
    insert or ignore into positions values
    ('1666-01-01', 'SP500', 0, 1000000)
    """
    )
    conn.commit()
