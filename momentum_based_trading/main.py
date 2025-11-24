import datetime
import sqlite3
import numpy as np

from momentum_based_trading.jobs import analyse


def main(
    cs: sqlite3.Cursor,
    conn: sqlite3.Connection,
    begin_on: datetime.date,
    settings: dict[str, float],
) -> None:
    cs.execute(f"select theday from prices where theday >= '{begin_on}'")
    days = [d[0] for d in cs.fetchall()]
    asset = {"old": np.nan, "new": np.nan}
    cash = {"old": np.nan, "new": np.nan}
    cs.execute("delete from positions where time_of_trade > '2020-01-01'")
    for d in days:
        asset["new"] = analyse(cs=cs, which_day=d, settings=settings)
        cs.execute(
            f"""
        select quantity, cash from positions
        where time_of_trade < '{d}'
        order by time_of_trade desc
        limit 1
        """
        )
        asset["old"], cash["old"] = cs.fetchall()[0]
        cs.execute(
            f"""
        select price from prices
        where theday <= '{d}'
        order by theday desc
        limit 1
        """
        )
        latest = cs.fetchall()[0][0]
        trade_size = round(asset["new"]) - round(asset["old"])
        if trade_size != 0 and abs(trade_size) > settings["min_trade_size"]:
            cash["new"] = cash["old"] - trade_size * latest
            cs.execute(
                f"""
            insert into positions values
            ('{d}', 'SP500', {round(asset['new'])}, {cash['new']})
            """
            )
        conn.commit()
