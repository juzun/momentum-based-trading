import datetime
import sqlite3
import numpy as np

from momentum_based_trading.gbm import GBM


def position_size(
    cs: sqlite3.Cursor,
    which_day: datetime.date,
    forecast_interval: np.ndarray,
    es: float,
    settings: dict[str, float],
) -> int:
    cs.execute(
        f"""
    select quantity, cash from positions
    where instrument = 'SP500'
    and time_of_trade < '{which_day}'
    order by time_of_trade desc
    limit 1
    """
    )
    qty, cash = cs.fetchall()[0]
    cs.execute(
        f"""
    select price from prices
    where theday <= '{which_day}'
    order by theday desc
    limit 1
    """
    )
    price = cs.fetchall()[0][0]
    capital = cash + qty * price

    # Prevent division by very small es values which would lead to extreme risk-taking
    if abs(es) < 0.001:
        es = 0.001 if es >= 0 else -0.001
    exposure = capital * settings["risk_sizing"] / es

    if price < forecast_interval[0]:
        # Ensure we don't buy more than we can afford - no bankruptcy allowed
        desired_shares = round(exposure / price)
        max_shares = int(cash / price)
        return min(desired_shares, max_shares)
    elif price > forecast_interval[1]:
        # Ensure we don't sell more than we have - no short selling beyond current holdings
        desired_short = -round(exposure / price)
        max_short = -int(qty)
        return max(desired_short, max_short)
    else:
        return 0


def analyse(
    cs: sqlite3.Cursor, which_day: datetime.date, settings: dict[str, float]
) -> int:
    cs.execute(
        f"""
    select price from prices where theday <= '{which_day}'
    order by theday desc limit {settings['lookback']}
    """
    )
    p = np.flipud(np.asarray(cs.fetchall())).flatten()
    model = GBM()
    dt = 1.0 / 252
    model.calibrate(trajectory=p, dt=dt)
    n = settings["forecast_days"]
    t = n * dt
    forecast = model.forecast(latest=p[-1], t=t, confidence=settings["confidence"])
    es = model.expected_shortfall(t=t, confidence=settings["confidence"])
    return position_size(
        cs=cs,
        which_day=which_day,
        forecast_interval=forecast["interval"],
        es=es,
        settings=settings,
    )


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
