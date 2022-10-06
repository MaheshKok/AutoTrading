import threading
from datetime import datetime
from typing import List

import requests
from sqlalchemy.util._collections import _LW

from apis.broker.alice_blue import buy_alice_blue_trades
from apis.broker.alice_blue import close_alice_blue_trades
from apis.constants import ACTION
from apis.constants import BROKER
from apis.constants import NFO_TYPE
from apis.constants import OPTION_TYPE
from extensions import db
from models.completed_profit import CompletedProfit
from models.nfo import NFO

EXPIRY_LISTS = [
    "29 SEP 2022",
    "06 OCT 2022",
    "13 OCT 2022",
    "20 OCT 2022",
    "27 OCT 2022",
    "03 NOV 2022",
    "10 NOV 2022",
    "17 NOV 2022",
    "24 NOV 2022",
    "01 DEC 2022",
    "08 DEC 2022",
    "15 DEC 2022",
    "22 DEC 2022",
    "29 DEC 2022",
]


class STATUS:
    SUCCESS = "success"
    ERROR = "error"
    COMPLETE = "complete"
    REJECTED = "rejected"
    VALIDATION_PENDING = "validation pending"


def get_current_expiry():
    today_date = datetime.now().date()
    for expiry_str in EXPIRY_LISTS:
        expiry_date = datetime.strptime(expiry_str, "%d %b %Y").date()
        if today_date <= expiry_date:
            return expiry_str
    return None


def get_profit(trade, ltp):
    if trade.quantity > 0:
        b, a = ltp, trade.entry_price
    else:
        b, a = trade.entry_price, ltp

    # TODO charges to be deducted should be dynamic because in future apart from Bank and Nifty we will have others F&O
    return (b - a) * trade.quantity - 30


def get_future_profit(trade, ltp):
    # TODO charges to be deducted should be dynamic because in future apart from Bank and Nifty we will have others F&O
    return (ltp - trade.future_entry_price) * trade.quantity - 320


def buy_or_sell_future(self, data: dict):
    last_trade = NFO.query.filter_by(
        strategy_id=data["strategy_id"], exited_at=None, nfo_type="future"
    ).scalar()

    ltp = data["entry_price"]

    if last_trade:
        last_trade.exit_price = ltp
        last_trade.profit = get_profit(last_trade, ltp)
        last_trade.exited_at = datetime.now()
        db.session.commit()
        db.session.refresh(last_trade)

    data["entry_price"] = ltp
    # just in case we receive strike price as an additional attribute delete it
    if data.get("strike_price"):
        del data["strike_price"]

    if data.get("option_type"):
        del data["option_type"]

    if data.get("strike"):
        del data["strike"]

    obj = self.create_object(data, kwargs={})
    return last_trade, obj


def get_constructed_data(symbol="BANKNIFTY", expiry=None):
    options_data_lst = fetch_data(symbol, expiry)

    constructed_data = {}
    # strikeprice cannot be a float value for banknifty so remove decimals
    for option_data in options_data_lst:
        strike_price = option_data["stkPrc"][:-2]
        constructed_data.update(
            {
                f"{strike_price}_ce": float(option_data["ceQt"]["ltp"]),
                f"{strike_price}_pe": float(option_data["peQt"]["ltp"]),
            }
        )

        if option_data["atm"]:
            constructed_data.update({"atm": option_data["stkPrc"]})

    return constructed_data


def get_final_data_to_ingest(data, expiry, current_time, constructed_data):
    option_type = OPTION_TYPE.CE if data["action"] == ACTION.BUY else OPTION_TYPE.PE
    data["option_type"] = option_type
    strike_price = data.get("strike_price")
    if strike := data.get("strike"):
        data["entry_price"] = constructed_data[f"{strike}_{option_type}"]
    elif strike_price:
        # get the strike price which is just less than the payload strike price
        entry_price, strike, prev_val = 0, 0, 0
        for key, value in constructed_data.items():
            if option_type == "ce" and option_type in key:
                if float(value) != 0.0 and float(value) <= strike_price:
                    entry_price, strike = value, key.split("_")[0]
                    break
            elif option_type == "pe" and option_type in key:
                if float(value) != 0.0 and float(value) >= strike_price:
                    entry_price, strike = prev_val[1], prev_val[0].split("_")[0]
                    break
                prev_val = (key, value)

        data["entry_price"] = entry_price
        data["strike"] = strike
        # strike_price doesnt make entry to database its only for selection of exact strike price which is entry price
        del data["strike_price"]
    else:
        if data["symbol"] in ["BANKNIFTY", "NIFTY"] or data.get("atm"):
            strike = constructed_data["atm"]
            data["strike"] = int(float(strike))
            data["entry_price"] = constructed_data[
                f'{strike.split(".")[0]}_{option_type}'
            ]
        else:
            # TODO not completed yet, need to decide which one to buy atm or with most vol
            options_data_lst = fetch_data(data["symbol"])
            max_vol = 0
            max_vol_strike = None
            max_vol_strike_price = None
            for option_data in options_data_lst:
                option_vol = float(option_data[f"{option_type}Qt"]["vol"])
                if option_vol > max_vol:
                    max_vol = option_vol
                    max_vol_strike = option_data["stkPrc"]
                    max_vol_strike_price = option_data[f"{option_type}Qt"]["ltp"]

            data["strike"] = int(float(max_vol_strike))
            data["entry_price"] = max_vol_strike_price

    if data.get("future_price"):
        del data["future_price"]

    if data.get("action"):
        data["quantity"] = (
            data["quantity"] if data["action"] == ACTION.BUY else -data["quantity"]
        )
        del data["action"]

    if data.get("atm"):
        del data["atm"]

    data["placed_at"] = current_time
    data["expiry"] = expiry

    return data


def close_ongoing_trades(
    ongoing_trades, constructed_data, current_time, data, broker_data=None
):
    from main import app

    mappings = []
    total_profit = 0
    with app.app_context():
        for trade in ongoing_trades:
            exit_price = constructed_data[f"{trade.strike}_{trade.option_type}"]
            if broker_data:
                exit_price = (
                    broker_data.get(f"{trade.strike}_{trade.option_type}") or exit_price
                )

            profit = get_profit(trade, exit_price)
            future_exit_price = data.get("future_entry_price", 0)
            future_profit = (
                get_future_profit(trade, future_exit_price)
                if trade.future_entry_price
                else 0
            )
            mappings.append(
                {
                    "id": trade.id,
                    "profit": profit,
                    "exit_price": exit_price,
                    "exited_at": current_time,
                    "future_exit_price": future_exit_price,
                    "future_profit": future_profit,
                }
            )
            total_profit += profit

        if cp := CompletedProfit.query.filter_by(
            strategy_id=trade.strategy_id
        ).scalar():
            cp.profit += total_profit
            cp.trades += len(ongoing_trades)
        else:
            cp = CompletedProfit(
                profit=total_profit,
                strategy_id=trade.strategy_id,
                trades=len(ongoing_trades),
            )
            db.session.add(cp)

        db.session.bulk_update_mappings(NFO, mappings)
        db.session.commit()
        # db.session.refresh(ongoing_trades)

        return ongoing_trades


def get_current_and_next_expiry():
    todays_date = datetime.now().date()
    is_today_expiry = False
    current_expiry_str = None
    next_expiry_str = None
    for index, expiry_str in enumerate(EXPIRY_LISTS):
        expiry_date = datetime.strptime(expiry_str, "%d %b %Y").date()
        if todays_date > expiry_date:
            continue
        elif expiry_date == todays_date:
            next_expiry_str = EXPIRY_LISTS[index + 1]
            current_expiry_str = expiry_str
            is_today_expiry = True
            break
        elif todays_date < expiry_date:
            current_expiry_str = expiry_str
            break

    return current_expiry_str, next_expiry_str, is_today_expiry


def get_aggregated_trades(trades: List[NFO]):
    aggregated_trades = {}
    for trade in trades:
        aggregated_trades[trade.strike] = (
            aggregated_trades.get(trade.strike, 0) + trade.quantity
        )
    return aggregated_trades


def task_buying_trade_of_next_expiry_on_expiry_day(
    self,
    today_expirys_ongoing_trades,
    data,
    next_expiry,
    current_time,
):
    payload_action = data["action"]
    current_expirys_ongoing_action = (
        ACTION.BUY if today_expirys_ongoing_trades[0].quantity > 0 else ACTION.SELL
    )

    constructed_data = get_constructed_data(data["symbol"], expiry=next_expiry)

    args = [self, data, next_expiry, current_time, constructed_data]
    if current_expirys_ongoing_action == payload_action:
        data["quantity"] = (
            sum(trade.quantity for trade in today_expirys_ongoing_trades)
            + today_expirys_ongoing_trades[0].quantity
        )

    # this is important
    if payload_action == ACTION.SELL:
        data["quantity"] = -data["quantity"]

    return task_buying_trade(*args)


def close_trades(data, ongoing_trades, expiry, current_time, constructed_data):
    if broker_id := data.get("broker_id"):
        strike_quantity_dict = get_aggregated_trades(ongoing_trades)
        if broker_id == BROKER.alice_blue_id:
            return close_alice_blue_trades(
                strike_quantity_dict,
                data["symbol"],
                expiry,
                NFO_TYPE.OPTION,
                ongoing_trades,
                data,
                current_time,
                constructed_data,
            )
    return close_ongoing_trades(ongoing_trades, constructed_data, current_time, data)


def task_closing_trade(data, expiry, current_time, constructed_data, close_it=False):
    from main import app

    with app.app_context():
        ongoing_trades = NFO.query.filter(
            NFO.strategy_id == data["strategy_id"],
            NFO.exited_at == None,
            NFO.nfo_type == NFO_TYPE.OPTION,
            NFO.symbol == data["symbol"],
            NFO.expiry == expiry,
            NFO.placed_at != current_time,
        ).all()

    args = [data, ongoing_trades, expiry, current_time, constructed_data]
    if close_it:
        return close_trades(*args)

    if ongoing_trades and ongoing_trades[0].option_type != data["option_type"]:
        return close_trades(*args)


def task_buying_trade(self, payload_data, expiry, current_time, constructed_data):
    from main import app

    data = get_final_data_to_ingest(
        data=payload_data,
        expiry=expiry,
        current_time=current_time,
        constructed_data=constructed_data,
    )

    with app.app_context():
        if not (broker_id := data.get("broker_id")):
            return self.create_object(data, kwargs={})

        if broker_id == BROKER.alice_blue_id:
            return buy_alice_blue_trades(
                self,
                data,
                expiry,
                NFO_TYPE.OPTION,
            )


def handle_buy_and_sell_trade(self, data, expiry, current_time):
    constructed_data = get_constructed_data(data["symbol"], expiry=expiry)
    closed_trades = threading.Thread(
        target=task_closing_trade, args=(data, expiry, current_time, constructed_data)
    ).start()
    bought_trades = threading.Thread(
        target=task_buying_trade,
        args=(self, data, expiry, current_time, constructed_data),
    ).start()
    return (*closed_trades, bought_trades) if closed_trades else [bought_trades]


def buy_or_sell_option(self, data: dict):
    """
    if today's expiry:
        if we have today's expiry ongoing trades
            close out all ongoing trades
            if current expiry's ongoing action == payload action
                the amount of trades we closed buy that much amount of trades + 1 of the next expiry
            else
                buy new trade of the next expiry
        if next expiry's ongoing trades
            if next expiry's ongoing trades action != payload action
                close out all trades of next expiry
        buy new trade of the next expiry
    else:
        if today expiry's ongoing trades
            if today expiry's ongoing trades action != payload action
                close out all trades
        buy new trade of the same expiry

    """
    current_time = datetime.now()
    current_expiry, next_expiry, todays_expiry = get_current_and_next_expiry()
    data["option_type"] = (
        OPTION_TYPE.CE if data["action"] == ACTION.BUY else OPTION_TYPE.PE
    )

    if not todays_expiry:
        return handle_buy_and_sell_trade(self, data, current_expiry, current_time)

    if today_expirys_ongoing_trades := NFO.query.filter_by(
        strategy_id=data["strategy_id"],
        exited_at=None,
        nfo_type=NFO_TYPE.OPTION,
        symbol=data["symbol"],
        expiry=current_expiry,
    ).all():
        constructed_data = get_constructed_data(data["symbol"], expiry=current_expiry)
        return *task_closing_trade(
            data, current_expiry, current_time, constructed_data, close_it=True
        ), task_buying_trade_of_next_expiry_on_expiry_day(
            self, today_expirys_ongoing_trades, data, next_expiry, current_time
        )

    return handle_buy_and_sell_trade(self, data, next_expiry, current_time)


def get_computed_profit_without_fetching_completed_profit(strategy_id=None):
    if strategy_id:
        constructed_data = get_constructed_data(
            symbol=NFO.query.filter_by(strategy_id=strategy_id).first().symbol,
        )
    else:
        bank_nifty_constructed_data = get_constructed_data(symbol="BANKNIFTY")
        nifty_constructed_data = get_constructed_data(symbol="NIFTY")
        # axis_bank_constructed_data = get_constructed_data(symbol="AXISBANK")
        # sbi_constructed_data = get_constructed_data(symbol="SBIN")
        # bajajauto_constructed_data = get_constructed_data(symbol="BAJAJ-AUTO")

    data = []

    total_profits = 0
    total_completed_profits = 0
    total_ongoing_profits = 0
    for _strategy_id in (
        [strategy_id]
        if strategy_id
        else (NFO.query.with_entities(NFO.strategy_id).distinct(NFO.strategy_id).all())
    ):
        ongoing_profit, completed_profit, completed_trades, ongoing_trades = 0, 0, 0, 0

        ongoing_action = None
        for nfo in NFO.query.filter_by(strategy_id=_strategy_id).all():
            if strategy_id:
                constructed_data = constructed_data
            elif nfo.symbol == "BANKNIFTY":
                constructed_data = bank_nifty_constructed_data
            elif nfo.symbol == "NIFTY":
                constructed_data = nifty_constructed_data
            # elif nfo.symbol == "AXISBANK":
            #     constructed_data = axis_bank_constructed_data
            # elif nfo.symbol == "SBIN":
            #     constructed_data = sbi_constructed_data
            # elif nfo.symbol == "BAJAJ-AUTO":
            #     constructed_data = bajajauto_constructed_data
            else:
                continue

            if nfo.exited_at:
                completed_profit += nfo.profit
                completed_trades += 1
            else:
                ongoing_profit += get_profit(
                    nfo,
                    float(constructed_data[f"{nfo.strike}_{nfo.option_type}"]),
                )
                ongoing_action = ACTION.BUY if nfo.quantity > 0 else ACTION.SELL
                ongoing_trades += 1

        total_strategy_profits = completed_profit + ongoing_profit
        total_profits += total_strategy_profits
        total_completed_profits += completed_profit
        total_ongoing_profits += ongoing_profit
        data.append(
            {
                "id": _strategy_id[0],
                "name": nfo.strategy_name,
                "completed": {
                    "trades": completed_trades,
                    "profit": round(completed_profit, 2),
                },
                "on_going": {
                    "trades": ongoing_trades,
                    "profit": round(ongoing_profit, 2),
                    "action": ongoing_action,
                },
                "total": {
                    "trades": ongoing_trades + completed_trades,
                    "profit": round(total_strategy_profits, 2),
                },
            }
        )

    return {
        "data": data,
        "meta": {
            "total_profits": round(total_profits, 2),
            "total_completed_profits": round(total_completed_profits, 2),
            "total_ongoing_profits": round(total_ongoing_profits, 2),
        },
    }


def get_computed_profit(strategy_id=None):
    current_expiry_str, next_expiry_str, todays_expiry = get_current_and_next_expiry()
    current_expiry_date = datetime.strptime(current_expiry_str, "%d %b %Y").date()
    if todays_expiry:
        next_expiry_date = datetime.strptime(next_expiry_str, "%d %b %Y").date()

    if strategy_id:
        if todays_expiry:
            next_expiry_constructed_data = get_constructed_data(
                symbol=NFO.query.filter_by(strategy_id=strategy_id).first().symbol,
                expiry=next_expiry_str,
            )
        current_expiry_constructed_data = get_constructed_data(
            symbol=NFO.query.filter_by(strategy_id=strategy_id).first().symbol,
            expiry=current_expiry_str,
        )
    else:
        if todays_expiry:
            bank_nifty_next_expiry_constructed_data = get_constructed_data(
                symbol="BANKNIFTY", expiry=next_expiry_str
            )
            nifty_next_expiry_constructed_data = get_constructed_data(
                symbol="NIFTY", expiry=next_expiry_str
            )

        bank_nifty_current_expiry_constructed_data = get_constructed_data(
            symbol="BANKNIFTY", expiry=current_expiry_str
        )
        nifty_current_expiry_constructed_data = get_constructed_data(
            symbol="NIFTY", expiry=current_expiry_str
        )

        # axis_bank_constructed_data = get_constructed_data(symbol="AXISBANK")
        # sbi_constructed_data = get_constructed_data(symbol="SBIN")
        # bajajauto_constructed_data = get_constructed_data(symbol="BAJAJ-AUTO")

    data = []

    completed_profit_dict = dict(
        CompletedProfit.query.with_entities(
            CompletedProfit.strategy_id, CompletedProfit
        ).all()
    )
    total_ongoing_profits = 0

    for _strategy_id in (
        [strategy_id]
        if strategy_id
        else (
            NFO.query.with_entities(NFO.strategy_id)
            .filter_by(exited_at=None)
            .distinct(NFO.strategy_id)
            .all()
        )
    ):
        if isinstance(_strategy_id, _LW):
            _strategy_id = _strategy_id[0]

        ongoing_profit, completed_profit, completed_trades, ongoing_trades = 0, 0, 0, 0

        query = NFO.query.filter_by(exited_at=None, strategy_id=_strategy_id)
        nfo = None
        ongoing_action = None
        for nfo in query.all():
            if strategy_id:
                if nfo.expiry == current_expiry_date:
                    constructed_data = current_expiry_constructed_data
                else:
                    constructed_data = next_expiry_constructed_data
            elif nfo.symbol == "BANKNIFTY":
                if nfo.expiry == current_expiry_date:
                    constructed_data = bank_nifty_current_expiry_constructed_data
                else:
                    constructed_data = bank_nifty_next_expiry_constructed_data

            elif nfo.symbol == "NIFTY":
                if nfo.expiry == current_expiry_date:
                    constructed_data = nifty_current_expiry_constructed_data
                else:
                    constructed_data = nifty_next_expiry_constructed_data

            else:
                continue

            ongoing_profit += get_profit(
                nfo,
                float(constructed_data[f"{nfo.strike}_{nfo.option_type}"]),
            )
            ongoing_action = ACTION.BUY if nfo.quantity > 0 else ACTION.SELL
            ongoing_trades += 1

        if completed_profit_dict.get(_strategy_id):
            cp_obj = completed_profit_dict[_strategy_id]
            completed_profit = cp_obj.profit
            completed_trades = cp_obj.trades

        total_strategy_profits = ongoing_profit + completed_profit

        total_ongoing_profits += ongoing_profit

        if not nfo:
            nfo = NFO.query.filter_by(strategy_id=_strategy_id).first()

        data.append(
            {
                "id": _strategy_id,
                "name": nfo.strategy_name,
                "completed": {
                    "trades": completed_trades,
                    "profit": completed_profit,
                },
                "on_going": {
                    "trades": ongoing_trades,
                    "profit": round(ongoing_profit, 2),
                    "action": ongoing_action,
                },
                "total": {
                    "trades": ongoing_trades + completed_trades,
                    "profit": round(total_strategy_profits, 2),
                },
            }
        )

    total_completed_profits = sum(
        cp_obj.profit for cp_obj in completed_profit_dict.values()
    )

    total_profits = total_completed_profits + total_ongoing_profits
    return {
        "data": data,
        "meta": {
            "total_profits": round(total_profits, 2),
            "total_completed_profits": round(total_completed_profits, 2),
            "total_ongoing_profits": round(total_ongoing_profits, 2),
        },
    }


def close_all_trades(strategy_id):
    # TODO: handle manual closing of brokers trades as well
    current_expiry, next_expiry, todays_expiry = get_current_and_next_expiry()
    exited_at = datetime.now()
    bank_nifty_current_expiry_constructed_data = get_constructed_data(
        symbol="BANKNIFTY"
    )
    nifty_current_expiry_constructed_data = get_constructed_data(symbol="NIFTY")

    # axis_bank_constructed_data = get_constructed_data(symbol="AXISBANK")
    # sbi_constructed_data = get_constructed_data(symbol="SBIN")
    # bajajauto_constructed_data = get_constructed_data(symbol="BAJAJ-AUTO")

    banknifty_ongoing_trades = NFO.query.filter_by(
        exited_at=None,
        symbol="BANKNIFTY",
        strategy_id=strategy_id,
        expiry=current_expiry,
        broker_id=None,
    ).all()
    nifty_ongoing_trades = NFO.query.filter_by(
        exited_at=None,
        symbol="NIFTY",
        strategy_id=strategy_id,
        expiry=current_expiry,
        broker_id=None,
    ).all()
    ongoing_trades = {
        "BANKNIFTY": banknifty_ongoing_trades,
        "NIFTY": nifty_ongoing_trades,
    }

    for symbol, value in ongoing_trades.items():
        if value:
            args = (
                [
                    banknifty_ongoing_trades,
                    bank_nifty_current_expiry_constructed_data,
                ]
                if symbol == "BANKNIFTY"
                else [nifty_ongoing_trades, nifty_current_expiry_constructed_data]
            )

            close_ongoing_trades(
                *args,
                exited_at,
                {},
            )

    if todays_expiry:
        bank_nifty_next_expiry_constructed_data = get_constructed_data(
            symbol="BANKNIFTY", expiry=next_expiry
        )
        nifty_next_expiry_constructed_data = get_constructed_data(
            symbol="NIFTY", expiry=next_expiry
        )

        banknifty_ongoing_trades = NFO.query.filter_by(
            exited_at=None,
            symbol="BANKNIFTY",
            strategy_id=strategy_id,
            expiry=next_expiry,
            broker_id=None,
        ).all()
        nifty_ongoing_trades = NFO.query.filter_by(
            exited_at=None,
            symbol="NIFTY",
            strategy_id=strategy_id,
            expiry=next_expiry,
            broker_id=None,
        ).all()
        ongoing_trades = {
            "BANKNIFTY": banknifty_ongoing_trades,
            "NIFTY": nifty_ongoing_trades,
        }

        for symbol, value in ongoing_trades.items():
            if value:
                args = (
                    [
                        banknifty_ongoing_trades,
                        bank_nifty_next_expiry_constructed_data,
                    ]
                    if symbol == "BANKNIFTY"
                    else [nifty_ongoing_trades, nifty_next_expiry_constructed_data]
                )

                close_ongoing_trades(
                    *args,
                    exited_at,
                    {},
                )
    return "All trades closed successfully"


def fetch_data(symbol="BANKNIFTY", expiry=None):
    if symbol in ["BANKNIFTY", "NIFTY"]:
        atyp = "OPTIDX"
        if not expiry:
            expiry = get_current_expiry()
    else:
        atyp = "OPTSTK"
        # TODO add logic here as well
        expiry = "24 FEB 2022"

    return requests.post(
        "https://ewmw.edelweiss.in/api/Market/optionchaindetails",
        data={"exp": expiry, "aTyp": atyp, "uSym": symbol},
    ).json()["opChn"]


# wpp martin suarel
#
# vivek bharghava
# dentsu
