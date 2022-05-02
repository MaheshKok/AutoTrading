import datetime
import logging
import time

from alice_blue import AliceBlue
from alice_blue import OrderType
from alice_blue import ProductType
from alice_blue import TransactionType
from sentry_sdk import capture_exception

from extensions import db
from models.broker import Broker

log = logging.getLogger(__name__)


def get_alice_blue_obj():
    from main import app

    with app.app_context():
        broker = Broker.query.filter_by(name="alice_blue").scalar()
        try:
            alice = AliceBlue(
                username=broker.username,
                password=broker.password,
                access_token=broker.access_token,
                master_contracts_to_download=["NFO"],
            )
        except:
            broker = (
                Broker.query.filter_by(name="alice_blue").with_for_update().scalar()
            )
            access_token = AliceBlue.login_and_get_access_token(
                username=broker.username,
                password=broker.password,
                twoFA="1994",
                api_secret=broker.api_secret,
                app_id=broker.app_id,
            )
            broker.access_token = access_token
            db.session.commit()

            alice = AliceBlue(
                username=broker.username,
                password=broker.password,
                access_token=access_token,
                master_contracts_to_download=["NFO"],
            )

        return alice


def close_alice_blue_trades(
    strike_quantity_dict,
    symbol,
    expiry: datetime.date,
    nfo_type,
    ongoing_trades,
    data,
    current_time,
        constructed_data
):
    """
    assumptions
     all trades to be executed should belong to same:
      symbol [ for ex: either BANKNIFTY, NIFTY ]
      expiry
      call type [ for ex: either CE, PE ]
      nfo type [ for ex: either future or option]
    """
    from apis.utils import close_ongoing_trades

    alice = get_alice_blue_obj()
    strike_option_type_close_order_id_dict = {}

    if isinstance(expiry, str):
        expiry = datetime.datetime.strptime(expiry, "%d %b %Y").date()

    for strike, quantity in strike_quantity_dict.items():
        option_type = "ce" if quantity > 0 else "pe"
        instrument = alice.get_instrument_for_fno(
            symbol=symbol,
            expiry_date=expiry,
            is_fut=nfo_type != "option",
            strike=strike,
            is_CE=option_type == "ce",
        )

        place_order_response = alice.place_order(
            transaction_type=TransactionType.Sell,
            instrument=instrument,
            quantity=quantity if quantity > 0 else (-1 * quantity),
            order_type=OrderType.Market,
            product_type=ProductType.Delivery,
            price=0.0,
            trigger_price=None,
            stop_loss=None,
            square_off=None,
            trailing_sl=None,
            is_amo=False,
        )
        strike_option_type_close_order_id_dict[
            f"{strike}_{option_type}"
        ] = place_order_response["data"]["oms_order_id"]

    strike_optiontype_exitprice_dict = {}
    # check for this
    for strike_option_type, order_id in strike_option_type_close_order_id_dict.items():
        order_history = alice.get_order_history(order_id)["data"][0]
        for _ in range(10):
            if order_history["order_status"] == "complete":
                strike_optiontype_exitprice_dict[strike_option_type] = order_history[
                    "average_price"
                ]
                break
            time.sleep(1)

    return close_ongoing_trades(
        ongoing_trades, constructed_data, current_time, data, strike_optiontype_exitprice_dict
    )


def buy_alice_blue_trades(self, data, quantity, expiry: datetime.date, nfo_type):
    """
    assumptions
     all trades to be executed should belong to same:
      symbol [ for ex: either BANKNIFTY, NIFTY ]
      expiry
      call type [ for ex: either CE, PE ]
      nfo type [ for ex: either future or option]
    """
    from main import telegram_bot
    from apis.utils import STATUS

    alice = get_alice_blue_obj()

    if isinstance(expiry, str):
        expiry = datetime.datetime.strptime(expiry, "%d %b %Y").date()

    quantity = quantity or data["quantity"]
    instrument = alice.get_instrument_for_fno(
        symbol=data["symbol"],
        expiry_date=expiry,
        is_fut=nfo_type != "option",
        strike=data["strike"],
        is_CE=quantity > 0,
    )

    place_order_response = alice.place_order(
        transaction_type=TransactionType.Buy,
        instrument=instrument,
        quantity=quantity if quantity > 0 else (-1 * quantity),
        order_type=OrderType.Market,
        product_type=ProductType.Delivery,
        price=0.0,
        trigger_price=None,
        stop_loss=None,
        square_off=None,
        trailing_sl=None,
        is_amo=False,
    )

    order_id = place_order_response["data"]["oms_order_id"]
    for _ in range(1, 10):
        order_status = alice.get_order_history(order_id)["data"][0]
        if order_status["order_status"] == STATUS.COMPLETE:
            data["entry_price"] = order_status["average_price"]
            return self.create_object(data, kwargs={})
        time.sleep(1)

    capture_exception(Exception(order_status["rejection_reason"], order_status))

    telegram_bot.send_message(chat_id="1229129389", text=order_status)
    log.warning(alice.get_order_history(order_id)["data"][0])


def get_order_status(alice, order_id):
    order_status = alice.get_order_history(order_id)["data"][0]
    if order_status["order_status"] == "complete":
        return "success"

    capture_exception(Exception(order_status["rejection_reason"], order_status))

    from main import telegram_bot

    telegram_bot.send_message(chat_id="1229129389", text=order_status)
    log.warning(alice.get_order_history(order_id)["data"][0])
    return "error"


# this is swastik algo syntax,

{
    "Apikey": "NOYM5GUM9P4JSFD6",
    "command": "PLACE_ORDER,AB012345,0,REGULAR,FNO,SBIN_26-DEP-2019_190_CE,BUY,MARKET,INTRADAY,1,0,0,0,0,0,0,DAY,FALSE,0,-1,Group ID,",
}
