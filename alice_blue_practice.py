import datetime
from datetime import date
from time import sleep

from alice_blue import AliceBlue, LiveFeedType

app_id = "FgshPEm3jw"
secret = "9wg5GZzBhiOjVrf10OGYjwECrMVSnwecSLRtp8hsvJwyWDiGpFpTzOvTkwypJOcs"
username = "569619"
password = "SANkok@94"

# access_token = AliceBlue.login_and_get_access_token(
#     username=username, password=password, twoFA="1994", api_secret=secret, app_id=app_id
# )
# print(access_token)

start_time = datetime.datetime.now()

alice = AliceBlue(
    username,
    password,
    'M-A7f5UaoQAmP7RzP-I_iVco5adDpnhsNuSWU47gDFs.ycjDjeQkM_uypRhyCKd7tWUiOvRzp68oiNC_v7haohE',
    master_contracts_to_download=["NFO"],
)

print(f"time taken to create alice blue object: {datetime.datetime.now() - start_time}")


socket_opened = False


def event_handler_quote_update(message):
    print(f"quote update {message}")


def open_callback():
    global socket_opened
    socket_opened = True


alice.start_websocket(
    subscribe_callback=event_handler_quote_update,
    socket_open_callback=open_callback,
    run_in_background=True,
)


instrument = alice.get_instrument_for_fno(
    symbol="BANKNIFTY",
    expiry_date=date(2022, 4, 7),
    is_fut=True,
    strike=None,
    is_CE=False,
)

while True:
    alice.subscribe(instrument, LiveFeedType.MARKET_DATA)
    sleep(2)


alice.place_order(
    transaction_type=TransactionType.Buy,
    instrument=instrument,
    quantity=50,
    order_type=OrderType.Market,
    product_type=ProductType.Delivery,
    price=0.0,
    trigger_price=None,
    stop_loss=None,
    square_off=None,
    trailing_sl=None,
    is_amo=False,
)
