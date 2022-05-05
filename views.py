# Create endpoints
import datetime
import logging
import threading
import time

from flask import jsonify
from flask_rest_jsonapi import Api

from apis.nfo import NFODetail
from apis.nfo import NFOList
from apis.option_chain import OptionChainDetail
from apis.option_chain import OptionChainList
from apis.till_yesterdays_profit import TillYesterdaysProfitDetail
from apis.till_yesterdays_profit import TillYesterdaysProfitList
from apis.utils import close_all_trades
from apis.utils import get_computed_profit
from apis.utils import get_constructed_data
from extensions import db
from models.option_chain import OptionChain

log = logging.getLogger(__name__)


def update_option_chain(symbol="BANKNIFTY"):
    print(f"dumping option chain at: {datetime.datetime.now().time()}")
    constructed_data = get_constructed_data(symbol=symbol)

    option_chain_db_stkprc_list = [
        r[0]
        for r in OptionChain.query.with_entities(OptionChain.strike, OptionChain.id)
        .filter(OptionChain.symbol == symbol)
        .all()
    ]

    update_mappings = []
    for strike, id in option_chain_db_stkprc_list:

        if f"{strike}_pe" in constructed_data:
            data_to_update = {
                "id": id,
                "peltp": constructed_data[f"{strike}_pe"],
                "celtp": constructed_data[f"{strike}_ce"],
            }

            if strike == constructed_data["atm"]:
                data_to_update.update({"atm": True})
            update_mappings.append(data_to_update)

    db.session.bulk_update_mappings(OptionChain, update_mappings)

    # if insert_mappings:
    #     db.session.bulk_insert_mappings(OptionChain, insert_mappings)

    db.session.commit()


def print_num(num):
    for i in range(num):
        print(i)


def register_base_routes(app):
    @app.route("/")
    def index():
        response = "Hello from a public endpoint! You don't need to be authenticated to see this."
        app.logger.debug('this is a DEBUG message')
        return jsonify(message=response)

    @app.route("/api/schedule/dump_option_chain")
    def dump_option_chain():
        schedule.every(15).seconds.do(update_option_chain)
        while True:
            schedule.run_pending()
            time.sleep(1)

    @app.route("/api/profit")
    def compute_profit():
        return get_computed_profit()

    @app.route("/api/profit/<strategy_id>")
    def compute_strategys_profit(strategy_id):
        return get_computed_profit(strategy_id)

    @app.route("/api/error")
    def raise_error():
        from sentry_sdk import capture_exception
        try:
            a = 1/0
        except Exception as e:
            capture_exception(Exception("this is random exception"))
            return "exception raised"

    # @app.route("/api/close_trades/<strategy_id>")
    # def close_specific_trades(strategy_id):
    #     return close_all_trades(strategy_id)
    #
    # @app.route("/api/close_trades")
    # def close_trades():
    #     return close_all_trades()

    @app.route("/api/thread")
    def test_threading():
        thread_1 = threading.Thread(target=print_num, args=(10,))
        thread_2 = threading.Thread(target=print_num, args=(20,))
        thread_1.start()
        thread_2.start()
        return "thread started successfully"

    @app.route("/api/testing")
    def test_threading():
        log.warning("this is testing url")
        return "success"


def register_json_routes(app):
    api = Api(app)

    # Expected Payload
    {
        "data": {
            "type": "option",
            "attributes": {
                "strategy": 1,  # mandatory
                "nfo_type": "option",  # mandatory for now
                "option_type": "ce",
                "action": "buy",  # mandatory
                "strike_price": 550,  # if not provided ATM strike price will be picked
                "symbol": "BANKNIFTY",  # its optional,
            },
        }
    }

    api.route(NFOList, "nfo_list", "/api/nfo")
    api.route(NFODetail, "nfo_detail", "/api/nfo/<int:id>")

    api.route(TillYesterdaysProfitList, "till_yesterdays_profit_list", "/api/till_yesterdays_profit")
    api.route(TillYesterdaysProfitDetail, "till_yesterdays_profit_detail", "/api/till_yesterdays_profit/<int:id>")

    api.route(OptionChainList, "option_chain_list", "/api/option_chain")
    api.route(OptionChainDetail, "option_chain_detail", "/api/nfo/<int:id>")
