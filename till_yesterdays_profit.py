from datetime import datetime, timedelta

import tzlocal
from apscheduler.schedulers.background import BackgroundScheduler
from apis.utils import get_computed_profit
from extensions import db
from main import app
from models.till_yesterdays_profit import TillYesterdaysProfit
import logging

log = logging.getLogger(__name__)
import pytz

from apscheduler.schedulers.blocking import BlockingScheduler

scheduler = BackgroundScheduler()


# @scheduler.scheduled_job("cron", day_of_week="mon-fri", hour=10, minute=42)
def update_till_yesterdays_profits():
    print("running jobs to update profit weekday")
    todays_date = datetime.today().date()
    if todays_date.weekday() < 5:
        with app.app_context():
            response = get_computed_profit()
            for strategy_profit in response["data"]:
                df = TillYesterdaysProfit(
                    strategy_id=strategy_profit["id"],
                    profit=strategy_profit["total"]["profit"],
                    date=todays_date,
                )
                db.session.add(df)

            try:
                db.session.commit()
                print("till yesterday profit updated")
            except Exception as e:
                print(f"Error occurred while updating {todays_date} profit: {e}")
                db.session.rollback()

update_till_yesterdays_profits()

# scheduler.start()

# @schedule.repeat(schedule.every(3).seconds)
# def log_time():
#     print("running jobs to update profit every weekday")
#     todays_date = datetime.today().date()
#     if todays_date.weekday() < 5:
#         with app.app_context():
#             response = get_computed_profit()
#             for strategy_profit in response["data"]:
#                 df = TillYesterdaysProfit(
#                     strategy_id=strategy_profit["id"],
#                     profit=strategy_profit["total"]["profit"],
#                     date=todays_date,
#                 )
#                 db.session.add(df)
#             try:
#                 db.session.commit()
#             except Exception as e:
#                 print(f"Error occurred while updating {todays_date} profit: {e}")
#                 db.session.rollback()
