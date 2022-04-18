from datetime import datetime

from apis.utils import get_computed_profit
from extensions import db
from main import app
from models.till_yesterdays_profit import TillYesterdaysProfit


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
                print("till yesterday profit upated")
            except Exception as e:
                print(f"Error occurred while updating {todays_date} profit: {e}")
                db.session.rollback()