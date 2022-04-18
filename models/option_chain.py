# Create data storage
from datetime import date

from extensions import db


class OptionChain(db.Model):
    __tablename__ = "option_chain"

    id = db.Column(db.Integer, primary_key=True)

    symbol = db.Column(db.String, nullable=False, default="BANKNIFTY")
    strike = db.Column(db.Integer, nullable=False, default=10000)
    celtp = db.Column(db.Float, nullable=True, default=0.0)
    peltp = db.Column(db.Float, nullable=True, default=0.0)
    atm = db.Column(db.Boolean, nullable=False, default=False)
    date = db.Column(db.Date, default=date.today())
