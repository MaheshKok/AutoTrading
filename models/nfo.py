# Create data storage
from datetime import datetime
from sqlalchemy.dialects.postgresql import UUID

from extensions import db


class NFO(db.Model):
    __tablename__ = "nfo"

    id = db.Column(db.Integer, primary_key=True)
    nfo_type = db.Column(db.String, nullable=False)

    # order detail
    quantity = db.Column(db.Integer, default=25)
    # Ex: 36000
    entry_price = db.Column(db.Float, nullable=False)
    # EX: 37000
    exit_price = db.Column(db.Float, nullable=True)
    # EX: 25000
    profit = db.Column(db.Float, nullable=True)

    # Future Details
    future_entry_price = db.Column(db.Float, nullable=True)
    future_exit_price = db.Column(db.Float, nullable=True)
    future_profit = db.Column(db.Float, nullable=True)

    # Timestamp of Order placed at
    placed_at = db.Column(
        db.TIMESTAMP(timezone=True), nullable=False, default=datetime.now()
    )
    # Timestamp of Order exited at
    exited_at = db.Column(db.TIMESTAMP(timezone=True), nullable=True)

    # option specific field
    # 34500
    strike = db.Column(db.Integer, nullable=True)
    # ce or pe
    option_type = db.Column(db.String, nullable=True)

    # strategy details
    strategy_id = db.Column(db.Integer, nullable=False)
    strategy_name = db.Column(db.String, nullable=False, default="RS[R0]")
    symbol = db.Column(db.String, nullable=False)

    expiry = db.Column(db.Date)

    broker_id = db.Column(
        UUID(as_uuid=True), db.ForeignKey("broker.id"), nullable=True
    )

    broker = db.relationship("Broker", backref="trades")
