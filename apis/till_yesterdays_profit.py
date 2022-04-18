# Create resource managers
from flask_rest_jsonapi import ResourceDetail, ResourceList

from extensions import db
from models.till_yesterdays_profit import TillYesterdaysProfit
from schema.till_yesterdays_profit import TillYesterdaysProfitSchema


class TillYesterdaysProfitList(ResourceList):
    methods = ["GET"]

    schema = TillYesterdaysProfitSchema
    data_layer = {"session": db.session, "model": TillYesterdaysProfit}


class TillYesterdaysProfitDetail(ResourceDetail):
    methods = ["GET"]

    schema = TillYesterdaysProfitSchema
    data_layer = {"session": db.session, "model": TillYesterdaysProfit}
