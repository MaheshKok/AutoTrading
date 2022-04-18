# Create resource managers
from flask_rest_jsonapi import ResourceDetail, ResourceList

from extensions import db
from models.option_chain import OptionChain
from schema.option_chain import OptionChainSchema


class OptionChainList(ResourceList):
    methods = ["GET"]
    schema = OptionChainSchema
    data_layer = {"session": db.session, "model": OptionChain}


class OptionChainDetail(ResourceDetail):
    schema = OptionChainSchema
    methods = ["GET"]
    data_layer = {"session": db.session, "model": OptionChain}
