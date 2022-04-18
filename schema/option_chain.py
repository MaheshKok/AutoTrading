# Create logical data abstraction (same as data storage for this first example)
from marshmallow_jsonapi import fields
from marshmallow_jsonapi.flask import Schema


class OptionChainSchema(Schema):
    class Meta:
        type_ = "option_chain"
        self_view = "option_chain_detail"
        self_view_kwargs = {"id": "<id>"}
        self_view_many = "option_chain_list"

    id = fields.Integer(as_string=True, dump_only=True)

    strike = fields.Integer()

    celtp = fields.Float()
    celtt = fields.Time()

    peltp = fields.Float()
    peltt = fields.Time()

    date = fields.Date()
