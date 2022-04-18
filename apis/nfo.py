# Create resource managers

import logging

from flask import request, url_for
from flask_rest_jsonapi import ResourceDetail, ResourceList
from flask_rest_jsonapi.decorators import check_method_requirements
from flask_rest_jsonapi.exceptions import ObjectNotFound
from flask_rest_jsonapi.pagination import add_pagination_links
from flask_rest_jsonapi.querystring import QueryStringManager as QSManager
from flask_rest_jsonapi.schema import compute_schema
from marshmallow import ValidationError
from marshmallow_jsonapi.exceptions import IncorrectTypeError
from sqlalchemy.orm.exc import NoResultFound

from apis.utils import buy_or_sell_future, buy_or_sell_option
from extensions import db
from models.nfo import NFO
from schema.nfo import NFOSchema

log = logging.getLogger(__file__)


class NFODetail(ResourceDetail):
    # ignore below code its dummy
    # def before_get_object(self, view_kwargs):
    #     if view_kwargs.get("computer_id") is not None:
    #         try:
    #             computer = (
    #                 self.session.query(NFO)
    #                 .filter_by(id=view_kwargs["computer_id"])
    #                 .one()
    #             )
    #         except NoResultFound:
    #             raise ObjectNotFound(
    #                 {"parameter": "computer_id"},
    #                 "Computer: {} not found".format(view_kwargs["computer_id"]),
    #             )
    #         else:
    #             if computer.person is not None:
    #                 view_kwargs["id"] = computer.person.id
    #             else:
    #                 view_kwargs["id"] = None

    schema = NFOSchema
    data_layer = {
        "session": db.session,
        "model": NFO,
    }


class NFOList(ResourceList):
    @check_method_requirements
    def get(self, *args, **kwargs):
        """Retrieve a collection of objects"""
        self.before_get(args, kwargs)

        qs = QSManager(request.args, self.schema)

        objects_count, objects = self.get_collection(qs, kwargs)

        schema_kwargs = getattr(self, "get_schema_kwargs", dict())
        schema_kwargs.update({"many": True})

        self.before_marshmallow(args, kwargs)

        schema = compute_schema(self.schema, schema_kwargs, qs, qs.include)

        result = schema.dump(objects).data

        view_kwargs = (
            request.view_args if getattr(self, "view_kwargs", None) is True else dict()
        )
        add_pagination_links(
            result, objects_count, qs, url_for(self.view, _external=True, **view_kwargs)
        )

        return self.after_get(result)

    @check_method_requirements
    def post(self, *args, **kwargs):
        """Create an object"""
        json_data = request.get_json() or {}

        qs = QSManager(request.args, self.schema)

        schema_kwargs = getattr(self, "post_schema_kwargs", dict())
        schema = compute_schema(self.schema, schema_kwargs, qs, qs.include)

        try:
            data, errors = schema.load(json_data)
        except IncorrectTypeError as e:
            errors = e.messages
            for error in errors["errors"]:
                error["status"] = "409"
                error["title"] = "Incorrect type"
            return errors, 409
        except ValidationError as e:
            errors = e.messages
            for message in errors["errors"]:
                message["status"] = "422"
                message["title"] = "Validation error"
            return errors, 422

        if errors:
            for error in errors["errors"]:
                error["status"] = "422"
                error["title"] = "Validation error"
            return errors, 422

        if data.get("nfo_type").lower() == "future":
            objects = buy_or_sell_future(self, data)
        elif data.get("nfo_type").lower() == "option":
            objects = buy_or_sell_option(self, data)
        else:
            object_1 = buy_or_sell_future(self, data)
            if object_2 := buy_or_sell_option(self, data):
                objects = [*object_1, *object_2]
            else:
                objects = object_1

        schema_kwargs.update({"many": True})
        schema = compute_schema(self.schema, schema_kwargs, qs, qs.include)
        return schema.dump(objects).data if objects else "No Trade"

    schema = NFOSchema
    data_layer = {
        "session": db.session,
        "model": NFO,
    }
