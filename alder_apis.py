import csv
import requests

from marshmallow import Schema, fields

blueprint_app_api = "https://app.giftango.com"
blueprint_api = "https://api.giftango.com"


def common_errors(res):
    if res.status_code == 400:
        return {
            "statusCode": res.status_code,
            "error": "Bad Request",
            "message": res.json(),
        }
    elif res.status_code == 401:
        return {
            "statusCode": res.status_code,
            "error": "Unauthorized access: check your token",
            "message": res.json(),
        }
    elif res.status_code == 403:
        return {
            "statusCode": res.status_code,
            "error": "Forbidden",
            "message": res.json(),
        }
    elif res.status_code == 404:
        return {
            "statusCode": res.status_code,
            "error": "Program not found",
            "message": res.reason,
        }
    elif res.status_code == 405:
        return {
            "statusCode": res.status_code,
            "error": "Method Not Allowed",
            "message": res.json(),
        }
    elif res.status_code == 409:
        return {
            "statusCode": res.status_code,
            "error": "Duplicate entity found",
            "message": res.json(),
        }
    elif res.status_code == 500:
        return {
            "statusCode": res.status_code,
            "error": "Internal Server Error",
            "message": res.json(),
        }
    elif res.status_code == 502:
        return {
            "statusCode": res.status_code,
            "error": "Bad Gateway",
            "message": res.json(),
        }
    elif res.status_code == 503:
        return {
            "statusCode": res.status_code,
            "error": "Service Unavailable",
            "message": res.json(),
        }


def get_access_token(event, context):
    """
    event:{
            grant_type: client_credentials
            client_id: string
            client_secret: string
        }

    """
    url = f"{blueprint_app_api}/auth/token"

    res = requests.post(url=url, data=event)

    if res.status_code == 200:
        # token is successfully received
        return {
            "statusCode": res.status_code,
            "body": res.json(),
        }
    return common_errors(res)


class CatalogueLinkSchema(Schema):
    href = fields.String(required=True, nullable=False)
    rel = fields.String(required=True, nullable=False)


class ProgramSchema(Schema):
    id = fields.Integer(required=True, nullable=False)
    name = fields.String(required=True, nullable=False)
    links = fields.List(fields.Nested(CatalogueLinkSchema))


class CatalogueSchema(Schema):
    id = fields.Integer(required=True, nullable=False)
    programId = fields.Integer(required=True, nullable=False)
    name = fields.String(required=True, nullable=False)
    productName = fields.String(required=True, nullable=False)
    brandName = fields.String(required=True, nullable=False)
    productSku = fields.String(required=True, nullable=False)
    maxAmount = fields.Integer(required=True, nullable=False)
    minAmount = fields.Integer(required=True, nullable=False)
    isDigital = fields.Boolean(required=True, nullable=False)
    description = fields.String(required=True, nullable=False)
    modifiedOn = fields.DateTime(required=True, nullable=False)
    currencyCode = fields.String(required=True, nullable=False)
    productType = fields.String(required=True, nullable=False)
    categories = fields.List(fields.Str())


class ProgramWithProductsSchema(Schema):
    id = fields.Integer(required=True, nullable=False)
    name = fields.String(required=True, nullable=False)
    programId = fields.Integer(required=True, nullable=False)
    products = fields.List(
        fields.Nested(CatalogueSchema(exclude=("id", "programId", "name")))
    )


class ProductSkuAssetSchema(Schema):
    productSku = fields.String(required=True, nullable=False)


class CatalogueAssets(Schema):
    languageCulture = fields.String(required=True, nullable=False)


class ProductsSchema(Schema):
    Sku = fields.String(required=True, nullable=False)
    Value = fields.Integer(required=True, nullable=True)
    Quantity = fields.Integer(required=True, nullable=True)
    EmbossedTextId = fields.Integer()
    Packaging = fields.String()
    MessageText = fields.String()
    MessageRecipientName = fields.String()
    MessageSenderName = fields.String()


class RecipientsSchema(Schema):
    ShippingMethod = fields.String(required=True, nullable=False)
    LanguageCultureCode = fields.String()
    FirstName = fields.String(required=True, nullable=False)
    LastName = fields.String(required=True, nullable=False)
    EmailAddress = fields.String(required=True, nullable=False)
    Address1 = fields.String(required=True, nullable=False)
    Address2 = fields.String(required=False, nullable=True)
    City = fields.String(required=True, nullable=False)
    StateProvinceCode = fields.String(required=True, nullable=False)
    PostalCode = fields.String(required=True, nullable=False)
    CountryCode = fields.String(required=True, nullable=False)
    DeliverEmail = fields.Boolean(required=True, nullable=False)
    Products = fields.List(fields.Nested(ProductsSchema))


class OrderSchema(Schema):
    PurchaseOrderNumber = fields.String(required=True, nullable=False)
    CatalogId = fields.Integer(required=True, nullable=True)
    Metadata = fields.String(required=True, nullable=False)
    CustomerOrderId = fields.String(required=True, nullable=False)

    id = fields.Integer()
    name = fields.String()
    OrderUri = fields.String()
    CreatedOn = fields.DateTime()
    OrderStatus = fields.String()
    Message = fields.String()
    TotalFaceValue = fields.Integer()
    ProgramId = fields.Integer()
    TotalFees = fields.Integer()
    TotalDiscounts = fields.Integer()
    TotalCustomerCost = fields.Integer()
    EmailTheme = fields.String()
    Recipients = fields.List(fields.Nested(RecipientsSchema))


class CardsSchema(Schema):
    CertificateLink = (fields.String(),)
    CardNumber = (fields.Integer(),)
    Pin = (fields.Integer(),)
    BarcodeImageUrl = (fields.String(),)
    ImageUrl = (fields.String(),)
    TermsAndConditions = (fields.String(),)
    MarketingDescription = (fields.String(),)
    InitialBalance = (fields.Float(),)
    CardUri = (fields.String(),)
    Sku = (fields.String(),)
    CreatedOn = (fields.DateTime(),)
    UsageInstructions = (fields.String(),)
    ActivationDate = fields.DateTime()


class FulfillmentSchema(Schema):
    FulfillmentStatus = (fields.String(),)
    OrderUri = (fields.String(),)
    CustomerOrderId = (fields.String(),)
    OrderDate = (fields.DateTime(),)
    Sku = (fields.String(),)
    Value = (fields.Float(),)
    Quantity = fields.Integer()


def retrieve_programs(event, context):
    url = f"{blueprint_app_api}/programs/programs"
    headers = {
        "Authorization": "Bearer dwFWBtfG0jwecoxlU4sRbZwht_IIVbWfwna3qYSVZ5w-1lnW8qqG4DQ_PZWc00YrlRFCR4IVOdOHwsK2fdHT03uM2ATQpB0NWJ9YYn-NdNHLtRkRCYX1BsYnNAApt_bSpf8JPVp2ieyCHdsBmjsSMajdPUiJHQIpTVH6CrNRXiaSkvqisxDN8AwGcO0Hn3vwzWymorhSLIIivGnEagZlpwKOTtNckrUGDhfBGXw0vgJ9SOijYKd2IkubQMB9b_4Zt7fVqxz_yVtSjKwko99qxCHW92-NpSIGmBo3NEatzG-QT8cf0TNGplirr9xw15n2rqBypALOykMhuJmmhVKMbs0AY-IASz6IXkGrzjCRGZaKyKEcs31GWwAXroPUs42UF3voZdsAlPPGFRyaBfjvEK4t6UnNDsjqQ6b7g95jC1JTqA2noV4qqjAxTznFGkxSNnS0VDmGu_AzgiLQYdhT9w629ENXTKzXusYLNcRyyQnOEGExRIAu9f9ZkZ5Uybog"
    }
    res = requests.get(url=url, headers=headers)

    if res.status_code == 200:
        try:
            # validate response
            ProgramSchema(many=True).load(res.json())
            return {
                "statusCode": res.status_code,
                "body": res.json(),
            }
        except Exception as e:
            return {
                "statusCode": res.status_code,
                "body": e,
            }
    return common_errors(res)


def get_programs_catalogue(event, context):
    programId = "6416"
    url = f"{blueprint_app_api}/programs/programs/{programId}/catalogs"
    headers = {
        "Authorization": "Bearer dwFWBtfG0jwecoxlU4sRbZwht_IIVbWfwna3qYSVZ5w-1lnW8qqG4DQ_PZWc00YrlRFCR4IVOdOHwsK2fdHT03uM2ATQpB0NWJ9YYn-NdNHLtRkRCYX1BsYnNAApt_bSpf8JPVp2ieyCHdsBmjsSMajdPUiJHQIpTVH6CrNRXiaSkvqisxDN8AwGcO0Hn3vwzWymorhSLIIivGnEagZlpwKOTtNckrUGDhfBGXw0vgJ9SOijYKd2IkubQMB9b_4Zt7fVqxz_yVtSjKwko99qxCHW92-NpSIGmBo3NEatzG-QT8cf0TNGplirr9xw15n2rqBypALOykMhuJmmhVKMbs0AY-IASz6IXkGrzjCRGZaKyKEcs31GWwAXroPUs42UF3voZdsAlPPGFRyaBfjvEK4t6UnNDsjqQ6b7g95jC1JTqA2noV4qqjAxTznFGkxSNnS0VDmGu_AzgiLQYdhT9w629ENXTKzXusYLNcRyyQnOEGExRIAu9f9ZkZ5Uybog"
    }
    res = requests.get(url=url, headers=headers)

    if res.status_code == 200:
        try:
            # validate response
            ProgramSchema(many=True).load(res.json())
            return {
                "statusCode": res.status_code,
                "body": res.json(),
            }
        except Exception as e:
            return {
                "statusCode": res.status_code,
                "body": e,
            }
    return common_errors(res)


def get_catalogue(event, context):
    programId = 6416
    catalogId = "1"
    url = f"{blueprint_app_api}/programs/programs/{programId}/catalogs/{catalogId}"
    headers = {
        "Authorization": "Bearer dwFWBtfG0jwecoxlU4sRbZwht_IIVbWfwna3qYSVZ5w-1lnW8qqG4DQ_PZWc00YrlRFCR4IVOdOHwsK2fdHT03uM2ATQpB0NWJ9YYn-NdNHLtRkRCYX1BsYnNAApt_bSpf8JPVp2ieyCHdsBmjsSMajdPUiJHQIpTVH6CrNRXiaSkvqisxDN8AwGcO0Hn3vwzWymorhSLIIivGnEagZlpwKOTtNckrUGDhfBGXw0vgJ9SOijYKd2IkubQMB9b_4Zt7fVqxz_yVtSjKwko99qxCHW92-NpSIGmBo3NEatzG-QT8cf0TNGplirr9xw15n2rqBypALOykMhuJmmhVKMbs0AY-IASz6IXkGrzjCRGZaKyKEcs31GWwAXroPUs42UF3voZdsAlPPGFRyaBfjvEK4t6UnNDsjqQ6b7g95jC1JTqA2noV4qqjAxTznFGkxSNnS0VDmGu_AzgiLQYdhT9w629ENXTKzXusYLNcRyyQnOEGExRIAu9f9ZkZ5Uybog"
    }
    res = requests.get(url=url, headers=headers)

    if res.status_code == 200:
        try:
            # validate response
            ProgramWithProductsSchema().load(res.json())
            return {
                "statusCode": res.status_code,
                "body": res.json(),
            }
        except Exception as e:
            return {
                "statusCode": res.status_code,
                "body": e,
            }
    return common_errors(res)


def get_catalogue_assets(event, context):
    programId = 6416
    catalogId = "1"
    url = (
        f"{blueprint_app_api}/programs/programs/{programId}/catalogs/{catalogId}/assets"
    )
    headers = {
        "Authorization": "Bearer dwFWBtfG0jwecoxlU4sRbZwht_IIVbWfwna3qYSVZ5w-1lnW8qqG4DQ_PZWc00YrlRFCR4IVOdOHwsK2fdHT03uM2ATQpB0NWJ9YYn-NdNHLtRkRCYX1BsYnNAApt_bSpf8JPVp2ieyCHdsBmjsSMajdPUiJHQIpTVH6CrNRXiaSkvqisxDN8AwGcO0Hn3vwzWymorhSLIIivGnEagZlpwKOTtNckrUGDhfBGXw0vgJ9SOijYKd2IkubQMB9b_4Zt7fVqxz_yVtSjKwko99qxCHW92-NpSIGmBo3NEatzG-QT8cf0TNGplirr9xw15n2rqBypALOykMhuJmmhVKMbs0AY-IASz6IXkGrzjCRGZaKyKEcs31GWwAXroPUs42UF3voZdsAlPPGFRyaBfjvEK4t6UnNDsjqQ6b7g95jC1JTqA2noV4qqjAxTznFGkxSNnS0VDmGu_AzgiLQYdhT9w629ENXTKzXusYLNcRyyQnOEGExRIAu9f9ZkZ5Uybog"
    }
    res = requests.get(url=url, headers=headers)

    if res.status_code == 200:
        # TODO create schema
        return {
            "statusCode": res.status_code,
            "body": res.json(),
        }
    return common_errors(res)


def submit_immediate_order(event, context):
    """
    expected payload
            {
          "OrderUri": "string",
          "CreatedOn": "2019-06-13T13:50:31Z",
          "OrderStatus": "Pending",
          "Message": "string",
          "PurchaseOrderNumber": "string",
          "ProgramId": 0,
          "CatalogId": 0,
          "Metadata": "string",
          "TotalFaceValue": 0,
          "TotalFees": 0,
          "TotalDiscounts": 0,
          "TotalCustomerCost": 0,
          "CustomerOrderId": "string",
          "EmailTheme": "string",
          "Recipients": [
            {
              "ShippingMethod": "string",
              "LanguageCultureCode": "string",
              "FirstName": "string",
              "LastName": "string",
              "EmailAddress": "string",
              "Address1": "string",
              "Address2": "string",
              "City": "string",
              "StateProvinceCode": "string",
              "PostalCode": "string",
              "CountryCode": "string",
              "DeliverEmail": True,
              "Products": [
                {
                  "Sku": "string",
                  "Value": 0,
                  "Quantity": 0,
                  "EmbossedTextId": 0,
                  "Packaging": "string",
                  "MessageText": "string",
                  "MessageRecipientName": "string",
                  "MessageSenderName": "string"
                }
              ]
            }
          ]
        }
    """

    event = {
        "PurchaseOrderNumber": "test007",
        "CatalogId": 1,
        "Metadata": "test",
        "CustomerOrderId": "testing0042",
        "Recipients": [
            {
                "ShippingMethod": "Email",
                "FirstName": "string f",
                "LastName": "string l",
                "EmailAddress": "phani@storecashapp.com",
                "Address1": "34500 Fremont Blvd",
                "Address2": "string",
                "City": "Fremont",
                "StateProvinceCode": "CA",
                "PostalCode": "94538",
                "CountryCode": "US",
                "DeliverEmail": True,
                "Products": [{"Sku": "VUSA-D-V-00", "Value": 5, "Quantity": 1}],
            }
        ],
    }
    try:
        # validate post payload
        OrderSchema().load(event)
    except Exception as e:
        return e

    headers = {
        "Authorization": "Bearer E5arEe4alFO7SsRmZcdR4t1g4e9DdTvP6gS-BiIYyqmZzTL6N-Ddy_Jfv3MfhKCOUcq8ie3oUIESGoGAKnQrhH-Zlcf_MBUMsWsnE_tdtI5-5PEOfThgsoB0Okvv88tzaIvZln-2akKfS4SlUH58MNguNHgVostPs9Xe_f6He_yBq93db4Ny9ABVwbyipJQLR0rDd2lc4q6UjUF0iQhyOedidqEPbwvvCVEcoyuhmvppzV9egXX1ZDM6n7IjmeiaoBhd16wxhRkZ491rQxAgNLDdcOpX15MWC4MVTrW_AbOTMDeKV-am1k1R-b4nrLJAgclONoRJvxyKA0mN8bHgk3yVT1CSEclCQ1MYTJrut4W78wVTqwwwSWBbZTy-xgZAYELGIW7Y9TP_uMOJjLtAZ4AeY5i8NAO1P3fkdr-LtGWzM1mmG7gazwoYIaL57qwGOa9As8iKNTe2Q2JBkyC1HYtSvbCW5Um1KcRb5TU43mBTzTALY5a02e7oomxGgogv",
        "ProgramId": "6416",
    }

    url = f"{blueprint_api}/Orders/Immediate"
    res = requests.post(url=url, headers=headers, json=event)
    if res.status_code == 201:
        try:
            # validate response
            OrderSchema().load(res.json())
            return {
                "statusCode": res.status_code,
                "body": res.json(),
            }
        except Exception as e:
            return {
                "statusCode": res.status_code,
                "body": e,
            }
    return common_errors(res)


def get_order(event, context):
    orderUri = "orderUri"
    orderUri = "08M-LW-1Z7"

    url = f"{blueprint_api}/Orders/{orderUri}"
    headers = {
        "Authorization": "Bearer dwFWBtfG0jwecoxlU4sRbZwht_IIVbWfwna3qYSVZ5w-1lnW8qqG4DQ_PZWc00YrlRFCR4IVOdOHwsK2fdHT03uM2ATQpB0NWJ9YYn-NdNHLtRkRCYX1BsYnNAApt_bSpf8JPVp2ieyCHdsBmjsSMajdPUiJHQIpTVH6CrNRXiaSkvqisxDN8AwGcO0Hn3vwzWymorhSLIIivGnEagZlpwKOTtNckrUGDhfBGXw0vgJ9SOijYKd2IkubQMB9b_4Zt7fVqxz_yVtSjKwko99qxCHW92-NpSIGmBo3NEatzG-QT8cf0TNGplirr9xw15n2rqBypALOykMhuJmmhVKMbs0AY-IASz6IXkGrzjCRGZaKyKEcs31GWwAXroPUs42UF3voZdsAlPPGFRyaBfjvEK4t6UnNDsjqQ6b7g95jC1JTqA2noV4qqjAxTznFGkxSNnS0VDmGu_AzgiLQYdhT9w629ENXTKzXusYLNcRyyQnOEGExRIAu9f9ZkZ5Uybog"
    }
    res = requests.get(url=url, headers=headers)

    if res.status_code == 200:
        try:
            # validate response
            OrderSchema().load(res.json())
            return {
                "statusCode": res.status_code,
                "body": res.json(),
            }
        except Exception as e:
            return {
                "statusCode": res.status_code,
                "body": e,
            }
    return common_errors(res)


def get_cards_from_order(event, context):
    orderUri = "orderUri"
    orderUri = "P8D-KK-SHT"

    url = f"{blueprint_api}/Orders/{orderUri}/cards"
    headers = {
        "Authorization": "Bearer dwFWBtfG0jwecoxlU4sRbZwht_IIVbWfwna3qYSVZ5w-1lnW8qqG4DQ_PZWc00YrlRFCR4IVOdOHwsK2fdHT03uM2ATQpB0NWJ9YYn-NdNHLtRkRCYX1BsYnNAApt_bSpf8JPVp2ieyCHdsBmjsSMajdPUiJHQIpTVH6CrNRXiaSkvqisxDN8AwGcO0Hn3vwzWymorhSLIIivGnEagZlpwKOTtNckrUGDhfBGXw0vgJ9SOijYKd2IkubQMB9b_4Zt7fVqxz_yVtSjKwko99qxCHW92-NpSIGmBo3NEatzG-QT8cf0TNGplirr9xw15n2rqBypALOykMhuJmmhVKMbs0AY-IASz6IXkGrzjCRGZaKyKEcs31GWwAXroPUs42UF3voZdsAlPPGFRyaBfjvEK4t6UnNDsjqQ6b7g95jC1JTqA2noV4qqjAxTznFGkxSNnS0VDmGu_AzgiLQYdhT9w629ENXTKzXusYLNcRyyQnOEGExRIAu9f9ZkZ5Uybog"
    }
    res = requests.get(url=url, headers=headers)

    if res.status_code == 200:
        try:
            # validate response
            CardsSchema().load(res.json())
            return {
                "statusCode": res.status_code,
                "body": res.json(),
            }
        except Exception as e:
            return {
                "statusCode": res.status_code,
                "body": e,
            }
    return common_errors(res)


def get_fullfillment(event, context):
    url = f"{blueprint_api}/Fulfillment"
    headers = {
        "Authorization": "Bearer dwFWBtfG0jwecoxlU4sRbZwht_IIVbWfwna3qYSVZ5w-1lnW8qqG4DQ_PZWc00YrlRFCR4IVOdOHwsK2fdHT03uM2ATQpB0NWJ9YYn-NdNHLtRkRCYX1BsYnNAApt_bSpf8JPVp2ieyCHdsBmjsSMajdPUiJHQIpTVH6CrNRXiaSkvqisxDN8AwGcO0Hn3vwzWymorhSLIIivGnEagZlpwKOTtNckrUGDhfBGXw0vgJ9SOijYKd2IkubQMB9b_4Zt7fVqxz_yVtSjKwko99qxCHW92-NpSIGmBo3NEatzG-QT8cf0TNGplirr9xw15n2rqBypALOykMhuJmmhVKMbs0AY-IASz6IXkGrzjCRGZaKyKEcs31GWwAXroPUs42UF3voZdsAlPPGFRyaBfjvEK4t6UnNDsjqQ6b7g95jC1JTqA2noV4qqjAxTznFGkxSNnS0VDmGu_AzgiLQYdhT9w629ENXTKzXusYLNcRyyQnOEGExRIAu9f9ZkZ5Uybog",
        "ProgramId": "6416",
    }
    res = requests.get(url=url, headers=headers)

    if res.status_code == 200:
        try:
            # validate response
            FulfillmentSchema(many=True).load(res.json())
            return {
                "statusCode": res.status_code,
                "body": res.json(),
            }
        except Exception as e:
            return {
                "statusCode": res.status_code,
                "body": e,
            }
    return common_errors(res)


# def get_card(event, context):
#     cardUri = "4519684766407396"
#     url = f"{blueprint_api}/Cards/{cardUri}"
#     headers = {
#         "Authorization": "Bearer rNVajBf_4UAwF0eOxECdZED1jcgkkzKSSxKDmGOT90QQQEVq5lDOR4jTtjPKEUPfs2kEhtBMA4MbW7E77JPk4VQ5N7wQXxoaxGnBGlJY0x5zzqQhCXjldfvK_c-NJhc6bMKbRIWAFUAOHAIKQ6ZzMw9OQMQxEb0k92F3cWXKKA3HbeQHjSQFTGsHSOyGx18-OUGYOqTEWn3Tnn_1H9V3AZZn9cXzT3MN0qMKMzpkm5pTCWIEdvXnGRulciEuYRjCPpKELTuvTKxqb2fP9_T5344HDmnkatQ-ddcH_lQeer2uBZpRDxYWsKtTipmfvQcod_mrmvx6Lu2PeeJhDYiLI_IxBJl6mXGVdIMdNH0DDNIHfhm2A4Rc2-tJxstQGFgUpPbdZjzTskAyoRzM0ChtwrtC6k7DIYIKwcmS3mB5a7e4xjmA-RFMwiH5-PVucI9XZuFgF05ZwCiuCVjhqxX7TrB0KOUgHeVoeDINmRmVhBQTdsLOSiNRtLCjgzbi_Lr2"
#     }
#     res = requests.get(url=url, headers=headers)
#
#     if res.status_code == 200:
#         # validate response
#         # TODO, dont know cards schema
#         return {
#             "statusCode": res.status_code,
#             "body": res.json(),
#         }
#     return common_errors(res)


def generate_csv():
    """
    get all catalogues
    loop through them
    fetch one asset
    and generate csv for all the fields containing on the left side of the document:
        "https://docs.google.com/spreadsheets/d/10SXqUXZj9OCz0Ebzz26lfRiSCfzDashBlt25F9cYk3c/edit#gid=0"
    """
    programId = 6416
    catalogId = "1"
    url = "https://app.giftango.com/programs/programs/6416/catalogs/1"
    headers = {
        "Authorization": "Bearer qyJV8frxn_G3E4C17quuPyNQQppT6FSbqskosexVjfXQuYaJEFXmGgAcMVQVj1-WiwRxQe181XgMnaHFZzsLfYoZW0VuQlVY_xCLCEJUkql9NsM-sBMPzIwACN_Veck6EodIC4QC1VZidE8z3nIqdhTUCCfy5MGI08oH9BwqlDW0d0dbwL1FkzLvewKHB-RZgL4BN3NYP409A_SM9PIypqi0SrXcxBU-5IQsLe8hk-NneVGyYGrE7t-QFR80ciLxfNnJZs4TugZLapTCsyiT_kT5WfRNJj_yhjYlOY_FSaE498YVyicxvPJo1PBLRpEuaeEXrtqAcJDqOxLIuqtpLlZpFjCI5ZmNiJfloKJxaK9451Mrh2gFXWKxZ_6J4hlu3ljkJks7JMDIpsmITs0WKnWbrppWqbiUAze_CbsfD3h3Q3Jt8Pz-UVBFlUWGH75J1xViH3INic-6YfCm7SwE6ia5Z51jaV6Lxco9BRkJiFJZGsnz8yLgnVyVjO8HixQn"
    }
    catalog_res = requests.get(url=url, headers=headers)

    catalog_asset_desc_res = requests.get(
        url="https://app.giftango.com/programs/programs/6416/catalogs/1/assets",
        headers=headers,
    )

    if catalog_res.status_code == 200 and catalog_asset_desc_res.status_code == 200:
        # validate response
        # CatalogueSchema().load(catalog_res.json())

        product_sku_details_dict = {}
        csv_row = []
        for product in catalog_res.json()["products"]:
            product_sku_details_dict.update(
                {
                    product["productSku"]: {
                        "sku": product["productSku"],
                        "name": product["productName"],
                        "max": product["maxAmount"],
                        "min": product["minAmount"],
                        "updated_at": product["modifiedOn"],
                        "currency": product["currencyCode"],
                        "subcategory": product["categories"],
                        "category": "INCOMM",
                    }
                }
            )

        db_field_mappings = {
            "legaldisclaimer": "legald",
            "redemptioninstructions": "inst",
            "termsconditions": "terms",
            "marketingdescription": "description",
            "cardimage": "image",
        }

        asset_desc_dict = {}
        for productSku_assets in catalog_asset_desc_res.json()[0]["products"]:
            db_field_mappings_found = {
                "legaldisclaimer": False,
                "redemptioninstructions": False,
                "termsconditions": False,
                "marketingdescription": False,
                "cardimage": False,
            }
            for asset in productSku_assets["assets"]:
                if asset["type"] in db_field_mappings:
                    asset_desc_dict.update(
                        {db_field_mappings[asset["type"]]: asset.get("text", "")}
                    )
                    db_field_mappings_found[asset["type"]] = True
            for mapping, found in db_field_mappings_found.items():
                if not found:
                    asset_desc_dict.update({db_field_mappings[mapping]: ""})

            csv_row.append(
                {
                    **product_sku_details_dict[productSku_assets["productSku"]],
                    **asset_desc_dict,
                }
            )
            asset_desc_dict = {}

        with open("mappings.csv", "w", encoding="utf8", newline="") as output_file:
            fc = csv.DictWriter(
                output_file,
                fieldnames=csv_row[0].keys(),
            )
            fc.writeheader()
            fc.writerows(csv_row)


print(generate_csv())

import csv

import requests
from marshmallow import Schema, fields


class CatalogueSchema(Schema):
    id = fields.Integer(required=True, nullable=False)
    programId = fields.Integer(required=True, nullable=False)
    name = fields.String(required=True, nullable=False)
    productName = fields.String(required=True, nullable=False)
    brandName = fields.String(required=True, nullable=False)
    productSku = fields.String(required=True, nullable=False)
    maxAmount = fields.Integer(required=True, nullable=False)
    minAmount = fields.Integer(required=True, nullable=False)
    isDigital = fields.Boolean(required=True, nullable=False)
    description = fields.String(required=True, nullable=False)
    modifiedOn = fields.DateTime(required=True, nullable=False)
    currencyCode = fields.String(required=True, nullable=False)
    productType = fields.String(required=True, nullable=False)
    categories = fields.List(fields.Str())


def lambda_handler(event, context):
    """
    get all catalogues
    loop through them
    fetch one asset
    and generate csv for all the fields containing on the left side of the document:
        "https://docs.google.com/spreadsheets/d/10SXqUXZj9OCz0Ebzz26lfRiSCfzDashBlt25F9cYk3c/edit#gid=0"
    """
    programId = 6416
    catalogId = "1"
    url = "https://app.giftango.com/programs/programs/6416/catalogs/1"
    headers = {
        "Authorization": "Bearer b5VPeYwnCOi8jWn79mCUImmNFWmSbhZk_MFVpGyf66DM96QEezIZZAZfc4FIYbNw_s-LEa8g7HRb_PtdaDv251Jvd7I0u9IOmZ8VAra6iixTlOEw1KYgQ8WqLkIPB0eoknDE3ktEmkRthOmdxJ5mMkZPTj3W6y1kPHjY7Xxt-9J_4cxWeJRh_7evo7O3hTNv9KTiiWKCkEIqn0uN_sEsxMuPV5xsGfCfUINDUdzV4bxJ_PkiRZp3708qEZydoa40SfUKtryBz5lBD_4aeOfe06M4ldV2uGJj7nYK-E-v5WHqKE2WcsxyCBt0dzLgRlfN7QeGC5-PH2QLPMw3Lekelj172zhVVMio7sw-NYHc2Bt6cWeDuj5NUZ6RtJyXbS4xGB2h7I8svVbD4YjleQmGHY1iFEVk8l4N4GMz12cYPl-G282J1M5h6CGczCxCfFgA64VR_Ck5oKaDzK3JH6mohsfKoipNXmt2uMR0P6xhmRLKjPlQtwcIYLTj4seWZJ54"
    }
    catalog_res = requests.get(url=url, headers=headers)

    catalog_asset_desc_res = requests.get(
        url="https://app.giftango.com/programs/programs/6416/catalogs/1/assets",
        headers=headers,
    )

    if catalog_res.status_code == 200 and catalog_asset_desc_res.status_code == 200:
        # validate response
        # CatalogueSchema().load(catalog_res.json())

        product_sku_details_dict = {}
        csv_row = []
        for product in catalog_res.json()["products"]:
            product_sku_details_dict.update(
                {
                    product["productSku"]: {
                        "sku": product["productSku"],
                        "name": product["productName"],
                        "max": product["maxAmount"],
                        "min": product["minAmount"],
                        "updated_at": product["modifiedOn"],
                        "currency": product["currencyCode"],
                        "subcategory": product["categories"],
                        "category": "INCOMM",
                    }
                }
            )

        db_field_mappings = {
            "legaldisclaimer": "legald",
            "redemptioninstructions": "inst",
            "termsconditions": "terms",
            "marketingdescription": "description",
            "cardimage": "image",
        }

        asset_desc_dict = {}
        for productSku_assets in catalog_asset_desc_res.json()[0]["products"]:
            db_field_mappings_found = {
                "legaldisclaimer": False,
                "redemptioninstructions": False,
                "termsconditions": False,
                "marketingdescription": False,
                "cardimage": False,
            }
            for asset in productSku_assets["assets"]:
                if asset["type"] in db_field_mappings:
                    asset_desc_dict.update(
                        {db_field_mappings[asset["type"]]: asset.get("text", "")}
                    )
                    db_field_mappings_found[asset["type"]] = True
            for mapping, found in db_field_mappings_found.items():
                if not found:
                    asset_desc_dict.update({db_field_mappings[mapping]: ""})

            csv_row.append(
                {
                    **product_sku_details_dict[productSku_assets["productSku"]],
                    **asset_desc_dict,
                }
            )
            asset_desc_dict = {}

        with open("/tmp/mappings.csv", "w", encoding="utf8", newline="") as output_file:
            fc = csv.DictWriter(
                output_file,
                fieldnames=csv_row[0].keys(),
            )
            fc.writeheader()
            fc.writerows(csv_row)
        with open("/tmp/mappings.csv") as file:
            content = file.readlines()
        return content
