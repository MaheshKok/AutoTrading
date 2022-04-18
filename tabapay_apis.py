import json
import random

import requests

from marshmallow import Schema, fields
from marshmallow import validate


class nameSchema(Schema):
    company = fields.Str()
    first = fields.Str()
    middle = fields.Str()
    last = fields.Str()
    suffix = fields.Str()


class addressSchema(Schema):
    line1 = fields.Str()
    line2 = fields.Str()
    city = fields.Str()
    state = fields.Str()
    zipcode = fields.Str()
    country = fields.Str()


class phoneSchema(Schema):
    countryCode = fields.Integer()
    number = fields.Integer(
        required=True, validate=validate.Range(min=9999, max=9999999999)
    )


class ownerSchema(Schema):
    name = fields.Nested(nameSchema, required=True)
    address = fields.Nested(addressSchema)
    phone = fields.Nested(phoneSchema)


class cardSchema(Schema):
    accountNumber = fields.Integer(required=True)
    expirationDate = fields.Integer(required=True)
    securityCode = fields.Integer(required=True)


class bankSchema(Schema):
    routingNumber = fields.Integer(validate=validate.Length(9))
    accountNumber = fields.Integer(validate=validate.Range(4, 17))
    accountType = fields.Str(validate=validate.Length(1))


class accountSchema(Schema):
    referenceID = fields.Str(required=True)
    bank = fields.Nested(bankSchema)
    owner = fields.Nested(ownerSchema)
    card = fields.Nested(cardSchema)


FQDN = "https://api.sandbox.tabapay.net:10443"
ClientID = "DpwE2JYRCEdlLjB3T8Yl9A"
AccountID = "iq0F3ffUQW4uG_SHnIKTeQ"


def createKey(event, context):
    headers = {
        "Authorization": "Bearer OZslCbLP8kmKvspMshtxNQugYPxgdiZywohPVJpOzvmhm6RenYYo8igdrPzoekCGofggKfrwTXld",
        "Content-type": "application/json",
    }
    uri = f"{FQDN}/v1/clients/{ClientID}/keys"

    # Assuming we have FQDN, ClientIDISO, and ClientIDISO
    res = requests.post(url=uri, json=event, headers=headers)
    json_res = res.json()

    if json_res["SC"] == 200 and json_res["EC"] == 00:
        # client is successfully retrieved","
        return {
            "statusCode": res.status_code,
            "account": json_res,
            "message": "client is successfully retrieved",
        }

    return {
        "statusCode": res.status_code,
        "message": json_res,
    }


event = {"format": "Raw", "expiration": 365}

# print(createKey(event, ""))

# response
create_key_response = {
    "statusCode": 200,
    "message": {
        "SC": 200,
        "EC": "0",
        "keyID": "A6YlFTQSmALColPNvjbk-g",
        "keyModulus": "AKqMDO26Eeb_ajZ91v2nEAjdYW1pE8XkTUAqv14hI50miZVQcouLZ8EX6uNZsSdAG1w34Q1_iP0dyO7nMMRy-xErsNJUl_SkOD-ZUDqrDURIONcdV9MpbYMRzpaAp-XbUW_xM0nnSUUlS0MLSBIYBgN8T_4FdcY3EBSUgVvkgGXYProZhP4qI8YXuXd9LIqEpUlRSaKojRzXa7--gc9nL3U5AXw0wK6PSG_EC8iRupt6eEbQmIoHR73CT3ajy8qTWEsBUho4AoRD7ewijqAKzSLNLieds-pEUErglCmcjkI5aKYPD6A34CcLBaAZVjrLCCuHUOiC63hunLuHS1pUif0",
        "keyExponent": "AQAB",
        "expiration": "2023-01-04T04:28:56Z",
        "notices": "**WARNING** Creating too many keys will cause you to be unable to create any keys!",
    },
}

another_create_key_response = {'statusCode': 200, 'message': {'SC': 200, 'EC': '0', 'keyID': 'wywHCr1CkEKjuMK2BsjI7g', 'keyModulus': 'AJqzMKyewfuaE5Agki6TRGTTfxoGb8B6VWIN-Byg7GVJCF8-K2t3Mze9om4Dj-Of6dowTaDlAM9cO-jjj3Hth96OKC220l3zrXqc4cCYfWvBxGmVgXpZvgYvweZgiVl9dFMKrbBNsHG9quD8OFQb6puWb9GFc83dFB6gZH7B0w1Qd0-1JzcmR_8MMO4gXBmfC9aE1fe08OCF5cL5p4cdqHzlygSIo2ve3n2uy4LWmR8AWFHshm3c2Cm179KtOYAaZyP2DIefEfuPmZwGulZWjeJ2ddD3r0BkVexCxNWs3u6pyJ1K9mMENmYCSqmz87YLJHn85522zUMIoauK00jBhBc', 'keyExponent': 'AQAB', 'expiration': '2023-01-06T05:56:10Z', 'notices': '**WARNING** Creating too many keys will cause you to be unable to create any keys!'}}

new_response = {'SC': 200,
 'EC': '0',
 'keyID': 'D-EVKrYTyAQhfUGzrb9W_g',
 'keyModulus': 'ALPjzrMD_nFB2tklZsInWecTd9uh_p67MXAqoErgzBuZYsxVEhDCiya0Gp310O7B2rE-PESvXvdAi409xngx3h7Bao1n6-Qy8LFRTmfBXTmZnGILxNxEKHf7uRDoRX5wsgkGuf9D9oEdeOGhoQ0z1ZgNBvnu1YDY97Dklzmz3IzanUeW3HRZjlCTF-68ges5O6DLvoRJ1sHZz3b61C763m0wonwwyvSWUFYi9n8xE-78n_Y8df6RcEZKQHfXVQs31koSql0lqkcTrq-lzddA4xnaHrq-w4u5OwsYM5VSCuedqkKtqhJPBFl_Tgq4LcWhOckuOVmAznRpj8eFZPyl_WM',
 'keyExponent': 'AQAB',
 'expiration': '2023-01-07T16:42:26Z',
 'notices': '**WARNING** Creating too many keys will cause you to be unable to create any keys!'}

{'SC': 200,
 'EC': '0',
 'keyID': 'T-0mCbdSSAZ-V_vstjOTCA',
 'keyModulus': 'AIT3sU0lFu9i_eAyuxb0iB7NwdGZR4fFirKSwLRNiHGCLjEwmJBvupAgWHTt8ebW23XZnNHBpCLxjOptE4AMFLx-Fpk47mOdSorcxQHBN6wyqI-hIGEM_HggOtdTKhSWqeDzoagaBLuZVWq3UJBtJzIySfv-yXqjH4zKO60kzfTsseFRQQkDn59qVQ9QgWGvSbj0B_ccIUqHVd1ATIYNJ79VAT9phyVMZIG9yt3xLeyK0GjTDlUafuM7hmPrIYawNyIACuMjuCT-NaxTIuLA-6Mk0c-O-nE5VikP0cAp7lN7o9v7G8z9s5WCxKHOQV_4T1ARwuCuN6puWH1uHy9oZ3c',
 'keyExponent': 'AQAB',
 'expiration': '2023-01-07T17:40:16Z',
 'notices': '**WARNING** Creating too many keys will cause you to be unable to create any keys!'}


def retrieveClient(event, context):
    headers = {
        "Authorization": "Bearer OZslCbLP8kmKvspMshtxNQugYPxgdiZywohPVJpOzvmhm6RenYYo8igdrPzoekCGofggKfrwTXld",
        "Content-type": "application/json",
    }
    uri = f"{FQDN}/v1/clients/{ClientID}"

    # Assuming we have FQDN, ClientIDISO, and ClientIDISO
    res = requests.get(url=uri, headers=headers)
    json_res = res.json()

    if json_res["SC"] == 200 and json_res["EC"] == 00:
        # client is successfully retrieved","
        return {
            "statusCode": res.status_code,
            "account": json_res,
            "message": "client is successfully retrieved",
        }

    return {
        "statusCode": res.status_code,
        "message": json_res,
    }


def validate_card(event, context):
    """
    Query Card information

    # case that has been handled is when we receive account details
    event = {
        "card": {
            "accountNumber": 4217651111111119,
            "expirationDate": 203001,
            "securityCode": 333,
        }
    }

    """
    headers = {
        "Authorization": "Bearer OZslCbLP8kmKvspMshtxNQugYPxgdiZywohPVJpOzvmhm6RenYYo8igdrPzoekCGofggKfrwTXld",
        "Content-type": "application/json",
    }
    uri = f"{FQDN}/v1/clients/{ClientID}/cards"

    try:
        cardSchema().load(event["card"])
    except Exception as e:
        return {
            "error": "validation error",
            "message": e,
        }

    res = requests.post(url=uri, json=event, headers=headers)

    if res.status_code == 200:
        json_res = json.loads(res.text)
        if json_res["SC"] == 200 and json_res["EC"] == "0":
            # card is successfully verified
            # check if card is available for pull and push
            if (
                json_res["card"]["pull"]["enabled"]
                and json_res["card"]["push"]["enabled"]
            ):
                return {
                    "statusCode": res.status_code,
                    "body": json_res["card"]["pull"],
                }
            elif not json_res["card"]["pull"]["enabled"]:
                return {"statuscode": 403, "body": "Pull is disabled for the Card"}

            elif not json_res["card"]["push"]["enabled"]:
                return {"statuscode": 403, "body": "Push is disabled for the Card"}
    return {
        "statuscode": res.status_code,
        "body": res.text,
    }


def retrieveAccount(event, context):
    headers = {
        "Authorization": "Bearer OZslCbLP8kmKvspMshtxNQugYPxgdiZywohPVJpOzvmhm6RenYYo8igdrPzoekCGofggKfrwTXld"
    }
    uri = f"{FQDN}/v1/clients/{ClientID}/accounts/{AccountID}"

    # Assuming we have FQDN, ClientIDISO, and ClientIDISO
    res = requests.get(url=uri, headers=headers)

    if res.status_code == 200:
        json_res = json.loads(res.text)

        # Validate Json Response
        try:
            data = accountSchema().load(json_res)
        except Exception as e:
            return {
                "error": "validation error",
                "statusCode": res.status_code,
                "message": e,
            }

        if json_res["SC"] == 200 and json_res["EC"] == "0":
            # account is successfully created
            return {
                "statusCode": res.status_code,
                "account": data,
                "message": "account is successfully retrieved",
            }

    return {"statusCode": res.status_code, "message": res.text}


def create_account(event, context):
    """
    create Account

    event = {
        "referenceID": "1",
        "card": {"accountNumber": 4005519200000004},
        "owner": {
            "name": {"first": "John", "last": "Customer"},
            "address": {
                "line1": "465 Fairchild Drive",
                "line2": "Suite #222",
                "city": "Mountain View",
                "state": "CA",
                "zipcode": "94043",
            },
            "phone": {"number": 4159808222},
        },
    }
    """
    # Assuming i have FQDN and ClientIDISO
    uri = f"{FQDN}/v1/clients/{ClientID}/accounts"

    headers = {
        "Authorization": "Bearer OZslCbLP8kmKvspMshtxNQugYPxgdiZywohPVJpOzvmhm6RenYYo8igdrPzoekCGofggKfrwTXld",
        "Content-type": "application/json",
    }
    # validate event
    try:
        accountSchema().load(event)
    except Exception as e:
        return {
            "error": "validation error",
            "message": e,
        }

    res = requests.post(url=uri, json=event, headers=headers)
    if res.status_code == 200:
        json_res = json.loads(res.text)
        if json_res["SC"] == 200 and json_res["EC"] == "0":
            # account is successfully created
            return {
                "statusCode": res.status_code,
                "data": json_res,
                "message": "account is successfully created",
            }
    elif res.status_code == 207:
        json_res = json.loads(res.text)
        return {
            "statusCode": res.status_code,
            "message": "Account created, but Duplicate Card Check Failed",
            "accountId": json_res["accountID"],
        }
    elif res.status_code == 409:
        json_res = json.loads(res.text)
        return {
            "statusCode": res.status_code,
            "message": "Duplicate Card Check",
            "accountId": json_res["accountID"],
        }
    return {
        "statuscode": res.status_code,
        "body": res.text,
    }


def updateAccount(event, context):
    """
    event = {
           "referenceID": "1",
           "card": {"accountNumber": 4005519200000004},
           "owner": {
               "name": {"first": "John", "last": "Customer"},
               "address": {
                   "line1": "465 Fairchild Drive",
                   "line2": "Suite #222",
                   "city": "Mountain View",
                   "state": "CA",
                   "zipcode": "94043",
               },
               "phone": {"number": 1159808222},
           },
       }
    """
    headers = {
        "Authorization": "Bearer OZslCbLP8kmKvspMshtxNQugYPxgdiZywohPVJpOzvmhm6RenYYo8igdrPzoekCGofggKfrwTXld",
        "Content-type": "application/json",
    }

    uri = f"{FQDN}/v1/clients/{ClientID}/accounts/{AccountID}"

    # validate event
    try:
        accountSchema().load(event)
    except Exception as e:
        return {
            "error": "validation error",
            "body": e,
        }

    res = requests.put(url=uri, json=event, headers=headers)
    if res.status_code == 200:
        json_res = json.loads(res.text)
        if json_res["SC"] == 200 and json_res["EC"] == "0":
            # account is successfully updated
            return {
                "statusCode": res.status_code,
                "message": "account is successfully updated",
            }
    elif res.status_code == 207:
        return {
            "statusCode": res.status_code,
            "message": "Account updated, but Duplicate Card Check Failed",
        }
    elif res.status_code == 409:
        return {
            "statusCode": res.status_code,
            "message": "Duplicate Card Check",
        }
    return {
        "statuscode": res.status_code,
        "body": res.text,
    }


def deleteAccount(event, context):
    uri = f"{FQDN}/v1/clients/{ClientID}/accounts/{AccountID}"
    headers = {
        "Authorization": "Bearer OZslCbLP8kmKvspMshtxNQugYPxgdiZywohPVJpOzvmhm6RenYYo8igdrPzoekCGofggKfrwTXld"
    }
    res = requests.delete(url=uri, headers=headers)
    if res.status_code == 200:
        json_res = json.loads(res.text)

        if json_res["SC"] == 200 and json_res["EC"] == 00:
            # account is successfully marked for deletion
            return {
                "statusCode": res.status_code,
                "message": "The Account is marked for deletion",
            }
    elif res.status_code == 200:
        json_res = json.loads(res.text)
        return {
            "statusCode": res.status_code,
            "message": "Account marked for deletion, but Delete Duplicate Card Check Failed",
            "error": json_res["EM"],
        }
    return {"statusCode": res.status_code, "error": res.text}


class bankOwnerCard(Schema):
    bank = fields.Nested(bankSchema)
    owner = fields.Nested(ownerSchema)
    card = fields.Nested(cardSchema)


class accountsSchema(Schema):
    sourceAccountID = fields.String(validate=validate.Length(22))
    sourceAccount = fields.Nested(bankOwnerCard)
    destinationAccountID = fields.String(validate=validate.Length(22))
    destinationAccount = fields.Nested(bankOwnerCard)


class TransactionSchema(Schema):
    referenceID = fields.Str(required=True, validate=validate.Length(min=1, max=15))
    type = fields.Str(required=True)
    amount = fields.Str(required=True)
    accounts = fields.Nested(accountsSchema)


def createTransaction(event, context):
    """
    Flow:
        validate card
        create Account
        Fetch Card details from Account
        call Pull Transactions
        call Push Transactions

     event = {
            "referenceID": "".join([str(random.randint(0, 9)) for _ in range(7)]),
            "card": {
                "accountNumber": 4005519200000004,
                "expirationDate": 202012,
                "securityCode": 344,
            },
            "owner": {
                "name": {"first": "John", "last": "Customer"},
                "address": {
                    "line1": "465 Fairchild Drive",
                    "line2": "Suite #222",
                    "city": "Mountain View",
                    "state": "CA",
                    "zipcode": "94043",
                },
                "phone": {"number": 4159808222},
            },
        }
    """

    headers = {
        "Authorization": "Bearer OZslCbLP8kmKvspMshtxNQugYPxgdiZywohPVJpOzvmhm6RenYYo8igdrPzoekCGofggKfrwTXld",
        "Content-type": "application/json",
    }

    validate_card_response = validate_card(event, "")

    if validate_card_response.get("statusCode") != 200:
        return validate_card_response

    create_account_response = create_account(event, "")
    if create_account_response.get("statusCode") != 200:
        return create_account_response

    pull_transaction_payload = {
        "referenceID": "".join([str(random.randint(0, 9)) for _ in range(7)]),
        "type": "pull",
        "accounts": {
            "sourceAccount": {
                "card": event["card"],
                "owner": event["owner"],
            },
            "destinationAccountID": "BhQ1yJYEgELKgS3Zgu7y1A",
        },
        "amount": "0.10",
    }

    transactions_url = f"{FQDN}/v1/clients/{ClientID}/transactions"
    pull_transaction_response = requests.post(
        url=transactions_url, json=pull_transaction_payload, headers=headers
    )

    pull_transaction_json_response = json.loads(pull_transaction_response.text)
    if (
        pull_transaction_json_response["SC"] == 200
        and pull_transaction_json_response["EC"] == "0"
    ):
        push_transaction_payload = {
            "referenceID": "".join([str(random.randint(0, 9)) for _ in range(7)]),
            "correspondingID": pull_transaction_json_response["transactionID"],
            "type": "push",
            "accounts": {
                "sourceAccountID": "BhQ1yJYEgELKgS3Zgu7y1A",
                "destinationAccount": {
                    "card": event["card"],
                    "owner": event["owner"],
                },
            },
            "amount": "0.10",
        }
        push_transaction_response = requests.post(
            url=transactions_url, json=push_transaction_payload, headers=headers
        )
        push_transaction_json_response = json.loads(push_transaction_response.text)
        if (
            push_transaction_json_response["SC"] == 200
            and push_transaction_json_response["EC"] == "0"
        ):
            return {
                "statusCode": push_transaction_response.status_code,
                "data": push_transaction_json_response,
                "message": "A Transaction is created and processing is completed",
            }
        return {
            "status": push_transaction_response.status_code,
            "error": push_transaction_json_response,
        }

    return {
        "status": pull_transaction_response.status_code,
        "error": pull_transaction_json_response,
    }


account_payload = {
    "referenceID": "".join([str(random.randint(0, 9)) for _ in range(7)]),
    "card": {
        "accountNumber": 4005519200000004,
        "expirationDate": 202012,
        "securityCode": 344,
    },
    "owner": {
        "name": {"first": "John", "last": "Customer"},
        "address": {
            "line1": "465 Fairchild Drive",
            "line2": "Suite #222",
            "city": "Mountain View",
            "state": "CA",
            "zipcode": "94043",
        },
        "phone": {"number": 4159808222},
    },
}
# print(createTransaction(event=account_payload, context=""))

# output
# {
#     "statusCode": 200,
#     "data": {
#         "SC": 200,
#         "EC": "0",
#         "transactionID": "xnUH5HoFKSFLd2APPPwZBA",
#         "network": "Visa",
#         "networkRC": "00",
#         "networkID": "500132008263490",
#         "status": "COMPLETED",
#         "approvalCode": "263490",
#         "fees": {"interchange": "0.10", "network": "0.00", "tabapay": "0.25"},
#     },
#     "message": "A Transaction is created and processing is completed",
# }


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


def lambda_handler(event, context):
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
        "PurchaseOrderNumber": "test017",
        "CatalogId": 1,
        "Metadata": "test",
        "CustomerOrderId": "testing0032",
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


# createKey
# create account
# use settlement account for push
# for pull destinationAccountID is settlement account ID
# we get reference ID and using it we need to push it
# use reference ID in correspondingID , sourceAccountID is settlment account ID and destinationAccount would be storecash card
