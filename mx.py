import json

from marshmallow import Schema, fields
import requests


headers = {
    "Accept": "application/vnd.mx.api.v1+json",
    "Content-Type": "application/json",
}
blueprint_api = "https://int-api.mx.com"

user_api = f"{blueprint_api}/users"
user_guid = ""

member_api = f"{user_api}/{user_guid}/managed_members"
member_guid = ""

account_api = f"{member_api}/{member_guid}/accounts"
account_guid = ""

transaction_api = f"{account_api}/{account_guid}/transactions"
transaction_guid = ""

merchant_api = f"{blueprint_api}/merchants"
merchant_guid = ""

category_api = f"{user_api}/{user_guid}/categories"


class UserSchema(Schema):
    email = fields.String()
    guid = fields.String()
    id = fields.String()
    is_disabled = fields.Boolean()
    # TODO check if fields support json
    metadata = fields.Dict()


class MemberSchema(Schema):
    aggregated_at = fields.String()
    connection_status = fields.String()
    guid = fields.String()
    id = fields.String()
    institution_code = fields.String()
    is_being_aggregated = fields.Boolean()
    is_managed_by_user = fields.Boolean()
    is_oauth = fields.Boolean()
    metadata = fields.String()
    name = fields.String()
    successfully_aggregated_at = fields.String()
    user_guid = fields.String()
    user_id = fields.String()


class AccountSchema(Schema):
    account_number = fields.String()
    apr = fields.Float()
    apy = fields.Float()
    available_balance = fields.Float()
    available_credit = fields.Float()
    balance = fields.Float()
    cash_balance = fields.Float()
    cash_surrender_value = fields.Float()
    created_at = fields.DateTime()
    credit_limit = fields.Float()
    currency_code = fields.String()
    day_payment_is_due = fields.Integer()
    death_benefit = fields.Integer()
    guid = fields.String()
    holdings_value = fields.Float()
    id = fields.String()
    imported_at = fields.DateTime()
    institution_code = fields.String()
    insured_name = fields.String()
    interest_rate = fields.Float()
    is_closed = fields.Boolean()
    is_hidden = fields.Boolean()
    last_payment = fields.Float()
    last_payment_at = fields.DateTime()
    loan_amount = fields.Float()
    matures_on = fields.DateTime()
    member_guid = fields.String()
    member_id = fields.String()
    member_is_managed_by_user = fields.Boolean()
    metadata = fields.String()
    minimum_balance = fields.Float()
    minimum_payment = fields.Float()
    name = fields.String()
    nickname = fields.String()
    original_balance = fields.Float()
    pay_out_amount = fields.Float()
    payment_due_at = fields.DateTime()
    payoff_balance = fields.Float()
    premium_amount = fields.Float()
    routing_number = fields.String()
    started_on = fields.DateTime()
    subtype = fields.String()
    total_account_value = fields.Float()
    type = fields.String()
    updated_at = fields.DateTime()
    user_guid = fields.String()
    user_id = fields.String()


class TransactionSchema(Schema):
    account_guid = fields.String()
    account_id = fields.String()
    amount = fields.String()
    category = fields.String()
    category_guid = fields.String()
    check_number_string = fields.String()
    created_at = fields.DateTime()
    currency_code = fields.String()
    date = fields.DateTime()
    description = fields.String()
    guid = fields.String()
    id = fields.String()
    is_bill_pay = fields.Boolean()
    is_direct_deposit = fields.Boolean()
    is_expense = fields.Boolean()
    is_fee = fields.Boolean()
    is_income = fields.Boolean()
    is_international = fields.Boolean()
    is_overdraft_fee = fields.Boolean()
    is_payroll_advance = fields.Boolean()
    is_recurring = fields.Boolean()
    is_subscription = fields.Boolean()
    latitude = fields.String()
    localized_description = fields.String()
    localized_memo = fields.String()
    longitude = fields.String()
    member_guid = fields.String()
    member_is_managed_by_user = fields.Boolean()
    memo = fields.String()
    merchant_category_code = fields.String()
    merchant_guid = fields.String()
    merchant_location_guid = fields.String()
    metadata = fields.String()
    original_description = fields.String()
    posted_at = fields.DateTime()
    status = fields.String()
    top_level_category = fields.String()
    transacted_at = fields.DateTime()
    type = fields.String()
    updated_at = fields.DateTime()
    user_guid = fields.String()
    user_id = fields.String()


class MerchantSchema(Schema):
    created_at = fields.DateTime()
    guid = fields.String()
    logo_url = fields.String()
    name = fields.String()
    updated_at = fields.DateTime()
    website_url = fields.String()


class CategorySchema(Schema):
    created_at = fields.DateTime()
    guid = fields.String()
    is_default = fields.Boolean()
    is_income = fields.Boolean()
    metadata = fields.String()
    name = fields.String()
    parent_guid = fields.String()
    updated_at = fields.DateTime()


def create_user(payload):
    response = requests.post(json=payload, url=user_api, headers=headers)

    if response.status_code == 200:
        try:
            UserSchema().load(response.json()["user"])
        except Exception as e:
            print("error occurred validating create user response: " + str(e))

    return response.json()


def create_member(payload):
    response = requests.post(json=payload, url=member_api, headers=headers)

    if response.status_code == 200:
        try:
            MemberSchema().load(response.json()["member"])
        except Exception as e:
            print(f"error occurred validating create member response: {e}")

    return response.json()


def create_account(payload):
    response = requests.post(json=payload, url=account_api, headers=headers)

    if response.status_code == 200:
        try:
            AccountSchema().load(response.json()["account"])
        except Exception as e:
            print(f"error occurred validating create account response: {e}")

    return response.json()


def create_transaction(payload):
    response = requests.post(json=payload, url=transaction_api, headers=headers)

    if response.status_code == 200:
        try:
            TransactionSchema().load(response.json()["transaction"])
        except Exception as e:
            print(f"error occurred validating create transaction response: {e}")

    return response.json()


def read_transaction():
    response = requests.get(
        url=f"{transaction_api}/{transaction_guid}", headers=headers
    )

    if response.status_code == 200:
        try:
            TransactionSchema().load(response.json()["transaction"])
        except Exception as e:
            print(f"error occurred validating read transaction response: {e}")

    return response.json()


def list_transactions():
    response = requests.get(url=transaction_api, headers=headers)

    if response.status_code == 200:
        try:
            TransactionSchema(many=True).load(response.json()["transactions"])
        except Exception as e:
            print(f"error occurred validating list transactions response: {e}")

    return response.json()


def read_merchant():
    response = requests.get(url=f"{merchant_api}/{merchant_guid}", headers=headers)

    if response.status_code == 200:
        try:
            MerchantSchema().load(response.json()["merchant"])
        except Exception as e:
            print(f"error occurred validating read merchant response: {e}")

    return response.json()


def list_merchants():
    response = requests.get(url=merchant_api, headers=headers)

    if response.status_code == 200:
        try:
            MerchantSchema(many=True).load(response.json()["merchants"])
        except Exception as e:
            print(f"error occurred validating list transactions response: {e}")

    return response.json()


def list_categories():
    response = requests.get(url=category_api, headers=headers)

    if response.status_code == 200:
        try:
            CategorySchema(many=True).load(response.json()["categories"])
        except Exception as e:
            print(f"error occurred validating list categories response: {e}")

    return response.json()


def lambda_handle(event, context):
    try:
        event_body = json.loads(event["body"])
    except:
        event_body = event

    action = event_body.get("action", None)

    if action == "create_user":
        return create_user(event.get("user"))

    if action == "create_member":
        return create_member(event)

    if action == "create_account":
        return create_account(event)

    if action == "create_transaction":
        return create_transaction(event)

    if action == "read_transaction":
        return read_transaction()

    if action == "list_transactions":
        return list_transactions()

    if action == "read_merchant":
        return read_merchant()

    if action == "list_merchants":
        return list_merchants()

    if action == "list_categories":
        return list_categories()


# For Testing use below payload


create_user_payload = {
    "user": {
        "email": "email@provider.com",
        "id": "My-Unique-ID",
        "is_disabled": False,
        "metadata": "some metadata",
    }
}


create_member_payload = {
    "member": {
        "id": "member123",
        "institution_code": "mxbank",
        "metadata": "some metadata",
        "name": "MX Bank",
    }
}


create_account_payload = {
    "account": {
        "account_number": "5366",
        "apr": 1.0,
        "apy": 1.0,
        "available_balance": 1000.0,
        "available_credit": 1000.0,
        "balance": 1000.0,
        "cash_surrender_value": 1000.0,
        "credit_limit": 100.0,
        "currency_code": "USD",
        "day_payment_is_due": 20,
        "death_benefit": 1000,
        "id": "1040434698",
        "interest_rate": 1.0,
        "is_closed": False,
        "is_hidden": False,
        "last_payment": 100.0,
        "last_payment_at": "2015-10-13T17:57:37.000Z",
        "loan_amount": 1000.0,
        "matures_on": "2015-10-13T17:57:37.000Z",
        "metadata": "some metadata",
        "minimum_balance": 100.0,
        "minimum_payment": 10.0,
        "name": "Test account 2",
        "nickname": "Swiss Account",
        "original_balance": 10.0,
        "payment_due_at": "2015-10-13T17:57:37.000Z",
        "payoff_balance": 10.0,
        "routing_number": "68899990000000",
        "started_on": "2015-10-13T17:57:37.000Z",
        "subtype": "NONE",
        "type": "SAVINGS",
    }
}

create_transaction_payload = {
    "transaction": {
        "amount": "61.11",
        "category": "Groceries",
        "check_number_string": "6812",
        "currency_code": "USD",
        "description": "Whole foods",
        "id": "transaction-265abee9-889b-af6a-c69b-25157db2bdd9",
        "is_international": False,
        "latitude": -43.2075,
        "localized_description": "This is a localized_description",
        "localized_memo": "This is a localized_memo",
        "longitude": 139.691706,
        "memo": "This is a memo",
        "merchant_category_code": 5411,
        "merchant_guid": "MCH-7ed79542-884d-2b1b-dd74-501c5cc9d25b",
        "merchant_location_guid": "MCL-00024e59-18b5-4d79-b879-2a7896726fea",
        "metadata": "some metadata",
        "posted_at": "2016-10-07T06:00:00.000Z",
        "status": "POSTED",
        "transacted_at": "2016-10-06T13:00:00.000Z",
        "type": "DEBIT",
    }
}

action_list = [
    "create_user",
    "create_member",
    "create_account",
    "create_transaction",
    "read_transaction",
    "list_transactions",
    "read_merchant",
    "list_merchants",
    "list_categories",
]


payload_list = [
    "create_user_payload",
    "create_member_payload",
    "create_account_payload",
    "create_transaction_payload",
]

step = 0
lambda_handler_event = {
    "body": payload_list[step],
    "action": action_list[step],
}


print(lambda_handle(lambda_handler_event, ""))
