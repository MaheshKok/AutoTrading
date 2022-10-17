from uuid import UUID

strategy_id_name_dct = {
    1: "BankNifty Pyr_1",
    2: "Nifty50 Pyr_1",
    3: "BankNifty Pyr_10",
    4: "Nifty50 Pyr_10",
    6: "AxisBank Pyr_10_vol",
    16: "AxisBank Pyr_10_atm",
    8: "SBI Pyr_10_atm",
    9: "SBI Pyr_10_vol",
    12: "BajajAuto pyr_10_atm",
    13: "BajajAuto pyr_10_vol",
}

strategy_symbol_dict = {
    1: "BANKNIFTY",
    2: "NIFTY",
    3: "BANKNIFTY",
    4: "NIFTY",
    6: "AXISBANK",
    16: "AXISBANK",
    8: "SBIN",
    9: "SBIN",
    12: "BAJAJ-AUTO",
    13: "BAJAJ-AUTO",
}


class SYMBOL:
    BANKNIFTY = "BANKNIFTY"
    NIFTY = "NIFTY"


class BROKER:
    alice_blue_id = UUID("faeda058-2d3a-4ad6-b29f-d3fb6897cd8b")


class NFO_TYPE:
    OPTION = "option"


class ACTION:
    BUY = "buy"
    SELL = "sell"


class OPTION_TYPE:
    CE = "ce"
    PE = "pe"
