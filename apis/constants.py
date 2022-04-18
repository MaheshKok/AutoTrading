import requests

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


class TRADES:
    BANKNIFTY = "BANKNIFTY"
    NIFTY = "NIFTY"

# TODO do not remove this unless you are confident that above code will work for atleast 15 days
# expiry = "30SEP2021"
# url = f"https://vbiz.in/optionchain/foc.php?symbol={symbol}&expiry={expiry}"
# headers = {
#     "accept": "*/*",
#     "accept-encoding": "gzip, deflate, br",
#     "accept-language": "en-IN,en;q=0.9,hi-IN;q=0.8,hi;q=0.7,mr-IN;q=0.6,mr;q=0.5,en-GB;q=0.4,en-US;q=0.3",
#     "cookie": "_ga=GA1.2.1230482458.1627675123; _gcl_au=1.1.598369330.1627675123; __tawkuuid=e::vbiz.in::vPlQa5Qtjn+TEni/bUWAUpK8oSnd68QMNNjIuP+nfD/Xydd6X71pTiw950uWVEwr::2; HstCfa4543512=1632894969432; HstCla4543512=1632894969432; HstCmu4543512=1632894969432; HstPn4543512=1; HstPt4543512=1; HstCnv4543512=1; HstCns4543512=1",
#     "referer": "https://vbiz.in/nseoptionchain.html",
#     "sec-ch-ua": 'Google Chrome";v="93", " Not;A Brand";v="99", "Chromium";v="93',
#     "sec-ch-ua-mobile": "?0",
#     "sec-fetch-dest": "empty",
#     "sec-fetch-mode": "cors",
#     "sec-fetch-site": "same-origin",
#     "sec-ch-ua-platform": "macOS",
#     "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.82 Safari/537.36",
# }
#
# return requests.get(url=url, headers=headers)
