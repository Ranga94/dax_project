import requests
import json

def get_messages(symbol):
    response = requests.get("https://api.stocktwits.com/api/2/streams/symbol/{}.json".format(symbol))
    if response.status_code == 200:
        return response.json()["messages"]

def main():
    symbol = "insert ticker symbol here"
    messages = get_messages(symbol)
    for m in messages:
        print(m)
        with open('directory of output file', 'a') as outfile:
            json.dump(messages, outfile)
main()

