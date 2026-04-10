import requests


"""

def process_bitcoin(ti):
    responde = ti.xcom_pull(task_ids="extract_bitcoin")
    logging.info(response)
    processed_data = {"usd": reponse["usd"], "change":["usd_24_change"]}
    ti.xcom_push(key="processed_data", value=processed_data)
    
"""


response = requests.get('https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT')
data = response.json()
processed_data = {"BTCUSD": data["price"]} # Eu passo a chave do JSON e oque é para puxar, nesse caso seria o preço
print(data)

