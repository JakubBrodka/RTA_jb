from kafka import KafkaConsumer
from collections import Counter, defaultdict, deque
import json
from datetime import datetime, timedelta

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    auto_offset_reset='earliest',
    group_id='count-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

store_counts = Counter()
total_amount = defaultdict(float)
msg_count = 0

user_transactions = defaultdict(deque)

WINDOW_SECONDS = 60
MAX_TRANSACTIONS = 3


def parse_timestamp(timestamp: str) -> datetime:
    return datetime.fromisoformat(timestamp)


for message in consumer:
    tx = message.value

    tx_id = tx["tx_id"]
    user_id = tx["user_id"]
    amount = tx["amount"]
    store = tx["store"]
    category = tx["category"]
    timestamp = parse_timestamp(tx["timestamp"])

    user_transactions[user_id].append(timestamp)

    while user_transactions[user_id] and timestamp - user_transactions[user_id][0] > timedelta(seconds=WINDOW_SECONDS):
        user_transactions[user_id].popleft()

    if len(user_transactions[user_id]) > MAX_TRANSACTIONS:
        print(
            f"[ALERT] Anomalia prędkości! "
            f"user_id={user_id}, "
            f"liczba_transakcji={len(user_transactions[user_id])}, "
            f"okno={WINDOW_SECONDS}s, "
            f"tx_id={tx_id}, "
            f"timestamp={tx['timestamp']}"
        )