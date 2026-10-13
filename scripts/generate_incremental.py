import csv
import random
from pathlib import Path

OUTPUT = Path("data/customers_new.csv")

START_ID = 1_000_001
ROWS = 100_000

cities = [
    "Jakarta",
    "Bandung",
    "Surabaya",
    "Bogor",
    "Depok",
    "Tangerang",
    "Bekasi",
]

OUTPUT.parent.mkdir(exist_ok=True)

with OUTPUT.open("w", newline="") as file:
    writer = csv.writer(file)

    writer.writerow(["customer_id", "name", "age", "city"])

    for customer_id in range(START_ID, START_ID + ROWS):
        writer.writerow([
            customer_id,
            f"Customer_{customer_id}",
            random.randint(18, 70),
            random.choice(cities),
        ])

print(f"Generated {ROWS:,} rows → {OUTPUT}")