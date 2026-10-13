import csv
import random
from pathlib import Path

OUTPUT = Path("data/customers_1m.csv")
ROWS = 1_000_000

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

    for customer_id in range(1, ROWS + 1):
        writer.writerow([
            customer_id,
            f"Customer_{customer_id}",
            random.randint(18, 70),
            random.choice(cities),
        ])

print(f"Generated {ROWS:,} rows → {OUTPUT}")