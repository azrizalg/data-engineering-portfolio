import csv
import random
from pathlib import Path

OUTPUT = Path("data/customers_messy.csv")

cities = [
    "Jakarta",
    "Bandung",
    "Surabaya",
    "Bogor",
]

OUTPUT.parent.mkdir(exist_ok=True)

with OUTPUT.open("w", newline="") as file:
    writer = csv.writer(file)

    writer.writerow(["customer_id", "name", "age", "city"])

    # Existing IDs
    for customer_id in range(1, 101):
        writer.writerow([
            customer_id,
            f"Existing_{customer_id}",
            random.randint(18, 70),
            random.choice(cities),
        ])

    # New IDs
    for customer_id in range(1_100_001, 1_100_901):
        writer.writerow([
            customer_id,
            f"New_{customer_id}",
            random.randint(18, 70),
            random.choice(cities),
        ])

    # Invalid ages
    for customer_id in range(1_100_901, 1_101_001):
        writer.writerow([
            customer_id,
            f"Invalid_{customer_id}",
            random.choice([-10, 0, 150, 200]),
            random.choice(cities),
        ])

print(f"Generated {OUTPUT}")