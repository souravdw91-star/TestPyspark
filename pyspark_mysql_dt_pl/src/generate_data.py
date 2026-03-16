import csv
import random

products = [
    ("Laptop", 50000),
    ("Mouse", 500),
    ("Keyboard", 1500),
    ("Monitor", 12000),
    ("Headphones", 2000),
    ("USB Cable", 200),
    ("Webcam", 3500),
    ("Speaker", 2500)
]

with open("data\\raw\\sales.csv", "w", newline="") as f:
    writer = csv.writer(f)

    # header
    writer.writerow(["id", "product", "price", "quantity", "date"])

    for i in range(1, 1001):
        product, price = random.choice(products)
        quantity = random.randint(1, 10)
        date = f"2024-{random.randint(1, 12):02d}-{random.randint(1, 28):02d}"

        writer.writerow([i, product, price, quantity, date  ])

print("CSV file with 1000 rows created.")