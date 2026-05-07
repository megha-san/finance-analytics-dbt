"""
FinTech Synthetic Data Generator
=================================
Generates 4 CSV files:
  - customers.csv     (~50,000 rows)
  - merchants.csv     (~5,000 rows)
  - cards.csv         (~80,000 rows)
  - transactions.csv  (~5,000,000 rows)

Usage:
  pip install faker pandas numpy tqdm
  python generate_data.py

Output: ./output/ directory
Upload to GCS:
  gsutil -m cp output/*.csv gs://<YOUR_BUCKET>/raw/
"""

import os
import random
import numpy as np
import pandas as pd
from faker import Faker
from datetime import datetime, timedelta
from tqdm import tqdm
import warnings
warnings.filterwarnings("ignore")

fake = Faker()
Faker.seed(42)
random.seed(42)
np.random.seed(42)

OUTPUT_DIR = "./output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ── CONFIG ────────────────────────────────────────────────────────────────────
N_CUSTOMERS    = 50_000
N_MERCHANTS    = 5_000
N_CARDS        = 80_000
N_TRANSACTIONS = 5_000_000
FRAUD_RATE     = 0.02          # 2% base fraud rate — realistic for card payments
START_DATE     = datetime(2022, 1, 1)
END_DATE       = datetime(2024, 12, 31)

COUNTRIES = {
    "CA": 0.35, "US": 0.30, "GB": 0.10, "IN": 0.08,
    "AU": 0.05, "DE": 0.04, "FR": 0.03, "SG": 0.03, "NG": 0.02
}

MERCHANT_CATEGORIES = {
    "GROCERY":          0.18,
    "TRAVEL":           0.12,
    "RESTAURANT":       0.14,
    "ENTERTAINMENT":    0.08,
    "RETAIL":           0.16,
    "FUEL":             0.07,
    "HEALTHCARE":       0.06,
    "UTILITIES":        0.05,
    "ONLINE_GAMING":    0.06,   # higher fraud signal
    "CRYPTO_EXCHANGE":  0.04,   # higher fraud signal
    "MONEY_TRANSFER":   0.04,   # higher fraud signal
}

HIGH_RISK_CATEGORIES = {"ONLINE_GAMING", "CRYPTO_EXCHANGE", "MONEY_TRANSFER"}

FRAUD_TYPES = [
    "card_not_present",
    "velocity_abuse",
    "account_takeover",
    "stolen_card",
    "synthetic_identity",
]

RISK_TIERS = ["LOW", "MEDIUM", "HIGH", "VERY_HIGH"]

CARD_TYPES = ["VISA_CREDIT", "VISA_DEBIT", "MASTERCARD_CREDIT",
              "MASTERCARD_DEBIT", "AMEX_CREDIT"]


def random_date(start: datetime, end: datetime) -> datetime:
    delta = end - start
    return start + timedelta(seconds=random.randint(0, int(delta.total_seconds())))


def weighted_choice(choices: dict) -> str:
    keys = list(choices.keys())
    weights = list(choices.values())
    return random.choices(keys, weights=weights, k=1)[0]


# ── 1. CUSTOMERS ──────────────────────────────────────────────────────────────
print("Generating customers...")

def age_band(age: int) -> str:
    if age < 26:   return "18-25"
    if age < 41:   return "26-40"
    if age < 61:   return "41-60"
    return "61+"

customers = []
for i in tqdm(range(N_CUSTOMERS)):
    age = random.randint(18, 80)
    country = weighted_choice(COUNTRIES)
    customers.append({
        "customer_id":   f"CUST_{i+1:06d}",
        "first_name":    fake.first_name(),
        "last_name":     fake.last_name(),
        "email":         fake.email(),
        "age":           age,
        "age_band":      age_band(age),
        "country":       country,
        "city":          fake.city(),
        "signup_date":   random_date(datetime(2018, 1, 1), START_DATE).date(),
        "risk_tier":     random.choices(
                             RISK_TIERS,
                             weights=[0.55, 0.25, 0.15, 0.05]
                         )[0],
        "is_active":     random.choices([True, False], weights=[0.92, 0.08])[0],
    })

customers_df = pd.DataFrame(customers)
customers_df.to_csv(f"{OUTPUT_DIR}/customers.csv", index=False)
print(f"  ✓ {len(customers_df):,} customers written")


# ── 2. MERCHANTS ──────────────────────────────────────────────────────────────
print("Generating merchants...")

merchants = []
for i in tqdm(range(N_MERCHANTS)):
    category = weighted_choice(MERCHANT_CATEGORIES)
    country = weighted_choice(COUNTRIES)
    is_high_risk = category in HIGH_RISK_CATEGORIES
    merchants.append({
        "merchant_id":       f"MERCH_{i+1:05d}",
        "merchant_name":     fake.company(),
        "category_code":     category,
        "country":           country,
        "city":              fake.city(),
        "is_high_risk":      is_high_risk,
        "onboarded_date":    random_date(datetime(2018, 1, 1), START_DATE).date(),
        "monthly_txn_limit": random.choice([10_000, 50_000, 100_000, 500_000]),
    })

merchants_df = pd.DataFrame(merchants)
merchants_df.to_csv(f"{OUTPUT_DIR}/merchants.csv", index=False)
print(f"  ✓ {len(merchants_df):,} merchants written")


# ── 3. CARDS ──────────────────────────────────────────────────────────────────
print("Generating cards...")

customer_ids = customers_df["customer_id"].tolist()

cards = []
for i in tqdm(range(N_CARDS)):
    issue_date = random_date(datetime(2018, 1, 1), datetime(2024, 6, 1))
    expiry_date = issue_date + timedelta(days=365 * random.randint(2, 5))
    cards.append({
        "card_id":         f"CARD_{i+1:07d}",
        "customer_id":     random.choice(customer_ids),
        "card_type":       random.choice(CARD_TYPES),
        "card_last4":      f"{random.randint(1000,9999)}",
        "issue_date":      issue_date.date(),
        "expiry_date":     expiry_date.date(),
        "is_expired":      expiry_date.date() < datetime(2024, 12, 31).date(),
        "is_blocked":      random.choices([False, True], weights=[0.97, 0.03])[0],
    })

cards_df = pd.DataFrame(cards)
cards_df.to_csv(f"{OUTPUT_DIR}/cards.csv", index=False)
print(f"  ✓ {len(cards_df):,} cards written")


# ── 4. TRANSACTIONS ───────────────────────────────────────────────────────────
print(f"Generating {N_TRANSACTIONS:,} transactions (this takes ~3-5 mins)...")

card_ids      = cards_df["card_id"].tolist()
merchant_ids  = merchants_df["merchant_id"].tolist()

# Pre-build merchant country lookup for cross-border flag
merch_country = dict(zip(merchants_df["merchant_id"], merchants_df["country"]))
# Pre-build card → customer lookup
card_to_cust  = dict(zip(cards_df["card_id"], cards_df["customer_id"]))
# Pre-build customer country lookup
cust_country  = dict(zip(customers_df["customer_id"], customers_df["country"]))

CURRENCIES = {"CA": "CAD", "US": "USD", "GB": "GBP", "IN": "INR",
              "AU": "AUD", "DE": "EUR", "FR": "EUR", "SG": "SGD",
              "NG": "NGN"}

CHUNK_SIZE = 250_000
chunks_written = 0
header_written = False

for chunk_start in tqdm(range(0, N_TRANSACTIONS, CHUNK_SIZE)):
    chunk_size = min(CHUNK_SIZE, N_TRANSACTIONS - chunk_start)

    txn_card_ids     = np.random.choice(card_ids, chunk_size)
    txn_merch_ids    = np.random.choice(merchant_ids, chunk_size)
    is_fraud_arr     = np.random.choice([True, False], chunk_size,
                                         p=[FRAUD_RATE, 1 - FRAUD_RATE])

    # Amounts: log-normal gives realistic payment distribution
    amounts = np.round(np.random.lognormal(mean=3.5, sigma=1.2, size=chunk_size), 2)
    # Fraud transactions tend to be larger
    amounts = np.where(is_fraud_arr, amounts * np.random.uniform(1.5, 4.0, chunk_size), amounts)
    amounts = np.clip(amounts, 0.50, 50_000.00)

    # Timestamps — skew toward business hours (9-21) for realism
    base_seconds    = int((END_DATE - START_DATE).total_seconds())
    random_offsets  = np.random.randint(0, base_seconds, chunk_size)
    timestamps      = [START_DATE + timedelta(seconds=int(s)) for s in random_offsets]

    fraud_types = np.where(
        is_fraud_arr,
        np.random.choice(FRAUD_TYPES, chunk_size),
        None
    )

    chunk_records = []
    for j in range(chunk_size):
        card_id   = txn_card_ids[j]
        merch_id  = txn_merch_ids[j]
        cust_id   = card_to_cust.get(card_id, "UNKNOWN")
        c_country = cust_country.get(cust_id, "CA")
        m_country = merch_country.get(merch_id, "CA")
        ts        = timestamps[j]

        chunk_records.append({
            "transaction_id":   f"TXN_{chunk_start + j + 1:08d}",
            "card_id":          card_id,
            "customer_id":      cust_id,
            "merchant_id":      merch_id,
            "amount":           float(amounts[j]),
            "currency":         CURRENCIES.get(c_country, "USD"),
            "transaction_ts":   ts.strftime("%Y-%m-%d %H:%M:%S"),
            "transaction_date": ts.strftime("%Y-%m-%d"),
            "hour_of_day":      ts.hour,
            "day_of_week":      ts.strftime("%A"),
            "is_fraudulent":    bool(is_fraud_arr[j]),
            "fraud_type":       fraud_types[j],
            "is_cross_border":  c_country != m_country,
            "status":           random.choices(
                                    ["COMPLETED", "DECLINED", "REVERSED"],
                                    weights=[0.91, 0.06, 0.03]
                                )[0],
        })

    chunk_df = pd.DataFrame(chunk_records)
    chunk_df.to_csv(
        f"{OUTPUT_DIR}/transactions.csv",
        mode="a",
        header=not header_written,
        index=False
    )
    header_written = True
    chunks_written += 1

print(f"  ✓ {N_TRANSACTIONS:,} transactions written")

# ── SUMMARY ───────────────────────────────────────────────────────────────────
print("\n── Summary ──────────────────────────────────────────────────────────")
for fname in ["customers.csv", "merchants.csv", "cards.csv", "transactions.csv"]:
    size_mb = os.path.getsize(f"{OUTPUT_DIR}/{fname}") / 1024 / 1024
    print(f"  {fname:<22} {size_mb:>8.1f} MB")

print("""
── Next Steps ───────────────────────────────────────────────────────────
1. Create GCS bucket:
     gsutil mb -l northamerica-northeast1 gs://<YOUR_BUCKET>

2. Upload files:
     gsutil -m cp output/*.csv gs://<YOUR_BUCKET>/raw/

3. Load into BigQuery (repeat for each table):
     bq load --autodetect --source_format=CSV \\
       <PROJECT>:raw.transactions \\
       gs://<YOUR_BUCKET>/raw/transactions.csv

4. Run dbt:
     cd ../dbt_project && dbt run
""")
