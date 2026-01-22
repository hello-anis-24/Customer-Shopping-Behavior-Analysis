import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import math

# ---------------- LOAD CSV ----------------
df = pd.read_csv(
    "customer_shopping_behavior.csv",
    encoding="latin1"
)

# ---------------- CONNECT TO POSTGRESQL ----------------
conn = psycopg2.connect(
    host="localhost",
    port=5432,
    database="customer_behavior",   # your DB name in pgAdmin
    user="postgres",               # your postgres username
    password="1234"                # your postgres password
)

cursor = conn.cursor()

# ---------------- INSERT QUERY ----------------
# Change table name if yours is different (e.g., customer_shopping_behavior)
table_name = "customer"

columns = [
    "customer_id",
    "age",
    "gender",
    "item_purchased",
    "category",
    "purchase_amount",
    "location",
    "size",
    "color",
    "season",
    "review_rating",
    "subscription_status",
    "shipping_type",
    "discount_applied",
    "promo_code_used",
    "previous_purchases",
    "payment_method",
    "frequency_of_purchases",
]

insert_sql = f"""
    INSERT INTO {table_name} ({", ".join(columns)})
    VALUES %s
"""

# ---------------- BULK INSERT WITH NaN FIX ----------------
batch_size = 10000
batch = []

def clean_value(v):
    # pandas may store missing numbers as float NaN
    if isinstance(v, float) and math.isnan(v):
        return None
    # also handle pandas NA / NaT if any appear
    if pd.isna(v):
        return None
    return v

try:
    for row in df.itertuples(index=False, name=None):
        cleaned_row = tuple(clean_value(v) for v in row)
        batch.append(cleaned_row)

        if len(batch) == batch_size:
            execute_values(cursor, insert_sql, batch)
            conn.commit()
            batch.clear()

    # Insert remaining rows
    if batch:
        execute_values(cursor, insert_sql, batch)
        conn.commit()

    print("✅ Bulk insert into PostgreSQL completed successfully!")

except Exception as e:
    conn.rollback()
    print("❌ Insert failed. Rolled back.")
    print("Error:", e)

finally:
    cursor.close()
    conn.close()
