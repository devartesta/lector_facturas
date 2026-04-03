import os
import psycopg2

conn = psycopg2.connect(os.environ["DATABASE_URL"])
cur = conn.cursor()
cur.execute(
    "UPDATE invoices.documents SET division_invoice = 'suplidos' WHERE id = '80987338-269b-4a58-8d7b-5d158846e797'",
)
print(f"Rows updated: {cur.rowcount}")
conn.commit()
cur.close()
conn.close()
