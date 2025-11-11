import pandas as pd

df = pd.read_csv('customer_shopping_behavior.csv')
print(df.head())

print (df.info())

print(df.describe(include='all'))

print(df.isnull().sum(0))

df['Review Rating'] = df.groupby('Category')['Review Rating'].transform(lambda x: x.fillna(x.median()))

print(df['Review Rating'])

print(df.isnull().sum())

df.columns = df.columns.str.lower()
df.columns = df.columns.str.replace(' ','_')

print(df.columns)

df = df.rename(columns={'purchase_amount_(usd)':'purchase_amount'})

print(df.columns)

print(df.columns)

'''Create a column age_group'''

labels = ['Young Adult', 'Adult', 'Middle-aged', 'Senior']
df['age_group'] = pd.qcut(df['age'], q=4, labels = labels)

print(df[['age', 'age_group']].head(10))

'''Create column purchase_frequency days'''

frequency_mapping = {
    'Fortnightly' : 14,
    'Weekly' : 7,
    'Monthly' : 30,
    'Quarterly' : 90,
    'Bi-Weekly' : 14,
    'Annually' : 365,
    'Every 3 Months' : 90
}

df['purchase_frequency_days'] =  df['frequency_of_purchases'].map(frequency_mapping)

print(df[['purchase_frequency_days','frequency_of_purchases']].head(10))

print(df[['discount_applied','promo_code_used']].head(10))

print((df['discount_applied']==df['promo_code_used']).all())

df = df.drop('promo_code_used', axis=1)

print(df.columns)

'''Connect to MySQL Workbench '''

import mysql.connector

# Create connection
conn = mysql.connector.connect(
    host="127.0.0.1",
    user="root",
    password="Minitab@137",
    database="customer_behavior"
)

cursor = conn.cursor()

# Show all tables
cursor.execute("SHOW TABLES")
tables = cursor.fetchall()

print("Available tables:")
for table in tables:
    print(f"  - {table[0]}")

# Close connection
cursor.close()
conn.close()


import pandas as pd
import mysql.connector
from sqlalchemy import create_engine
from urllib.parse import quote_plus

# Read the CSV file
df = pd.read_csv('customer_shopping_behavior.csv')

# YOUR TRANSFORMATIONS (that you already did) - ADD THEM HERE
# Clean column names
df.columns = df.columns.str.lower().str.replace(' ', '_').str.replace('(', '').str.replace(')', '').str.replace('usd', '').str.strip('_')

# Drop 'promo_code_used' column if it exists
if 'promo_code_used' in df.columns:
    df = df.drop('promo_code_used', axis=1)

# Add age_group column
df['age_group'] = pd.cut(df['age'], bins=[0, 25, 35, 50, 100], 
                         labels=['18-25', '26-35', '36-50', '50+'])

# Add purchase_frequency_days column (example - adjust based on your logic)
df['purchase_frequency_days'] = df['frequency_of_purchases'].map({
    'Weekly': 7,
    'Fortnightly': 14,
    'Monthly': 30,
    'Quarterly': 90,
    'Annually': 365,
    'Bi-Weekly': 14,
    'Every 3 Months': 90
})

print(f"Transformed data shape: {df.shape}")
print(f"Columns: {list(df.columns)}")
print(f"\nFirst few rows:")
print(df.head())

# URL-encode the password
password = quote_plus('Minitab@137')

# Create SQLAlchemy engine
engine = create_engine(f'mysql+mysqlconnector://root:{password}@127.0.0.1/customer_behavior')

# Load transformed data to MySQL
df.to_sql('customer_shopping_behavior', con=engine, if_exists='replace', index=False)

print(f"\n✅ Success! Transformed data loaded to MySQL")
print(f"   Table: customer_shopping_behavior")
print(f"   Rows: {len(df)}")
print(f"   Columns: {len(df.columns)}")

# Verify the data in MySQL
conn = mysql.connector.connect(
    host="127.0.0.1",
    user="root",
    password="Minitab@137",
    database="customer_behavior"
)

cursor = conn.cursor()

# Show tables
cursor.execute("SHOW TABLES")
print("\n Tables in database:")
for table in cursor.fetchall():
    print(f"   - {table[0]}")

# Show column names
cursor.execute("DESCRIBE customer_shopping_behavior")
print("\n Column structure:")
for col in cursor.fetchall():
    print(f"   {col[0]}: {col[1]}")

# Show sample data
cursor.execute("SELECT * FROM customer_shopping_behavior LIMIT 3")
results = cursor.fetchall()
print("\n First 3 rows from MySQL:")
for row in results:
    print(row)

cursor.close()
conn.close()