import mysql.connector
import pandas as pd

# Database 
db_connection = mysql.connector.connect(
    host="localhost",
    user="your_username",
    password="your_password",
    database="your_database"
)

# citizen income data
query_citizens = "SELECT citizen_id, name, income FROM citizens"
citizens_df = pd.read_sql(query_citizens, con=db_connection)

# tax bracket information
query_brackets = "SELECT * FROM tax_brackets"
brackets_df = pd.read_sql(query_brackets, con=db_connection)

# calculate tax by bracket
def calculate_tax(income, brackets):
    tax = 0
    for index, row in brackets.iterrows():
        if pd.isnull(row['income_max']):  # Upper bracket (no income limit)
            tax += (income - row['income_min']) * row['tax_rate']
            break
        elif income > row['income_max']:
            tax += (row['income_max'] - row['income_min']) * row['tax_rate']
        else:
            tax += (income - row['income_min']) * row['tax_rate']
            break
    return tax

#  Calculate tax for each citizen
citizens_df['tax_paid'] = citizens_df['income'].apply(calculate_tax, brackets=brackets_df)

# Update the database with tax information
cursor = db_connection.cursor()

for index, row in citizens_df.iterrows():
    update_query = f"UPDATE citizens SET tax_paid = {row['tax_paid']} WHERE citizen_id = {row['citizen_id']}"
    cursor.execute(update_query)

db_connection.commit()

# Generate a report of total taxes paid by income group
income_groups = pd.cut(citizens_df['income'], bins=[0, 30000, 100000, float('inf')], labels=['Low', 'Middle', 'High'])
report = citizens_df.groupby(income_groups)['tax_paid'].sum()

print("Tax Revenue by Income Group:")
print(report)

# Close the database connection
cursor.close()
db_connection.close()
