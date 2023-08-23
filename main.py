import requests
from snowflake.connector import connect
api_url = "https://datausa.io/api/data?drilldowns=Nation&measures=Population"
response = requests.get(api_url)
#print(response.status_code)

if response.status_code == 200 :
    api_data = response.json()
else:
    print("Api request failed!!!")
    api_data = []

#print(api_data)

#transformation
transformed_data = []

for record in api_data['data']:
    transformed_data.append(record)

#print(transformed_data[0])

snowflake_config = {
    "user": "bob123",
    "password": "Bob123*#",
    "account": "am33284.central-india.azure",
    "warehouse": "COMPUTE_WH",
    "database": "ELT_DB",
    "schema": "ELT_SCHEMA"
}
conn = connect(**snowflake_config)
#print(conn)
cursor = conn.cursor()
#print(cursor)
#cursor.execute('''
#CREATE OR REPLACE TABLE ELT_DB.ELT_SCHEMA.US_POPULATION(
#    "ID Nation" VARCHAR(20),
#    NATION VARCHAR(30),
#    "ID Year" VARCHAR(4),
#    YEAR VARCHAR(4),
#    POPULATION VARCHAR(20),
#    "Slug Nation" VARCHAR(30)
#)
#''')

try:
    for row_dict in transformed_data:
        #columns = ', '.join(row_dict.keys())
        #print(columns)
        placeholders = ', '.join(['%s']*len(row_dict))
        #print(placeholders)
        values = tuple(row_dict.values())
        print(values)
        insert_query = f"INSERT INTO ELT_DB.ELT_SCHEMA.US_POPULATION (ID_Nation, NATION, ID_Year, YEAR, POPULATION, Slug_Nation) VALUES ({placeholders})"
        print(insert_query)
        cursor.execute(insert_query,values)

finally:
    cursor.close()