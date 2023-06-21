import mysql.connector
import csv

# Establish a connection to the MySQL server
cnx = mysql.connector.connect(
    user='nguyentri',
    password='minhchuoi',
    host='localhost',
    database='db_tiki',
    port=3306
)

# Create a cursor object to interact with the database
cursor = cnx.cursor()

# Function to insert data from CSV into a table
def insert_data_from_csv(file_path, table_name):
    with open(file_path, 'r') as csvfile:
        csv_data = csv.reader(csvfile)
        next(csv_data)  # Skip header row if present
        insert_query = f"INSERT INTO {table_name} VALUES ({', '.join(['%s'] * len(next(csv_data)))})"
        print(insert_query)
        
        # Insert each row of CSV data into the table
        for row in csv_data:
            try:    
                print(row)
                cursor.execute(insert_query, row)
            except:
                continue
        
        cnx.commit()
        print(f"Insert into {table_name} table completed!")


# Insert data into dim_product table
insert_data_from_csv('./dataClean/dim_product.csv', 'dim_product')
print("Insert into dim_product table completed!")

insert_data_from_csv('./dataClean/dim_product2.csv', 'dim_product')
print("Insert into dim_product2 table completed!")

insert_data_from_csv('./dataClean/dim_product3.csv', 'dim_product')
print("Insert into dim_product3 table completed!")

# Insert data into fact_category table
insert_data_from_csv('./dataClean/fact_category.csv', 'fact_category')
print("Insert into fact_category table completed!")

insert_data_from_csv('./dataClean/fact_category2.csv', 'fact_category')
print("Insert into fact_category2 table completed!")

insert_data_from_csv('./dataClean/fact_category3.csv', 'fact_category')
print("Insert into fact_category3 table completed!")

# Insert data into dim_level1_category table
insert_data_from_csv('./dataClean/dim_level1Category.csv', 'dim_level1_category')
print("Insert into dim_level1_category table completed!")

insert_data_from_csv('./dataClean/dim_level1Category2.csv', 'dim_level1_category')
print("Insert into dim_level1_category2 table completed!")

insert_data_from_csv('./dataClean/dim_level1Category3.csv', 'dim_level1_category')
print("Insert into dim_level1_category3 table completed!")

# Insert data into dim_level2_category table
insert_data_from_csv('./dataClean/dim_level2Category.csv', 'dim_level2_category')
print("Insert into dim_level2_category table completed!")

insert_data_from_csv('./dataClean/dim_level2Category2.csv', 'dim_level2_category')
print("Insert into dim_level2_category2 table completed!")

insert_data_from_csv('./dataClean/dim_level2Category3.csv', 'dim_level2_category')
print("Insert into dim_level2_category3 table completed!")

# Insert data into dim_level3_category table
insert_data_from_csv('./dataClean/dim_level3Category.csv', 'dim_level3_category')
print("Insert into dim_level3_category table completed!")

insert_data_from_csv('./dataClean/dim_level3Category2.csv', 'dim_level3_category')
print("Insert into dim_level3_category2 table completed!")

insert_data_from_csv('./dataClean/dim_level3Category3.csv', 'dim_level3_category')
print("Insert into dim_level3_category3 table completed!")


# Close the cursor and the connection
cursor.close()
cnx.close()