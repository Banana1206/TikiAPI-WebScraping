import pandas as pd
import re
import glob

# Extract
def extract_product_data(folder_path: str):

    # Get a list of all CSV file paths in the folder
    csv_files = glob.glob(folder_path + '*.csv')
    dataframes = []
    # Read each CSV file and append its DataFrame to the list
    for csv_file in csv_files[:500]:
        try:
            df = pd.read_csv(csv_file, on_bad_lines='skip',encoding='utf-8')
        except:
            print(csv_file)
            df = pd.read_csv(csv_file)
        dataframes.append(df)

    # Concatenate all DataFrames into a single DataFrame
    combined_df = pd.concat(dataframes, ignore_index=True)
    return combined_df

# Hàm chuyển đổi giá trị
def convert_quantity_sold(value):
    if pd.isna(value):
        return 0
    else:
        match = re.search(r"'value': (\d+)", value)
        if match:
            return int(match.group(1))
        else:
            return 0

# Tranform
def transform_product_data(df_product: pd.DataFrame()):
    df_product = df_product[['id','seller_id', 'name', 'brand_name', 'original_price', 'price', 'discount', 'discount_rate', 'quantity_sold','rating_average', 'primary_category_path']]
    df_product['quantity_sold'] = df_product['quantity_sold'].apply(convert_quantity_sold)
    df_product = df_product.rename(columns={
    'id': 'productId',
    'seller_id': 'sellerId',
    'name': 'name',
    'brand_name': 'brandName',
    'original_price': 'originalPrice',
    'price': 'price',
    'discount': 'discount',
    'discount_rate': 'discountRate',
    'quantity_sold': 'quantitySold',
    'rating_average': 'ratingAverage',
    'primary_category_path': 'categoryId'
    })

    # df_product['productId'] = df_product['productId'].astype(int)
    # df_product['sellerId'] = df_product['sellerId'].astype(int)
    # df_product['name'] = df_product['name'].astype(str)
    # df_product['brandName'] = df_product['brandName'].astype(str)
    # df_product['originalPrice'] = df_product['originalPrice'].astype(float)
    # df_product['price'] = df_product['price'].astype(float)
    # df_product['discount'] = df_product['discount'].astype(float)
    # df_product['discountRate'] = df_product['discountRate'].astype(float)
    # df_product['quantitySold'] = df_product['quantitySold'].astype(int)
    # df_product['ratingAverage'] = df_product['ratingAverage'].astype(float)
   
    # df_product_category = pd.DataFrame()
    df_product_category = df_product[['productId','categoryId']]
    df_product_category['categoryId'] = df_product_category['categoryId'].str.split('/')
    df_product_category=df_product_category.dropna()
    
    df_product = df_product[['productId','sellerId','name','brandName','originalPrice','price','discount','discountRate','quantitySold','ratingAverage']]

    return df_product, df_product_category

def transform_category_data(df_product_category: pd.DataFrame()):

    # Add table
    df_product_category['level3CategoryId'] = df_product_category['categoryId'].apply(lambda x: x[2:])
    df_product_category['level2CategoryId'] = df_product_category['categoryId'].apply(lambda x: x[2:4])
    df_product_category['level1CategoryId'] = df_product_category['categoryId'].apply(lambda x: x[2])

    # read all raw category table
    df_cate = pd.read_csv('link_category2.csv')
    df_cate = df_cate[['query_value', 'display_value']]
    df_cate = df_cate.rename(columns={'query_value':'categoryId','display_value':'categoryName'}).drop_duplicates()


    # process dim_level1Category
    df_cate_home = pd.read_csv('category_home.csv')
    df_cate_home =df_cate_home.rename(columns={"text":"categoryName"})
    df_cate_home = df_cate_home[['categoryId','categoryName']].sort_values('categoryId')
    df_cate_home = df_cate_home.astype({'categoryId':int,'categoryName':str})

    # process dim_level2Category
    df_product_category2 = pd.DataFrame()
    df_product_category2['level2CategoryId'] = df_product_category['level2CategoryId'].apply(lambda x: x[1])
    df_product_category2['level1CategoryId'] = df_product_category['level2CategoryId'].apply(lambda x: x[0])
    df_product_category2 = df_product_category2.drop_duplicates()
    df_product_category2 = df_product_category2.astype({'level2CategoryId': int, 'level1CategoryId': int})
    df_cate2 = pd.merge(df_product_category2, df_cate, left_on='level2CategoryId', right_on='categoryId')
    df_cate2 = df_cate2[['level2CategoryId','level1CategoryId','categoryName']]

     # process dim_level3Category
    df_product_category3 = pd.DataFrame()
    df_product_category3['level3CategoryId'] = df_product_category['level3CategoryId'].apply(lambda x: x[-1])
    df_product_category3['level2CategoryId'] = df_product_category['level3CategoryId'].apply(lambda x: x[1])
    df_product_category3['level1CategoryId'] = df_product_category['level3CategoryId'].apply(lambda x: x[0])
    df_product_category3=df_product_category3.drop_duplicates()
    df_product_category3 = df_product_category3.astype({'level3CategoryId': int,'level2CategoryId': int, 'level1CategoryId': int})
    df_cate3 = pd.merge(df_product_category3, df_cate, left_on='level3CategoryId', right_on='categoryId')
    df_cate3 = df_cate3[['level3CategoryId','level2CategoryId', 'level1CategoryId','categoryName']]

     # process fact_Category
    df_fact_cate = df_product_category.explode('level3CategoryId', ignore_index=True)[['productId',	'level3CategoryId']]
    df_fact_cate = df_fact_cate.rename(columns={'level3CategoryId': 'categoryId'})

    return df_fact_cate.drop_duplicates(), df_cate_home.drop_duplicates(), df_cate2, df_cate3


if __name__=="__main__":
    folderPath = './data2/'
    df_pro = extract_product_data(folder_path=folderPath)
    print("================================")
    print("Extract")
    print(df_pro.tail())

    df_pro_clean, df_product_category = transform_product_data(df_pro)
    df_pro_clean = df_pro_clean.drop_duplicates(subset=['productId'], keep='last')
    print("================================")
    print("Transform df_pro")
    print(df_pro_clean.tail())
    print("================================")
    print("df_product_category")
    print(df_product_category.tail())


    df_fact_cate, df_cate1, df_cate2, df_cate3 = transform_category_data(df_product_category)
    print("================================")
    print("Fact Cate")
    print(df_fact_cate.tail())

    print("================================")
    print("Dim Cate 1")
    print(df_cate1.tail())

    print("================================")
    print("Dim Cate 2")
    print(df_cate2.tail())

    print("================================")
    print("Dim Cate 3")
    print(df_cate3.tail())

    df_pro_clean.to_csv('./dataClean/dim_product.csv', index=False)
    df_fact_cate.to_csv('./dataClean/fact_category.csv', index=False)
    df_cate1.to_csv('./dataClean/dim_level1Category.csv', index=False)
    df_cate2.to_csv('./dataClean/dim_level2Category.csv', index=False)
    df_cate3.to_csv('./dataClean/dim_level3Category.csv', index=False)

    # df_pro_clean.to_csv('./dataClean/dim_product2.csv', index=False)
    # df_fact_cate.to_csv('./dataClean/fact_category2.csv', index=False)
    # df_cate1.to_csv('./dataClean/dim_level1Category2.csv', index=False)
    # df_cate2.to_csv('./dataClean/dim_level2Category2.csv', index=False)
    # df_cate3.to_csv('./dataClean/dim_level3Category2.csv', index=False)

    # df_pro_clean.to_csv('./dataClean/dim_product3.csv', index=False)
    # df_fact_cate.to_csv('./dataClean/fact_category3.csv', index=False)
    # df_cate1.to_csv('./dataClean/dim_level1Category3.csv', index=False)
    # df_cate2.to_csv('./dataClean/dim_level2Category3.csv', index=False)
    # df_cate3.to_csv('./dataClean/dim_level3Category3.csv', index=False)











