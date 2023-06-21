import pandas as pd
import requests

def header_param2( cateId, cateTex, cateUrl):
    headers ={
    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36 Edg/114.0.1823.51',
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'en-US,en;q=0.9',
    # fix
    'Referer': f'https://tiki.vn{cateUrl}',
    'x-guest-token': '81n0c5t7OCxfZSRNe69uDWGpE3MJrVPd',
    'Connection': 'keep-alive',
    'TE': 'Trailers',
    }

    params = {
    'limit': '40',
    'include': 'advertisement',
    'aggregations': '2',
    'trackity_id': 'c5464b70-2f56-0421-5dcc-93cc46c3dd2f',
    # fix
    'category': f'{cateId}',
    'page': '1',
    'urlKey': f'{cateTex}'
    }

    return headers, params

def header_param( cateId, cateTex, cateUrl):
    headers ={
    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36 Edg/114.0.1823.51',
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'en-US,en;q=0.9',
    # fix
    'Referer': f'{cateUrl}',
    'x-guest-token': '81n0c5t7OCxfZSRNe69uDWGpE3MJrVPd',
    'Connection': 'keep-alive',
    'TE': 'Trailers',
    }

    params = {
    'limit': '40',
    'include': 'advertisement',
    'aggregations': '2',
    'trackity_id': 'c5464b70-2f56-0421-5dcc-93cc46c3dd2f',
    # fix
    'category': f'{cateId}',
    'page': '1',
    'urlKey': f'{cateTex}'
    }

    return headers, params

def read_sub(df_sub,  big_df = pd.DataFrame(), index = 4):

    # # Create an empty DataFrame
    # big_df = pd.DataFrame()
    if(index ==0):
        return

    for i in range(len(df_sub)):
        headers, params = header_param2(cateId=df_sub['query_value'][i], cateTex=df_sub['url_key'][i], cateUrl=df_sub['url_path'][i])
        response = requests.get('https://tiki.vn/api/personalish/v1/blocks/listings',headers=headers, params=params)
        if response.status_code == 200:
            filters_values = response.json().get('filters')[0].get('values')
            print('filters_values: ')
            print(filters_values)
            url_key = filters_values[0].get('url_key')
            if url_key is not None:
                temp_df = pd.DataFrame(filters_values)
                # print('temp_df: ', temp_df.head())
                big_df = pd.concat([big_df, temp_df], ignore_index=True)
                read_sub(temp_df, big_df, index=index-1)
            else:
                return
            
    return big_df

def browse(df_home):
    # Create an empty DataFrame
    big_df = pd.DataFrame()
    for i in range(len(df_home)):
        headers, params = header_param(cateId=df_home['categoryId'][i], cateTex=df_home['text'][i], cateUrl=df_home['link'][i])
        response = requests.get('https://tiki.vn/api/personalish/v1/blocks/listings',headers=headers, params=params)
        if(response.json().get('filters')[0].get('values')[0].get('url_path')== None):
            continue
        
        df_sub_home = pd.DataFrame(response.json().get('filters')[0].get('values'), columns=['count', 'display_value', 'query_value', 'url_key', 'url_path'])

        # print('df_sub_home: ', df_sub_home.head())
        # Append the temporary DataFrame to the big DataFrame
        big_df = pd.concat([big_df, df_sub_home], ignore_index=True)
        big_df = pd.concat([big_df, read_sub(df_sub_home)], ignore_index=True)
    
    return big_df

if __name__ == "__main__":
    headers ={
    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36 Edg/114.0.1823.51',
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'en-US,en;q=0.9',
    'Referer': 'https://tiki.vn/',
    'x-guest-token': '81n0c5t7OCxfZSRNe69uDWGpE3MJrVPd',
    'Connection': 'keep-alive',
    'TE': 'Trailers',
    }

    params = {
        'platform': 'desktop'
    }

    response_home = requests.get('https://api.tiki.vn/raiden/v2/menu-config',headers=headers, params=params)
    df_home = pd.DataFrame(response_home.json().get('menu_block').get('items'))
    df_home['categoryId'] = df_home['link'].str.extract(r'/c(\d+)')
    # df_home['url_key'] = df_home['link'].str.replace('https://tiki.vn/', '').str.replace('/c*', '')

    # print(df_home.head(2))
    df_home.to_csv("category_home.csv", index=False)

    df_category_link = browse(df_home)
    df_category_link.to_csv("link_category2.csv", index=False)