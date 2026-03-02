import os
import pandas as pd
from dotenv import load_dotenv
import springernature_api_client.meta as meta



# daily fetch function
def fetch_metadata(inupt_path, output_path, column_name, client, daily_limit = 450):
    '''

    '''
    df = pd.read_csv(inupt_path)
    doi_list = list(df[column_name])
    
    fetched_metadata = pd.DataFrame
    daily_query_count = 0
    fetched_dois = set()

    for doi in doi_list:
        if doi in fetched_dois:
            continue
        else:
            response = client.search(doi)
            daily_query_count += 1
            fetched_metadata = fetched_metadata.append(response)
            if daily_query_count >= 450:
                continue
            else:
                break
    
        
# 1. load input data
# 2. load / tracked already fetched dois
# 3. query client
# 4. save raw JSON
# 5. save and update doi tracker
#----------------

# function to flatten and format to CSV file

