import os
import pandas as pd
import json
from dotenv import load_dotenv
import springernature_api_client.meta as meta



# daily fetch function
def fetch_metadata(input_paths, output_path, column_name, client, path_fetched_dois, daily_limit = 450):
    '''

    '''
    if isinstance(input_paths, str):
        input_paths = [input_paths]

    # Collect DOIs from all input files
    temp_doi_list = []
    for path in input_paths:
        df = pd.read_csv(path)
        temp_doi_list.append(df[column_name])

    # Combine, drop NaN, preserve order, remove duplicates
    all_dois = pd.concat(temp_doi_list).dropna().astype(str)
    doi_list = pd.unique(all_dois)

    
    # Load fetched DOIs if file exists
    if os.path.exists(path_fetched_dois):
        fetched_dois = set(pd.read_csv(path_fetched_dois)["doi"])
    else:
        fetched_dois = set()

    # Create JSON file and file for fetched DOIs
    os.makedirs(os.path.dirname(output_path), exist_ok = True)
    os.makedirs(os.path.dirname(path_fetched_dois), exist_ok = True)

    # Create df and counter
    daily_query_count = 0
    new_fetched_dois = set()

    # Open JSON file in append mode
    with open(output_path, "a") as json_file:

        for doi in doi_list:

            if doi in fetched_dois:
                continue

            if daily_query_count >= daily_limit:
                break

            try:
                response = client.search(doi)

                if response:
                    json.dump(response, json_file)
                    json_file.write("\n")
                    json_file.flush()

                    new_fetched_dois.add(doi)
                    daily_query_count += 1

            except Exception as e:
                print(f"Error fetching {doi}: {e}")

    # Update fetched DOIs file
    all_fetched_dois = fetched_dois.union(new_fetched_dois)
    pd.DataFrame({"doi": list(all_fetched_dois)}).to_csv(
        path_fetched_dois,
        index = False
    )

    print(f"Fetched {daily_query_count} new records.")

    return daily_query_count


def fetch_missing_dois(missing_dois_csv, output_json_path, api_client, daily_limit=450):
    """
    Fetch metadata only for missing DOIs and append to existing JSON file.
    
    Parameters:
    - missing_dois_csv: CSV file with column 'doi' listing missing DOIs
    - output_json_path: path to append the new JSON metadata
    - api_client: instance of springernature_api_client.meta.MetaAPI
    - daily_limit: max number of API calls per run
    """
    # Load missing DOIs
    df_missing = pd.read_csv(missing_dois_csv)
    doi_list = df_missing["doi"].dropna().astype(str).str.strip().tolist()
    
    print(f"Fetching metadata for {len(doi_list)} missing DOIs...")
    
    os.makedirs(os.path.dirname(output_json_path), exist_ok=True)
    
    count_fetched = 0
    with open(output_json_path, "a") as f_json:
        for doi in doi_list:
            if count_fetched >= daily_limit:
                print(f"Daily limit of {daily_limit} reached. Stopping.")
                break
            try:
                # Use a DOI-specific query
                query = f'doi:"{doi}"'
                response = api_client.search(query)
                
                if response:
                    json.dump(response, f_json)
                    f_json.write("\n")
                    f_json.flush()
                    count_fetched += 1
                    print(f"Fetched {count_fetched}: {doi}")
                else:
                    print(f"No result for DOI: {doi}")
                    
            except Exception as e:
                print(f"Error fetching DOI {doi}: {e}")
    
    print(f"Finished fetching. Total new records: {count_fetched}")
    return count_fetched