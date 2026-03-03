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

