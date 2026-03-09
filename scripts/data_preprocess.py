import os
import pandas as pd
import json
from pybtex.database import parse_file


def bib_to_csv(bib_paths, csv_output_path):
    '''
    Converts one or more bib files to a CSV file. Formats author names to Lastname, Firstname; ... 
    '''
    if isinstance(bib_paths, str):
        bib_paths = [bib_paths]

    combined_records = []

    for path in bib_paths:
        bib_data = parse_file(path)

        for key, entry in bib_data.entries.items():
            record = {}

            # Add Item Type
            record["Item Type"] = entry.type 

            for col, val in entry.fields.items():
                record[col] = val

            authors = entry.persons.get("author", [])
            author_list = [f"{p.last_names[0]}, {' '.join(p.first_names)}" for p in authors]
            record["Authors"] = "; ".join(author_list)

            combined_records.append(record)

    df = pd.DataFrame(combined_records)

    os.makedirs(os.path.dirname(csv_output_path), exist_ok = True)

    df.to_csv(csv_output_path, index = False)

    return df


# JSON to CSV
def json_to_csv(input_path, output_path):
    '''
    
    '''
    data = []
    with open(input_path) as f:
        for line in f:
            obj = json.loads(line)

            if obj.get("records"):
                data.append(obj["records"][0])   # <-- change here
        
    df = pd.json_normalize(data)

    df["url"] = df["url"].apply(lambda x: x[0]["value"] if isinstance(x, list) else None)
    
    creators_col = []

    for i in df["creators"]:

        if isinstance(i, list):
            lst = [(k,v)[1] for x in i if isinstance(x, dict) 
                    for (k,v) in x.items() if k == "creator"]
            lst = "; ".join([str(s) for s in lst])

        else:
            lst = str(i)

        creators_col.append(lst)
    
    df["creators"] = creators_col

    os.makedirs(os.path.dirname(output_path), exist_ok = True)
    
    df.to_csv(output_path, index = False)

    return df