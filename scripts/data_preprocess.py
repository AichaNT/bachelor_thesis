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
def json_to_csv(input_path, output_path, valid_dois):
    '''
    
    '''
    data = []

    if not isinstance(valid_dois, set):
        valid_dois = set([str(d).lower().strip() for d in valid_dois])


    with open(input_path) as f:
        for line in f:
            obj = json.loads(line)

            for rec in obj.get("records", []):
                doi = rec.get("doi", "").lower().strip()
                if doi in valid_dois:
                    data.append(rec)
                
    df = pd.json_normalize(data)

    df["url"] = df.get("url").apply(lambda x: x[0]["value"] if isinstance(x, list) and len(x) > 0 else None)

    creators_col = []
    for creators in df.get("creators", []):
        if isinstance(creators, list):
            lst = [c.get("creator") for c in creators if isinstance(c, dict) and "creator" in c]
            creators_col.append("; ".join(lst))
        else:
            creators_col.append(str(creators))
    df["creators"] = creators_col

    os.makedirs(os.path.dirname(output_path), exist_ok = True)
    
    df.to_csv(output_path, index = False)

    return df