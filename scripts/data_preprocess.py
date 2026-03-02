import os
import pandas as pd
from pybtex.database import parse_file


def bib_to_csv(bib_paths, csv_output_path):
    '''
    
    '''
    if isinstance(bib_paths, str):
        bib_paths = [bib_paths]

    combined_records = []

    for path in bib_paths:
        bib_data = parse_file(path)

        for key, entry in bib_data.entries.items():
            record = {}

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