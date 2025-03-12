"""
For each folder in ./datasets there will be some number of csv files.

For each fodler a schema file will be created that contains
the schema of the csv files in the folder. Then a top-level schema file
will be created that contains a list of all schemas.
"""

import os
import pandas as pd
import json


def generate_schema(csv_file):
    df = pd.read_csv(csv_file)
    schema = {
        "columns": [{"name": col, "dtype": str(df[col].dtype)} for col in df.columns]
    }
    return schema


def process_folder(folder_path):
    schema_list = []
    for file_name in os.listdir(folder_path):
        if file_name.endswith(".csv"):
            csv_file_path = os.path.join(folder_path, file_name)
            schema = generate_schema(csv_file_path)
            schema_list.append(schema)
            schema_file_path = os.path.join(folder_path, f"{file_name}_schema.json")
            with open(schema_file_path, "w") as schema_file:
                json.dump(schema, schema_file, indent=4)
    return schema_list


def main():
    datasets_path = "./datasets"
    all_schemas = []
    for folder_name in os.listdir(datasets_path):
        folder_path = os.path.join(datasets_path, folder_name)
        if os.path.isdir(folder_path):
            folder_schemas = process_folder(folder_path)
            all_schemas.extend(folder_schemas)

    top_level_schema_path = os.path.join(datasets_path, "schema.json")
    with open(top_level_schema_path, "w") as top_level_schema_file:
        json.dump(all_schemas, top_level_schema_file, indent=4)


if __name__ == "__main__":
    main()
