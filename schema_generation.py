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
    folder_schema = {"files": []}

    # Collect schema for each CSV file in the folder
    for file_name in os.listdir(folder_path):
        if file_name.endswith(".csv"):
            csv_file_path = os.path.join(folder_path, file_name)
            schema = generate_schema(csv_file_path)
            folder_schema["files"].append({"file_name": file_name, "schema": schema})

    # Create a schema file for the folder
    schema_file_path = os.path.join(folder_path, "schema.json")
    with open(schema_file_path, "w") as schema_file:
        json.dump(folder_schema, schema_file, indent=4)

    return folder_schema["files"]  # Return only the list of files


def main():
    datasets_path = "./datasets"
    all_schemas = []  # This will hold all file schemas in one list
    for folder_name in os.listdir(datasets_path):
        folder_path = os.path.join(datasets_path, folder_name)
        if os.path.isdir(folder_path):
            folder_schemas = process_folder(folder_path)
            all_schemas.extend(
                folder_schemas
            )  # Merge the folder schemas into the main list

    # Create the top-level schema file with the combined list
    top_level_schema_path = os.path.join(datasets_path, "schema.json")
    with open(top_level_schema_path, "w") as top_level_schema_file:
        json.dump(all_schemas, top_level_schema_file, indent=4)


if __name__ == "__main__":
    main()
