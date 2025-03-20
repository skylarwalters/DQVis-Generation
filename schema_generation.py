import os
import pandas as pd
import json

# import pyarrow.csv as pv_csv
# import pyarrow as pa
# from ydata_profiling import ProfileReport


def get_column_schema(df):
    schema = []
    for col in df.columns:
        dtype = df[col].dtype
        column_spec = {"name": col}
        if dtype == "object":
            column_spec["data_type"] = "nominal"
        elif pd.api.types.is_numeric_dtype(dtype):
            column_spec["data_type"] = "quantitative"
        else:
            column_spec["data_type"] = "unknown"
        column_spec["cardinality"] = len(df[col].unique())
        schema.append(column_spec)
    return schema


def process_folder(folder_path):
    folder_schema = []
    folder = folder_path.split("/")[-1]
    if folder.startswith("_"):
        return

    entity_relationships = {}
    possible_entity_file = os.path.join(folder_path, "entity_relationships.json")
    if os.path.exists(possible_entity_file):
        er_file = open(possible_entity_file, "r")
        entity_relationships = json.load(er_file)
        er_file.close()

    er_lookup = {}
    for er in entity_relationships:
        from_name = er["name"]["from"]
        to_name = er["name"]["to"]
        from_cardinality = er["cardinality"]["from"]
        to_cardinality = er["cardinality"]["to"]

        er_lookup.setdefault(from_name, {})
        er_lookup[from_name][to_name] = {
            "id": {"from": er["id"]["from"], "to": er["id"]["to"]},
            "cardinality": {"from": from_cardinality, "to": to_cardinality},
        }

        er_lookup.setdefault(to_name, {})
        er_lookup[to_name][from_name] = {
            "id": {"from": er["id"]["to"], "to": er["id"]["from"]},
            "cardinality": {"from": to_cardinality, "to": from_cardinality},
        }

    # Collect schema for each CSV file in the folder
    for file_name in os.listdir(folder_path):
        if file_name.endswith(".csv"):
            csv_file_path = os.path.join(folder_path, file_name)
            df = pd.read_csv(csv_file_path)
            rows = df.shape[0]
            cols = df.shape[1]
            schema = get_column_schema(df)
            name = file_name[:-4] if file_name.endswith(".csv") else file_name
            url = "./data/" + folder + "/" + name + ".csv"
            new_schema = {
                "name": name,
                "folder": folder,
                "url": url,
                "row_count": rows,
                "column_count": cols,
                "columns": schema,                
            }
            if name in er_lookup:
                new_schema["relationships"] = er_lookup[name]
            folder_schema.append(new_schema)

    # Create a schema file for the folder
    schema_file_path = os.path.join(folder_path, "schema.json")
    with open(schema_file_path, "w") as schema_file:
        json.dump(folder_schema, schema_file, indent=4)

    return {"name": folder, "schema": folder_schema}


def main():
    datasets_path = "./datasets"
    all_schemas = []  # This will hold all file schemas in one list
    for folder_name in os.listdir(datasets_path):
        folder_path = os.path.join(datasets_path, folder_name)
        if os.path.isdir(folder_path):
            folder_schemas = process_folder(folder_path)
            if folder_schemas:
                all_schemas.append(folder_schemas)

    # Create the top-level schema file with the combined list
    top_level_schema_path = os.path.join(datasets_path, "schema.json")
    with open(top_level_schema_path, "w") as top_level_schema_file:
        json.dump(all_schemas, top_level_schema_file, indent=4)


if __name__ == "__main__":
    main()
