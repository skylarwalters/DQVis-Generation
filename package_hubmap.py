import os
import argparse
import frictionless
from frictionless import fields, Schema
import pandas as pd

'''
Takes as input the three main HubMAP metadata files (datasets, donors, and samples) and
packages them into a frictionless data package.
'''

def load_metadata(file_path):
    """
    Load metadata from a CSV file into a pandas DataFrame.
    """
    try:
        return pd.read_csv(file_path)
    except Exception as e:
        raise ValueError(f"Error loading file {file_path}: {e}")

def get_df_schema(df, primary_keys, foreign_keys, descriptions):
    fields = get_field_definitions(df, descriptions)
    return Schema(fields=fields, primary_key=primary_keys, foreign_keys=foreign_keys)

def get_field_definitions(df, descriptions):
    field_definitions = []

    [fields.StringField(name='id')]

    for col in df.columns:
        dtype = df[col].dtype
        # column_spec = {"name": col}
        description = descriptions[col]
        # if is nan
        if pd.isna(description):
            description = ""
        if dtype == "object":
            # column_spec["data_type"] = "string"
            field = fields.StringField(name=col, description=description)
        elif pd.api.types.is_numeric_dtype(dtype):
            # column_spec["data_type"] = "number"
            field = fields.NumberField(name=col, description=description)
        else:
            # column_spec["data_type"] = "unknown"
            field = fields.AnyField(name=col, description=description)
        # column_spec["cardinality"] = len(df[col].unique())
        field_definitions.append(field)
    return field_definitions

def create_frictionless_package(input_folder, output_path):
    """
    Create a frictionless data package from HubMAP metadata files.
    """
    timestamped_data = {
        "datasets": "hubmap-datasets-metadata-2025-05-06_04-30-42.tsv",
        "donors": "hubmap-donors-metadata-2025-05-06_04-29-48.tsv",
        "samples": "hubmap-samples-metadata-2025-05-06_04-29-50.tsv"
        }
    description_lookup = {}
    
    # for each timestamped file, open it as a df, save the first two rows as a dict
    # and then save the rest of the file as a csv.
    for key, value in timestamped_data.items():
        # open the file
        file_path = os.path.join(input_folder, value)
        df = pd.read_csv(file_path, sep="\t")
        # save the first two rows as a dict
        descriptions = df.iloc[0:2].to_dict(orient="records")
        description_lookup[key] = descriptions[0]
        # print(metadata_dict)
        # save the rest of the file as a csv
        df.iloc[1:].to_csv(os.path.join(input_folder, f"{key}.csv"), index=False)

    dataset_info = [
        {
            "name": "datasets.csv",
            "primary_keys": ["hubmap_id"],
            "foreign_keys": [
                {
                    "fields": ["donor.hubmap_id"],
                    "reference": {
                        "resource": "donors",
                        "fields": ["hubmap_id"]
                    },
                }
            ],
        },
        {
            "name": "donors.csv",
            "primary_keys": ["hubmap_id"],
            "foreign_keys": [
                {
                    "fields": ["hubmap_id"],
                    "reference": {
                        "resource": "datasets",
                        "fields": ["donor.hubmap_id"]
                    },
                },
                {
                    "fields": ["hubmap_id"],
                    "reference": {
                        "resource": "datasets",
                        "fields": ["donor.hubmap_id"]
                    },
                },
            ],
        },
        {
            "name": "samples.csv",
            "primary_keys": ["hubmap_id"],
            "foreign_keys": [
                {
                    "fields": ["donor.hubmap_id"],
                    "reference": {
                        "resource": "donors",
                        "fields": ["hubmap_id"]
                    },
                }
            ],
        },
    ]
    # dataset_paths = [os.path.join(input_folder, name) for name in dataset_names]
    resources = []
    for dataset in dataset_info:
        name = dataset["name"]
        primary_keys = dataset["primary_keys"]
        foreign_keys = dataset["foreign_keys"]
        dataset_path = os.path.join(input_folder, name)
        df = load_metadata(dataset_path)
        key = os.path.splitext(name)[0]
        # get schema from the dataframe
        schema = get_df_schema(df, primary_keys, foreign_keys, description_lookup[key])
        resource = frictionless.Resource(path=name, schema=schema)
        resources.append(resource)
    # resources = [frictionless.Resource(path) for path in dataset_paths]

    # Create the package
    package = frictionless.Package(resources=resources, name="hubmap_metadata")

    # Save the package to the specified output path
    package.to_json(output_path)
    print(f"Frictionless package created at {output_path}")

if __name__ == "__main__":

    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="Package HubMAP metadata into a frictionless data package.")
    parser.add_argument("input_folder", help="Path to the hubmap folder")
    parser.add_argument("output_path", help="Path to save the frictionless data package (ZIP)")

    args = parser.parse_args()

    # Create the frictionless package
    create_frictionless_package(args.input_folder, args.output_path)