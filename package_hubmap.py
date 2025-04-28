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

def get_df_schema(df, primary_keys, foreign_keys):
    fields = get_field_definitions(df)
    return Schema(fields=fields, primary_key=primary_keys, foreign_keys=foreign_keys)

def get_field_definitions(df):
    field_definitions = []

    [fields.StringField(name='id')]

    for col in df.columns:
        dtype = df[col].dtype
        # column_spec = {"name": col}
        description = ''
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
        # get schema from the dataframe
        schema = get_df_schema(df, primary_keys, foreign_keys)
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