import os
import sys
import json
from frictionless import Package
import pandas as pd
import json

def main():
    datasets_path = "./datasets"
    input_catalogue = os.path.join(datasets_path, "input_catalogue.json")
    datapackage_list = []
    with open(input_catalogue, 'r') as f:
        data_packages = json.load(f)
        for data_package in data_packages:
            if not data_package['process']:
                continue
            name = data_package['name']
            print('Processing Data Package:', name)
            out_path = data_package['outName']
            datapackage = augment_datapackage(name, out_path)
            datapackage_list.append(datapackage)

    # Create the top-level schema file with the combined list
    # top_level_catalogue_path = os.path.join(datasets_path, "output_catalogue.json")
    # with open(top_level_catalogue_path, "w") as top_level_schema_file:
    #     json.dump(datapackage_list, top_level_schema_file, indent=4)

    return

def augment_datapackage(in_path, out_path):
    """
    Augment a datapackage with additional metadata we expect.
    """
    folder = in_path.split('/')[-2]
    package = Package(in_path)
    package.custom['udi:name'] = folder
    package.custom['udi:path'] = './data/' + folder + '/'
    # report = package.validate()
    # if not report.valid:
    #     print("The package is not valid:")
    #     print(report.flatten(["rowPosition", "fieldPosition", "code"]))
    #     raise ValueError("Invalid datapackage. Please fix the errors and try again.")
    
    print('...updating metadata')
    for resource in package.resources:
        ephemeral_print(resource.name)
        df = resource.to_pandas()
        df = df.reset_index()

        rows = df.shape[0]
        cols = df.shape[1]
        resource.custom['udi:row_count'] = rows
        resource.custom['udi:column_count'] = cols
        for field in resource.schema.fields:
            if field.type == 'array':
                cardinality = 0
                # pandas.nunique does not work on arrays and 
                # we don't use array types so we can ignore this
            elif rows == 0:
                cardinality = 0
            else:
                col = field.name
                cardinality = df[col].nunique()
            field.custom['udi:cardinality'] = cardinality
            field.custom['udi:unique'] = cardinality == rows
            field.custom['udi:data_type'] = infer_data_type(field)

    print('\n...updating relationships')
    # handle relationships in another pass so we can assume udi fields are populated
    for resource in package.resources:
        ephemeral_print(resource.name)
        foreignKeys = resource.schema.foreign_keys
        for foreignKey in foreignKeys:
            from_unique = unique_multi_key(resource, foreignKey['fields'])
            from_cardinality = 'one' if from_unique else 'many'
            to_resource = package.get_resource(foreignKey['reference']['resource'])
            to_unique = unique_multi_key(to_resource, foreignKey['reference']['fields'])
            to_cardinality = 'one' if to_unique else 'many'

            foreignKey['udi:cardinality'] = {
                "from": from_cardinality,
                "to": to_cardinality,
            }
    print('\n...exporting')
    package.to_json(out_path)
    return json.load(open(out_path, 'r'))

def unique_multi_key(resource, key_fields):
    """
    Check if the combination of fields in the dataframe is unique.
    """
    if len(key_fields) == 0:
        raise ValueError("No fields provided")
    if len(key_fields) == 1:
        key_field = key_fields[0]
        fields = resource.schema.fields
        field = next((f for f in fields if f.name == key_field), None)
        if field is None:
            raise ValueError(f"Field '{key_field}' not found in resource schema")
        return field.custom.get('udi:unique', False)
    else:
        df = resource.to_pandas()
        df = df.reset_index()
        if df.empty:
            # Can't really determine based on empty data so give "safer" answer.
            return False
        return len(df[key_fields].drop_duplicates()) == df.shape[0]


def ephemeral_print(message):
    sys.stdout.write("\r\033[K")  # Clear the line
    sys.stdout.write(f"\t{message}")
    sys.stdout.flush()

def infer_data_type(field):
    """
    Infer the simplified data type (ordinal, nominal, quantitative) of a data package field.
    https://datapackage.org/standard/table-schema/#field
    """
    if field.custom.get("categories"):
        if field.custom.get('categoriesOrdered'):
            return "ordinal"
        return "nominal"
    if field.custom.get('enum') is not None:
        return "nominal"
    type = field.type
    if type is None:
        return "unknown"
    if type == "boolean":
        return "nominal"
    if type == "integer" or type == "number":
        return "quantitative"
    if type == "string":
        # TODO: I could check the cardinality
        return "nominal"
    else:
        return "other"

if __name__ == "__main__":
    main()