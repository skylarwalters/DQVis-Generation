import os
import sys
import json
from frictionless import Package
import pandas as pd

def main():
    datasets_path = "./datasets"
    input_catalogue = os.path.join(datasets_path, "input_catalogue.json")
    reference_df = pd.read_csv(os.path.join(datasets_path, "C2M2_reference.tsv"), delimiter='\t')
    out_path = './out/'
    datapackage_list = []
    with open(input_catalogue, 'r') as f:
        data_packages = json.load(f)
        for data_package in data_packages:
            if not data_package['process']:
                continue
            name = data_package['outName']
            print('Inserting Reference Values into Data Package:', name)
            data_package_out_path = os.path.join(out_path, os.path.dirname(name))
            datapackage = insert_reference_values(name, reference_df, data_package_out_path, not data_package['c2m2'])
            datapackage_list.append(datapackage)


        # Create the top-level schema file with the combined list
    top_level_catalogue_path = os.path.join(datasets_path, "output_catalogue.json")
    with open(top_level_catalogue_path, "w") as top_level_schema_file:
        json.dump(datapackage_list, top_level_schema_file, indent=4)

    return

def insert_reference_values(in_path, ref_df, out_path, pass_through):
    """
    for every resource in the datapackage, add the reference values based on the
    reference_df.
    """
    package = Package(in_path)

    if not os.path.exists(out_path):
        os.makedirs(out_path)

    for resource in package.resources:
        ephemeral_print(resource.name)
        df = resource.to_pandas()
        df = df.reset_index()
        if not pass_through:
            df = df.replace(ref_df['id'].tolist(), ref_df['name'].tolist())

        # print('\n...exporting')
        # export the updated datapackage resource to the out_path
        file_out_path = os.path.join(out_path, resource.name + '.tsv')
        # print('\n', file_out_path)
        df.to_csv(file_out_path, sep='\t', index=False)
        if pass_through:
            continue
        for field in resource.schema.fields:
            if 'enum' in field.custom:
                new_enum = [x for x in field.custom['enum']]
                for i, x in enumerate(new_enum):
                    if x in ref_df['id'].tolist():
                        new_enum[i] = ref_df['name'][ref_df['id'].tolist().index(x)]
                field.custom['enum'] = new_enum
    print('\n...exporting')
    file_out_path = os.path.join(out_path, os.path.basename(in_path))
    package.to_json(file_out_path)
    return json.load(open(file_out_path, 'r'))

def ephemeral_print(message):
    sys.stdout.write("\r\033[K")  # Clear the line
    sys.stdout.write(f"\t{message}")
    sys.stdout.flush()

if __name__ == "__main__":
    main()