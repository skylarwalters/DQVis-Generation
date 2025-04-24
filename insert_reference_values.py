import os
import sys
import json
from frictionless import Package
import pandas as pd

def main():
    datasets_path = "./datasets"
    input_catalogue = os.path.join(datasets_path, "input_C2M2_catalogue.txt")
    reference_df = pd.read_csv(os.path.join(datasets_path, "C2M2_reference.tsv"), delimiter='\t')
    out_path = './out/C2M2_updated/'
    with open(input_catalogue, 'r') as f:
        lines = f.readlines()
        lines = [line.strip() for line in lines if line.strip()]
        # remove commented out lines from the input_catalogue
        lines = [line for line in lines if not line.startswith('#')]
        lines = [line for line in lines if not line.startswith('//')]
        for line in lines:
            print('Processing Data Package:', line)
            data_package_out_path = os.path.join(out_path, os.path.dirname(line))
            insert_reference_values(line, reference_df, data_package_out_path)
    return

def insert_reference_values(in_path, ref_df, out_path):
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

        df = df.replace(ref_df['id'].tolist(), ref_df['name'].tolist())

        print('\n...exporting')
        # export the updated datapackage resource to the out_path
        file_out_path = os.path.join(out_path, resource.name + '.tsv')
        print('\n', file_out_path)
        df.to_csv(file_out_path, sep='\t', index=False)
        for field in resource.schema.fields:
            if 'enum' in field.custom:
                new_enum = [x for x in field.custom['enum']]
                for i, x in enumerate(new_enum):
                    if x in ref_df['id'].tolist():
                        new_enum[i] = ref_df['name'][ref_df['id'].tolist().index(x)]
                field.custom['enum'] = new_enum
    file_out_path = os.path.join(out_path, os.path.basename(in_path))
    package.to_json(file_out_path)
    return

def ephemeral_print(message):
    sys.stdout.write("\r\033[K")  # Clear the line
    sys.stdout.write(f"\t{message}")
    sys.stdout.flush()

if __name__ == "__main__":
    main()