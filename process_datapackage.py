from frictionless import Package
import pandas as pd

def main():
    # package = Package("./datasets/SenNet_C2M2_Spring_2025/C2M2_datapackage.json")
    # print(package.name)

    # report = package.validate()
    # if report.valid:
    #     print("Everything looks good!")
    # else:
    #     print(report.flatten(["rowPosition", "fieldPosition", "code"]))


    # # subject = package.get_resource("subject")
    # # df = subject.to_pandas()
    # # print(df.head())

    # # for resource in package.resources:
    # #     print(resource.name)
    # #     print(len(resource.schema.fields))

    # # print(package.metadata)
    # package.description = "This is a new description"
    # package.to_json("./datasets/test_datapackage.json")
    augment_datapackage(
        "./datasets/SenNet_C2M2_Spring_2025/C2M2_datapackage.json",
        "./datasets/test_datapackage.json"
    )
    return

def augment_datapackage(in_path, out_path):
    """
    Augment a datapackage with additional metadata we expect.
    """
    package = Package(in_path)
    report = package.validate()
    if not report.valid:
        print("The package is not valid:")
        print(report.flatten(["rowPosition", "fieldPosition", "code"]))
        raise ValueError("Invalid datapackage. Please fix the errors and try again.")
    
    
    for resource in package.resources:
        # print(resource.name)
        # print('header :', resource.header)
        df = resource.to_pandas()
        # print(df.index)
        df = df.reset_index()
        # df_raw = pd.read_csv('./datasets/SenNet_C2M2_Spring_2025/' + resource.path, delimiter='\t')
        # print('raw    :', df_raw.columns.tolist())
        # print('package:', df.columns.tolist())
        # print('schema :', [f.name for f in resource.schema.fields])

        rows = df.shape[0]
        cols = df.shape[1]
        resource.custom['udi:row_count'] = rows
        resource.custom['udi:column_count'] = cols
        # print(df.head())
        for field in resource.schema.fields:
            if rows == 0:
                cardinality = 0
            else:
                col = field.name
                cardinality = df[col].nunique()
            field.custom['udi:cardinality'] = cardinality
            field.custom['udi:unique'] = cardinality == rows
            field.custom['udi:data_type'] = infer_data_type(field)

    # handle relationships in another pass so we can assume udi fields are populated
    for resource in package.resources:
        # print(type(resource.schema))
        # print(resource.schema.foreign_keys)
        # if 'foreignKeys' not in resource.schema:
        #     continue
        # print(resource.name)
        # print('\t', resource.schema.foreignKeys)
        foreignKeys = resource.schema.foreign_keys
        # if foreignKeys is None:
        #     continue
        # print('has FK')
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

    # package.description = "This is a new description"
    package.to_json(out_path)
    return

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