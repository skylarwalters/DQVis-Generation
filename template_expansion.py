import pandas as pd


def expand(df, dataset_schemas):
    expanded_rows = []
    for _, row in df.iterrows():
        for schema in dataset_schemas:
            new_row = row.copy()
            new_row.update(schema)
            expanded_rows.append(new_row)
    expanded_df = pd.DataFrame(expanded_rows)
    return expanded_df


def expand_template(row, schema):
    # iterate over all possible field matches that satisfy the constraints.
    # For each match, generate a new row with the template expanded.

    # For each possible constraint satifaction, expand the template
    expanded_rows = []
    return


def get_satisfied_constraints(entities, fields, constraints, schema):
    # returns a list of possible satisfied constraints
    # e.g [{E: "Country", F: "Population"}, {E: Country, F: "GDP"}, ...]
    return
