import pandas as pd
import re
from constraint import *
from parsimonious.grammar import Grammar
from pprint import pprint


def expand(df, dataset_schemas):
    expanded_rows = []
    for _, row in df.iterrows():
        for schema in dataset_schemas:
            expanded_rows.extend(expand_template(row, schema))
    expanded_df = pd.DataFrame(expanded_rows)
    return expanded_df


def expand_template(row, schema):
    # iterate over all possible field matches that satisfy the constraints.
    # For each match, generate a new row with the template expanded.

    # For each possible constraint satifaction, expand the template
    expanded_rows = []
    return


def get_satisfied_constraints(entities, fields, constraints, schema):
    """
    Returns a list of possible satisfied constraints based on the provided entities, fields, constraints, and schema.
    Args:
        entities (list): A list of entities to be considered.
        fields (list): A list of fields to be considered.
        constraints (list): A list of constraints to be satisfied.
        schema (dict): A dictionary representing the schema of the data.
    Returns:
        list: A list of dictionaries representing the satisfied constraints.
              Each dictionary contains keys 'E' for entity and 'F' for field.
    Example:
        entities = ["E"]
        fields = ["F1", "F2"]
        constraints = [
            {"field": "F1", "type": ["nominal", "ordinal"]},
            {"field": "F2", "type": ["nominal", "ordinal"]},
            {"F1.C > F2.C"}
        ]
        schema = {
            "Country": ["Population", "GDP", "Area"],
            "City": ["Population", "Area"]
        }
        result = get_satisfied_constraints(entities, fields, constraints, schema)
        # result might be:
        # [{"E": "Country", "F": "Population"}, {"E": "Country", "F": "GDP"}]
    """

    # returns a list of possible satisfied constraints
    # e.g [{E: "Country", F: "Population"}, {E: Country, F: "GDP"}, ...]
    return


def is_constraint_satisfied(entities, fields, constraint, solution):
    return


def test_constraint_solver():
    problem = Problem()
    options = [
        {"data_type": "nominal", "name": "A", "cardinality": 3},
        {"data_type": "nominal", "name": "B", "cardinality": 18},
        {"data_type": "quantitative", "name": "C", "cardinality": 5},
        {"data_type": "quantitative", "name": "D", "cardinality": 23},
    ]
    problem.addVariables(["F1", "F2"], options)
    # problem.addVariable("F2", options)
    problem.addConstraint("F1['type'] == 'nominal'")
    problem.addConstraint("F1['cardinality'] > F2['cardinality']")
    problem.addConstraint("F2['cardinality'] > 4")
    s = problem.getSolutions()
    names = [[[k, v["name"]] for k, v in x.items()] for x in s]
    # result: [[['F1', 'B'], ['F2', 'C']]]
    print(names)
    return


def test_custom_parser():
    text = "This is just to test <E> and <E1> and <E2> and <F:O> and <E.F:O> and <E1.F1:N> and <E2.F2:O|N> and more text"
    """should extract
    [
        {"entity": "E", "field": None, "field_type": None},
        {"entity": "E1", "field": None, "field_type": None},
        {"entity": "E2", "field": None, "field_type": None},
        {"entity": None, "field": "F", "field_type": "O"},
        {"entity": "E1", "field": "F1", "field_type": "N"},
        {"entity": "E2", "field": "F2", "field_type": "(O|N)"}
    ]
    """
    pattern = r"<([^>]+)>"
    matches = re.findall(pattern, text)

    result = []
    for match in matches:
        parts = match.split(".")
        entity, field, field_type = None, None, None
        if len(parts) == 1:
            first = parts[0]
            if first.startswith("E") or first.startswith("e"):
                entity = first
            else:
                field = first
        elif len(parts) == 2:
            entity, field = parts
        else:
            raise ValueError(
                f"Invalid match: {match}. There should only be a single '.'"
            )

        if field:
            field_parts = field.split(":")
            if len(field_parts) == 2:
                field, field_type = field_parts
                field_type = field_type.split("|")
            else:
                raise ValueError(
                    f"Invalid match: {match}. Field type must be specified"
                )

        result.append(
            {
                "entity": entity,
                "field": field,
                "allowed_fields": field_type,
                "original": match,
            }
        )
    pprint(result)
    return


def test_grammar_parser():
    grammar = Grammar("""
    tag = tag_start entity.field:field_type tag_end
    tag_start = '<'
    tag_end = '>'
    entity = E
    field = ~r'[A-Za-z0-9]+'
    field_type = ~r'[A-Za-z0-9]+'
""")
    print(grammar.parse("How many <E> are there, grouped by <E.F:N>?"))
    return


if __name__ == "__main__":
    test_custom_parser()
    # test_grammar_parser()

    # # Example usage
    # data = {
    #     'Entity': ['Country', 'City'],
    #     'Field': ['Population', 'Area']
    # }
    # df = pd.DataFrame(data)

    # dataset_schemas = [
    #     {
    #         "Country": ["Population", "GDP", "Area"],
    #         "City": ["Population", "Area"]
    #     }
    # ]

    # expanded_df = expand(df, dataset_schemas)
    # print(expanded_df)
