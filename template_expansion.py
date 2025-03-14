from typing import List, Dict, Union
import pandas as pd
import re
from constraint import *
from parsimonious.grammar import Grammar
from pprint import pprint


def expand(df, dataset_schemas):
    expanded_rows = []
    # TODO: optimize, flip the order of these for loops?
    for _, row in df.iterrows():
        for schema in dataset_schemas:
            schema_name = schema["name"]
            schema_def = schema["schema"]
            # flatten schema_def
            schema_flattened = []
            for file in schema_def:
                entity = file["name"]
                row_count = file["row_count"]
                column_count = file["column_count"]
                url = file["url"]
                for col in file["columns"]:
                    expanded_col = col.copy()
                    expanded_col.update(
                        {
                            "entity": entity,
                            "row_count": row_count,
                            "column_count": column_count,
                            "url": url,
                        }
                    )
                    schema_flattened.append(expanded_col)
            field_options = schema_flattened
            # unique_entities = set([x["entity"] for x in schema_flattened])
            url_lookup = {x["entity"]: x["url"] for x in schema_flattened}
            unique_entities = url_lookup.keys()
            empty_entity_options = {
                "name": None,
                "data_type": None,
                "cardinality": None,
            }

            entity_options = [
                {"entity": entity, "url": url_lookup[entity]}
                for entity in unique_entities
            ]
            for e in entity_options:
                e.update(empty_entity_options)
            new_rows = expand_template(row, entity_options, field_options)
            for new_row in new_rows:
                new_row["dataset_schema"] = schema_name
            expanded_rows.extend(new_rows)
    expanded_df = pd.DataFrame(expanded_rows)
    return expanded_df


def expand_template(row, entity_options, field_options):
    extract = extract_tags(row["query_template"])
    tags = extract["tags"]
    entities = extract["entities"]
    fields = extract["fields"]
    constraints = expand_constraints(row["constraints"], tags)
    s = constraint_solver(entities, fields, constraints, entity_options, field_options)

    return expand_solutions(row, tags, s)


def expand_solutions(row, tags, solutions):
    result = []
    for s in solutions:
        expanded_row = row.copy()
        expanded_row["query_base"] = resolve_query_template(
            row["query_template"], tags, s
        )
        expanded_row["spec"] = resolve_spec_template(row["spec_template"], tags, s)
        result.append(expanded_row)
    # pprint(result)
    return result


def resolve_query_template(query_template, tags, solution):
    query_base = query_template
    for tag in tags:
        if tag["field"]:
            k = tag["entity"] + "_" + tag["field"]
            resolved = solution[k]["name"]
        else:
            resolved = solution[tag["entity"]]["entity"]
        query_base = query_base.replace(f"<{tag['original']}>", resolved, 1)
    return query_base


def resolve_spec_template(spec_template, tags, solution):
    spec = spec_template
    pattern = r"<([^>]+)>"
    while True:
        match = re.search(pattern, spec)
        if match == None:
            break
        match = match.group(0)
        content = match.strip("<>")
        parts = content.split(".")
        if len(parts) == 1:
            if parts[0].startswith("E"):
                entity = parts[0]
                resolved = solution[entity]["entity"]
            else:
                resolved = solution["E_" + parts[0]]["name"]
        elif len(parts) == 2:
            left, right = parts
            if right == "url":
                resolved = solution[left]["url"]
            else:
                resolved = solution[left + "_" + right]["name"]
        else:
            raise ValueError(
                f"Invalid match: {match}. Only a single '.' is supported in spec template"
            )

        # resolved = "RESOLVED"
        spec = spec.replace(match, resolved, 1)
    return spec


def extract_tags(text: str) -> List[Dict[str, Union[str, List[str]]]]:
    """
    Example input:
     "This is just to test <E> and <E1> and <E2> and <F:O> and <E.F:O> and <E1.F1:N> and <E2.F2:O|N> and more text"
    Example output:
        [
            {"original": "E", "entity": "E", "field": None, "field_type": None},
            {"original": "E1", "entity": "E1", "field": None, "field_type": None},
            {"original": "E2", "entity": "E2", "field": None, "field_type": None},
            {"original": "F:O", "entity": None, "field": "F", "field_type": ["O"]},
            {"original": "E1.F1:N", "entity": "E1", "field": "F1", "field_type": ["N"]},
            {"original": "E2.F2:O|N", "entity": "E2", "field": "F2", "field_type": ["O", "N"]}
        ]
    """
    pattern = r"<([^>]+)>"
    matches = re.findall(pattern, text)

    tags = []
    for match in matches:
        parts = match.split(".")
        entity, field, field_type = None, None, None
        if len(parts) == 1:
            first = parts[0]
            if first.startswith("E"):
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
                field_type = [
                    {"N": "nominal", "O": "ordinal", "Q": "quantitative"}[t]
                    for t in field_type.split("|")
                ]
            else:
                raise ValueError(
                    f"Invalid match: {match}. Field type must be specified"
                )

        tags.append(
            {
                "entity": entity,
                "field": field,
                "allowed_fields": field_type,
                "original": match,
            }
        )
    infer_entity(tags)
    entities = set([tag["entity"] for tag in tags])
    # fields = set([tag["field"] for tag in tags if tag["field"]])
    fields = set(
        [str(tag["entity"]) + "_" + tag["field"] for tag in tags if tag["field"]]
    )
    return {"tags": tags, "entities": list(entities), "fields": list(fields)}


def infer_entity(
    tags: List[Dict[str, Union[str, List[str]]]],
) -> List[Dict[str, Union[str, List[str]]]]:
    """
    Infer the based on the other entities. If none is provided, default to E.
    If there is an empty entity and multiple other entities defined, thwrow an error.
    """
    defined_entities = [tag["entity"] for tag in tags if tag["entity"]]
    unique_entities = set(defined_entities)

    if len(unique_entities) > 1 and any(not tag["entity"] for tag in tags):
        raise ValueError("Multiple entities defined, cannot infer empty entity.")
    for tag in [x for x in tags if not x["entity"]]:
        tag["entity"] = "E"
    return tags


def expand_constraints(
    contstraints: List[str], tags: List[Dict[str, Union[str, List[str]]]]
) -> List[str]:  # type: ignore
    """
    the current constraints will be expanded a bit
        e.g. F.C > 4 → F["cardinality"] > 4
    the tags will add constraints for each field type
    and will add a constraint to ensure unique fields
    """
    expanded_constraints = []
    # TODO expand input constraints

    # Turn field types into constraints
    expanded_constraints.extend(
        [
            f"{tag['entity']}_{tag['field']}['data_type'] in {tag['allowed_fields']}"
            for tag in tags
            if tag["field"]
        ]
    )

    # Ensure fields are not repeated
    unique_fields = set(
        [str(tag["entity"]) + "_" + tag["field"] for tag in tags if tag["field"]]
    )
    # if len(unique_fields) > 1:
    #     for field in unique_fields:
    #         other_fields = unique_fields - {field}
    #         expanded_constraints.append(f"{field} not in {other_fields}")

    if len(unique_fields) > 1:
        for field in unique_fields:
            other_fields = unique_fields - {field}
            name_str = "['name']"
            other_fields_string = (
                "[" + ",".join([str(x) + name_str for x in other_fields]) + "]"
            )
            expanded_constraints.append(
                f"{field + name_str} not in {other_fields_string}"
            )

    # ensure that entities are not repeated
    unique_entities = set([tag["entity"] for tag in tags])
    if len(unique_entities) > 1:
        for entity in unique_entities:
            other_entities = unique_entities - {entity}
            e_str = "['entity']"
            other_entities_string = (
                "[" + ",".join([str(x) + e_str for x in other_entities]) + "]"
            )
            expanded_constraints.append(
                f"{entity + e_str} not in {other_entities_string}"
            )

    # ensure that fields belong to their entity
    for field in unique_fields:
        entity = field.split("_")[0]
        expanded_constraints.append(f"{field}['entity'] == {entity}['entity']")

    # TODO: do I need this one now?
    # ensure that fields within the same entity are preserved
    # unique_entities = set([tag["entity"] for tag in tags if tag["entity"]])
    # # for entity in unique_entities:
    #     entity_fields = [tag["field"] for tag in tags if tag["entity"] == entity]
    #     if len(entity_fields) > 1:
    #         expanded_constraints.append("==".join(f"{entity_fields}['entity']"))

    return expanded_constraints


def constraint_solver(
    entities: List[str],
    fields: List[str],
    constraints: List[str],
    entity_options: List[Dict[str, Union[str, int]]],
    field_options: List[Dict[str, Union[str, int]]],
) -> List[Dict[str, str]]:
    problem = Problem()
    # print("⭐ constraints ⭐")
    # pprint(constraints)
    # print("⭐ entities ⭐")
    # pprint(entities)
    # pprint(entity_options)
    # print("⭐ fields ⭐")
    # pprint(fields)
    # pprint(field_options)
    problem.addVariables(fields, field_options)
    problem.addVariables(entities, entity_options)
    for constraint in constraints:
        problem.addConstraint(constraint)
    s = problem.getSolutions()
    # pprint("⭐ solutions ⭐")
    # pprint(s)
    return s


def test_constraint_solver():
    problem = Problem()
    fields = [
        {"entity": "donors", "data_type": "nominal", "name": "A", "cardinality": 3},
        {"entity": "donors", "data_type": "nominal", "name": "B", "cardinality": 18},
        {
            "entity": "samples",
            "data_type": "quantitative",
            "name": "C",
            "cardinality": 5,
        },
        {
            "entity": "samples",
            "data_type": "quantitative",
            "name": "D",
            "cardinality": 23,
        },
    ]
    entities = [
        {"entity": "donors", "data_type": None, "name": None, "cardinality": 200},
        {"entity": "samples", "data_type": None, "name": None, "cardinality": 200},
    ]
    problem.addVariables(["F1"], fields)
    problem.addVariables(["E1", "E2"], entities)
    # problem.addConstraint("F1['data_type'] in {'nominal'}")
    # problem.addConstraint("F1['entity'] == E1['entity']")
    problem.addConstraint("E1['entity'] != E2['entity']")
    # problem.addConstraint("F1['cardinality'] > F2['cardinality']")
    # problem.addConstraint("F2['cardinality'] > 4")
    # problem.addConstraint("F1['entity'] == F2['entity']== 'samples'")
    s = problem.getSolutions()
    names = [[{k: v["name"], "ent": v["entity"]} for k, v in x.items()] for x in s]
    # result: [[['F1', 'B'], ['F2', 'C']]]
    pprint(names)
    return


if __name__ == "__main__":
    # test_custom_parser()
    # test_grammar_parser()
    # test_constraint_solver()
    # def expand_template(row, entity_options, field_options):
    expand_template(
        row={
            "constraints": [],
            "query_template": "How many <E> are there <F:Q>?",
            "spec_template": "{ source: '<E>', '<E.url> rep: <F>'}",
        },
        entity_options=[
            {
                "url": "./data/fake_sample.csv",
                "entity": "fake_sample",
                "name": None,
                "data_type": None,
                "cardinality": None,
            },
            {
                "url": "./data/fake_donor.csv",
                "entity": "fake_donor",
                "name": None,
                "data_type": None,
                "cardinality": None,
            },
            {
                "url": "./data/fake_file.csv",
                "entity": "fake_file",
                "name": None,
                "data_type": None,
                "cardinality": None,
            },
        ],
        field_options=[
            {
                "entity": "fake_sample",
                "name": "organ",
                "data_type": "nominal",
                "cardinality": 6,
            },
            {
                "entity": "fake_donor",
                "name": "weight",
                "data_type": "quantitative",
                "cardinality": 27,
            },
            {
                "entity": "fake_donor",
                "name": "height",
                "data_type": "quantitative",
                "cardinality": 27,
            },
        ],
    )

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
