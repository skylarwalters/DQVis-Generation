from typing import List, Dict, Union
import pandas as pd
import re
from constraint import *
import json
import ast

# from parsimonious.grammar import Grammar
from pprint import pprint

def expand(df, dataset_schemas):
    expanded_rows=[]
    for _, row in df.iterrows():
        for schema in dataset_schemas:
            # 1. extract sample metadata
            sample_name=schema['name']
            sample_id = schema['udi:sample-id']
            sample_assembly=schema["udi:assembly"]
            sample_cancer = schema["udi:cancer-type"]
            
            # 2. extract gene data
            schema_genes = schema["udi:genes"]
            gene_list = []
            for elem in schema_genes:
                gene_list.append({'name':elem["name"],'chr':elem["chr"], 'pos':elem["pos"]})
                

            # 3. extract data about each file + actual file
            schema_flattened = []
            entity_info=[]
            for file in schema["resources"]:
                file_name = file["name"] 
                sample_id = sample_name
                file_schema = file["schema"]
                foreignKeys = file_schema.get("foreignKeys", [])
                file_path = file["path"]
                
                entity_info.append({
                    'name':file_name,
                    'format':file["format"],
                    'position-fields':file["position-fields"],
                    'sample':sample_id,
                    'url': file_path,
                    'fields': file_schema["fields"]
                })

                for col in file_schema["fields"]:
                    schema_flattened.append({
                        "file": file_name,
                        "field": col["name"],  
                        "udi:data_type": col.get("type"),
                        "url": file_path,
                        "foreignKeys": foreignKeys,
                        "column_metadata": col,
                        "sample": sample_id,
                    })

            # 4. Create sample and entity options
            sample_options = create_sample_options(schema_flattened, sample_assembly, sample_cancer)
            entity_options=entity_info
            
            # 5. Create location options
            gene_positions=[]
            gene_chr=[]
            gene_names=[]
            for gene_info in gene_list:
                gene_positions.append(gene_info['pos'])
                gene_chr.append(gene_info['chr'])
                gene_names.append(gene_info['name'])
                
            location_options = [
                {'gene': gene_name, 'position': position, 'chromosome': chromo}
                for gene_name, position, chromo in zip(gene_names, gene_positions, gene_chr)
            ]
            
            # 6. Create field options
            field_options=schema_flattened
                                    
            new_rows = expand_template(row, sample_options, entity_options, field_options, location_options)
            for new_row in new_rows:
                new_row["dataset_schema"] = sample_name
            expanded_rows.extend(new_rows)
            
    expanded_df = pd.DataFrame(expanded_rows)
    return expanded_df

from typing import List, Dict


def create_sample_options(flattened_resources, sample_assembly, sample_cancer) -> List[Dict]:
    """
    Create sample options from the flattened resources (and maybe assembly
    and cancer ?)
    """
    
    sample_options = []
    # collect all unique samples
    unique_samples = set(x["sample"] for x in flattened_resources)

    # iterate thru unique samples
    for sample in unique_samples:
        sample_rows = [x for x in flattened_resources if x["sample"] == sample]
        
        # list files
        files = {}
        for row in sample_rows:
            file_name = row["file"]
            if file_name not in files:
                files[file_name] = {
                    "file": file_name,
                    "url": row["url"],
                    "fields": [],
                }
            # add data for file
            
            files[file_name]["fields"].append({
                "field": row["field"],
                "udi:data_type": row.get("udi:data_type", None),
                "column_metadata": row.get("column_metadata", {}),
            })
        
        # put together sample options
        sample_options.append({
            "sample": sample,
            "files": list(files.values()),
            "udi:assembly": sample_assembly,
            "udi:cancer-type": sample_cancer,
        })

    return sample_options

def expand_template(row, sample_options, entity_options, field_options, location_options):
    extract = extract_tags(row["query_template"])
    tags = extract["tags"]
    
    samples = extract["samples"]
    entities = extract["entities"]
    locations = extract["location"]
    fields = extract["fields"]
        
    constraints = expand_constraints(row["constraints"], tags) # added to convert to a list
    # print("⭐ expanded constraints ⭐")

    s = constraint_solver(samples, entities, fields, locations, constraints, sample_options, entity_options, field_options, location_options)

    return expand_solutions(row, tags, s)


def expand_solutions(row, tags, solutions):
    result = []
    for s in solutions:
        expanded_row = row.copy()
        expanded_row["query_base"] = resolve_query_template(
            row["query_template"], tags, s
        )
        expanded_row["spec"] = resolve_spec_template(row["spec_template"], tags, s)
        expanded_row["solution"] = cleanup_solution(s)
        result.append(expanded_row)
    return result

def cleanup_solution(solution):
    cleaned = {}
    for k in solution:
        newK = k.replace('_', '.')
        cleaned[newK] = solution[k]
        if 'F' in newK and 'foreignKeys' in cleaned[newK]:
            cleaned[newK].pop('foreignKeys')
    return cleaned

def resolve_query_template(query_template, tags, solution):
    query_base = query_template
    for tag in tags:
        if tag["field"]:
            k = tag["sample"] + "_" + tag["entity"] + "_" + tag["field"] 
            resolved = solution[k]["field"]
        elif tag["location"]:
            k = tag["sample"] + "_" + tag["location"] 
            resolved = solution[k]["gene"] # integrate chromosome here too?
        elif tag["entity"]:
            k = tag["sample"] + "_" + tag["entity"]
            resolved = solution[k]["name"]
        else:
            resolved = solution[tag["sample"]]["sample"] #redefine entity as sample
        query_base = query_base.replace(f"<{tag['original']}>", resolved, 1)
    return query_base  

def expand_field(field, tags):
    for tag in tags:
        if tag['field'] == field:
            return tag['sample'] + '_' + tag['entity'] + '_' + tag['field']
    
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
        
        print(f'match: {match}')
        print(f'content: {content}')
        print(f'parts: {parts}')

        if len(parts) == 1:
            content = parts[0]
            if content.startswith("S"):
                sample = content
                resolved = solution[sample]["sample"]
            elif content.startswith("E") or content.startswith('L'):
                resolved = solution["S_" + parts[0]]["sample"]
            else:
                resolved = solution[expand_field(content, tags)]["sample"]
                
        elif len(parts) == 2:
            left, right = parts
            
            if left.startswith("S"):
                left = content
            elif left.startswith("E") or content.startswith('L'):
                left = "S_" + left
            else:
                left = expand_field(left, tags)
            
            
            if right == 'url':
                resolved = solution[left]['url']
            elif right == "field":
                resolved = solution[left]["field"]
            elif right == 'format':
                resolved = solution[left]["format"]
            elif right == 'chr1':
                resolved = solution[left]['position-fields'][0]['chromosome-field']
            elif right == 'chr2':
                resolved = solution[left]['position-fields'][1]['chromosome-field']
            elif right == 'genomicfields1':
                resolved = str(solution[left]['position-fields'][0]['genomic-fields'])
            elif right == 'genomicfields2':
                resolved = str(solution[left]['position-fields'][1]['genomic-fields'])
            else:
                resolved = solution[left + "_" + right]["name"]
                
                
        elif len(parts) == 5:
            S1, r, S2, id, source = parts
            if S1[0] != "S" or S2[0] != "S" or r != "r" or id != "id" or source not in ["from", "to"]:
                raise ValueError(
                    f"Invalid match: {match}. Unexpected formatting of spec template tag."
                )
            S2_name = solution[S2]["source"]
            # resolved = solution[E1]["foreignKeys"][E2_name]["id"][source]
            # What needs to happen here, is it should loop over foreign keys to find if reference.resource matches E2_name
            # and then if source is 'from' return fields else return reference.fields
            foreignKeys = solution[S1]["foreignKeys"]
            matchedKey = next(
                (fk for fk in foreignKeys if fk["reference"]["resource"] == S2_name),
                None,
            )
            if matchedKey is None:
                raise ValueError(
                    f"Invalid match: {match}. Could not find foreign key for {S1} to {S2}"
                )
            if source == "from":
                resolved = matchedKey["fields"]
            else:
                resolved = matchedKey["reference"]["fields"]
            if len(resolved) == 1:
                resolved = resolved[0]
            else:
                resolved = f"[\"{'","'.join(resolved)}\"]"
                match = f"\"{match}\"" # add quotes because the replace needs to replace the string with a list of strings.
        else:
            raise ValueError(
                f"Invalid match: {match}. Unexpected formatting length of spec template tag."
            )
        
        print(f'the resolved: {resolved}')
        spec = spec.replace(match, resolved, 1)
        pprint(f'new spec: {spec}')
        print('-----')
    
    # Special care needs to take place to handle comparisons
    # e.g. {lte} should be replaced with <=
    # this must happen after the other replacements that are 
    # looking for < and > characters.
    comparisons = [
        {"content": "{lte}", "resolved": "<="},
        {"content": "{gte}", "resolved": ">="},
        {"content": "{lt}", "resolved": "<"},
        {"content": "{gt}", "resolved": ">"}
    ]
    for comparison in comparisons:
        spec = spec.replace(comparison["content"], comparison["resolved"])
    
    print(f'final spec: {spec}')
    return spec


def extract_tags(text: str) -> List[Dict[str, Union[str, List[str]]]]:
    """
    Example input:
     "This is just to test <E> and <E1> and <E2> and <F:o> and <E.F:o> and <E1.F1:N> and <E2.F2:o|n> and more text"
    Example output:
        [
            {"original": "E", "entity": "E", "field": None, "field_type": None},
            {"original": "E1", "entity": "E1", "field": None, "field_type": None},
            {"original": "E2", "entity": "E2", "field": None, "field_type": None},
            {"original": "F:o", "entity": None, "field": "F", "field_type": ["o"]},
            {"original": "E1.F1:n", "entity": "E1", "field": "F1", "field_type": ["n"]},
            {"original": "E2.F2:o|n", "entity": "E2", "field": "F2", "field_type": ["o", "n"]}
        ]
        
    """
    pattern = r"<([^>]+)>"
    matches = re.findall(pattern, text)

    tags = []
    # match: each time the pattern appears in the text
    # <F.p.q> or <F.p>
    # <F.g>
    for match in matches:
        parts = match.split(".")
        sample, entity, field, location, field_type = None, None, None, None, None
        if len(parts) == 1:
            first = parts[0]
            # check if sample, field, entity, or location
            if first.startswith("S"):
                sample = first
            elif first.startswith("L"):
                location = first
            elif first.startswith("E"):
                entity=first
            else:
                field=first
                
        elif len(parts) == 2:
            first, second = parts
            sample=first
            if first.startswith("L"):
                location = second
            else:
                field=second
        elif len(parts) == 3:
            first, second, third=parts
            sample=first
            entity=second
            field=third
        else:
            raise ValueError(
                f"Invalid match: {match}. There should only be 1 or 2 '.'"
            )

        if field:
            field_parts = field.split(":")

            if len(field_parts) == 2:
                field, field_type = field_parts          
                
                #if len(field_type)==1:
                field_type = [
                    {"n": "nominal", 
                    "o": "ordinal", 
                    "q": "quantitative", 
                    "g": "genomic",
                    "g&q": "quantitative genomic",
                    "g&c": "categorical genomic",
                    "p": "point",
                    "p&n": "nominal point",
                    "p&o":"ordinal point",
                    "p&q":'quantitative point',
                    "s": "segment",
                    "s&n": "nominal segment",
                    "s&o":"ordinal segment",
                    "s&q": "quantitative segment",
                    "c": "connective"}[t]
                    for t in field_type.split("|")
                ]
                
                #else:
                #    raise ValueError(
                #        f"Invalid match: {match}. Field type must be specified"
                #    )
        tags.append(
            {
                "sample": sample,
                "entity": entity,
                "field": field,
                "location": location,
                "allowed_fields": field_type,
                "original": match,
            }
        )
     
    # infer sample and entity  
    infer_entity(tags) 
    infer_sample(tags)
    
    samples = set([tag["sample"] for tag in tags])
    #fields = set([tag["field"] for tag in tags if tag["field"]])
    entities = set(
        [str(tag["sample"]) + "_" + tag["entity"] for tag in tags if tag["entity"]]
    )
    fields = set(
        [str(tag["sample"]) + "_" + tag["entity"] + "_" + tag["field"] for tag in tags if tag["field"]]
    )
    
    locations = set(
        [str(tag["sample"]) + "_" + tag["location"] for tag in tags if tag["location"]]
    )
    
    #pprint({"tags": tags, "samples": list(samples), "entities": list(entities), "location": list(locations), "fields": list(fields)})
    return {"tags": tags, "samples": list(samples), "entities": list(entities), "location": list(locations), "fields": list(fields)}


def infer_sample(
    tags: List[Dict[str, Union[str, List[str]]]],
) -> List[Dict[str, Union[str, List[str]]]]:
    """
    Infer the sample based on the other samples. If none is provided, default to S.
    If there is an empty sample and multiple other samples defined, thwrow an error.
    """
    defined_samples = [tag["sample"] for tag in tags if tag["sample"]]
    
    unique_samples = set(defined_samples)

    if len(unique_samples) > 1 and any(not tag["sample"] for tag in tags):
        raise ValueError("Multiple samples defined, cannot infer empty sample.")
    for tag in [x for x in tags if not x["sample"]]:
        tag["sample"] = "S"
    return tags

def infer_entity(
    tags: List[Dict[str, Union[str, List[str]]]],
) -> List[Dict[str, Union[str, List[str]]]]:
    """
    Infer the entity based on the other entities. If none is provided, default to E.
    If there is an empty entity and multiple other entities defined, throw an error.
    """
    defined_entities = [tag["entity"] for tag in tags if tag["entity"]]
    unique_entities = set(defined_entities)

    if len(unique_entities) > 1 and any(not tag["entity"] for tag in tags):
        raise ValueError("Multiple entities defined, cannot infer empty entity.")
    
    for tag in [x for x in tags if not x["entity"] and not x["original"][0] == 'S']:
        tag["entity"] = "E"
    return tags


def expand_constraints(
    constraints: List[str], tags: List[Dict[str, Union[str, List[str]]]]
) -> List[str]:  # type: ignore
    """
    the current constraints will be expanded a bit
        e.g. F.c > 4 → F["udi:cardinality"] > 4
    the tags will add constraints for each field type
    and will add a constraint to ensure unique fields
    """
    expanded_constraints = []
    #constraints.split(',')
    if isinstance(constraints, str):
        constraints = ast.literal_eval(constraints)
        
    for constraint in constraints:
        # E1.r.E2.c.to → E1.r.E2['cardinality'].to
        resolved = constraint.replace(".c", "['udi:cardinality']")
        
        resolved = constraint.replace(".a", "['udi:assembly']")
        
        # E1.r.E2['cardinality'].to → E1.r[E2['entity']]['cardinality'].to
        resolved, isErConstraint = resolve_related_entity(resolved)
       
        if isErConstraint:
            relationship_existance = create_relationship_existence_constraint(constraint)
            expanded_constraints.append(relationship_existance)
        # E1.r[E2["name"]]['cardinality'].to →
        # E1['relationships][E2["name"]]['cardinality'].to
        resolved = resolved.replace(".r", "['foreignKeys']")
        # E1['relationships][E2["name"]]['cardinality'].to →
        # E1['relationships][E2["name"]]['cardinality']['to']
        resolved = resolved.replace(".to", "['to']")
        resolved = resolved.replace(".from", "['from']")
        resolved = resolved.replace(".fields", "['fields']")
        resolved = resolved.replace(".name", "['name']")
        #  E1.F1 → E1_F1
        resolved = resolved.replace(".", "_")
        #  F → E_F1
        resolved = add_default_entity(resolved)
        expanded_constraints.append(resolved)
        #print(f'final resolved: {resolved}')

    # Turn field types into constraints
    #print(f"tag sample: {[tag['sample'] for tag in tags if tag['sample']]}")
    #print(f"allowed fields: {[tag['allowed_fields'] for tag in tags if tag['field']]}")
    #print(f"tag fields: {[tag['field'] for tag in tags if tag['field']]}")
    #pprint(tags)
    
    #print("------")
    
    expanded_constraints.extend(
        [
            #f"{tag['sample']}_{tag['field']}['udi:data_type'] in {tag['allowed_fields']}",
            f"{tag['sample']}_{tag['entity']}_{tag['field']}['udi:data_type'] in {tag['allowed_fields']}"
            for tag in tags
            if tag["field"]
        ]
    )
    
    # Ensure fields are not repeated
    unique_fields = set(
        [str(tag["sample"]) + "_" + tag["entity"] + "_" + tag["field"] for tag in tags if tag["field"]]
    )
    # if len(unique_fields) > 1:
    #     for field in unique_fields:
    #         other_fields = unique_fields - {field}
    #         expanded_constraints.append(f"{field} not in {other_fields}")

    if len(unique_fields) > 1:
        for field in unique_fields:
            other_fields = unique_fields - {field}
            name_str = "['field']"
            other_fields_string = (
                "[" + ",".join([str(x) + name_str for x in other_fields]) + "]"
            )
            expanded_constraints.append(
                f"{field + name_str} not in {other_fields_string}"
            )

    # ensure that entities are not repeated
    unique_samples = set([tag["sample"] for tag in tags])
    if len(unique_samples) > 1:
        for sample in unique_samples:
            other_samples = unique_samples - {sample}
            e_str = "['sample']"
            other_samples_string = (
                "[" + ",".join([str(x) + e_str for x in other_samples]) + "]"
            )
            expanded_constraints.append(
                f"{sample + e_str} not in {other_samples_string}"
            )

    # ensure that fields belong to their entity and sample
    for field in unique_fields:
        sample = field.split("_")[0]
        expanded_constraints.append(f"{field}['sample'] == {sample}['sample']")
        entity = field.split("_")[1]
        #expanded_constraints.append(f"{field}['sample'] == {sample + "_" + entity}['sample']")
        #expanded_constraints.append(f"{field}['file'] == {sample + "_" + entity}['name']")

    return expanded_constraints


def create_relationship_existence_constraint(constraint: str) -> str:
    '''
    given an input E1.r.E2.c.to == 'one'
    want to generate a relationship existence constraint:
        E2['entity'] in [fk['reference']['resource'] for fk in E1['foreignKeys']]
    '''
    parts = constraint.split('.')
    if len(parts) < 3:
        raise ValueError("Unexpected relationship constraint length:", constraint)
    S1, r, S2 = parts[:3]
    if (S1[0] != 'S') or (S2[0] != 'S') or (r != 'r'):
        raise ValueError("Unexpected relationship constraint:", constraint)
    existence_constraint =  f"{S2}['sample'] in [fk['reference']['resource'] for fk in {S1}['foreignKeys']]"
    return existence_constraint

def resolve_related_entity(text): 
    # use regex to replace E1.r.E2.c.to with:
    # {fk['reference']['resource']:fk for fk in E1['foreignKeys']}.r[E2['entity']].c.to
    # other things (.c, .to) well be resolved elsewhere
    # assumes that the only time .E exists is when finding a relationship
    foundConstraint = False
    pattern = r'\.S[0-9]*'
    while re.search(pattern, text):
        foundConstraint = True
        match = re.search(pattern, text).group(0)
        resolved = f"[{match.lstrip('.')}['sample']]"
        #(f'Text before: {text}')
        text = text.replace(match, resolved)
        
    if foundConstraint:
        pattern = r'S[0-9]*\.r'
        while re.search(pattern, text):
            match = re.search(pattern, text).group(0)
            S_number = match.split(".")[0].lstrip("S")
            resolved = "{fk['reference']['resource']:fk for fk in S" + str(S_number) + "['foreignKeys']}"
            text = text.replace(match, resolved)
    return text, foundConstraint

def add_default_entity(text):
    # Use regex to match "F" that is not preceded by "_" and replace it with "S_E_F"
    modified_text = re.sub(r'(?<!_)F', r'S_E_F', text)
    # replace L that does not have _ to replace with S_L
    modified_text = re.sub(r'(?<!_)E', r'S_E', modified_text)
    # replace L that does not have _ to replace with S_L
    modified_text = re.sub(r'(?<!_)L', r'S_L', modified_text)
    return modified_text

def constraint_solver(
    samples: List[str],
    entities: List[str],
    fields: List[str],
    locations: List[str],
    constraints: List[str],
    sample_options: List[Dict[str, Union[str, int]]],
    entity_options: List[Dict[str, Union[str, int]]],
    field_options: List[Dict[str, Union[str, int]]],
    location_options:  List[Dict[str, Union[str, int]]]
) -> List[Dict[str, str]]:
    problem = Problem()
    # print(constraints)
    # print("⭐ constraints ⭐")
    # pprint(constraints)
    # print("⭐ entities ⭐")
    # pprint(samples)
    # pprint(sample_options)
    # print("⭐ locations ⭐")
    # pprint(locations)
    # pprint(location_options)
    # print("⭐ fields ⭐")
    # pprint(fields)
    #pprint(field_options)
    problem.addVariables(fields, field_options)
    problem.addVariables(samples, sample_options)
    problem.addVariables(entities, entity_options)
    problem.addVariables(locations, location_options)
        
    for constraint in constraints:
        problem.addConstraint(constraint) 
    
    s = problem.getSolutions()
    
    return s

# def test_constraint_solver():
#     problem = Problem()
#     fields = [
#         {"entity": "donors", "data_type": "nominal", "name": "A", "cardinality": 3},
#         {"entity": "donors", "data_type": "nominal", "name": "B", "cardinality": 18},
#         {
#             "entity": "samples",
#             "data_type": "quantitative",
#             "name": "C",
#             "cardinality": 5,
#         },
#         {
#             "entity": "samples",
#             "data_type": "quantitative",
#             "name": "D",
#             "cardinality": 23,
#         },
#     ]

#     entities = [
#         {"entity": "donors", "data_type": None, "name": None, "cardinality": 200},
#         {"entity": "samples", "data_type": None, "name": None, "cardinality": 200},
#     ]
#     problem.addVariables(["F1"], fields)
#     problem.addVariables(["E1", "E2"], entities)
#     # problem.addConstraint("F1['data_type'] in {'nominal'}")
#     # problem.addConstraint("F1['entity'] == E1['entity']")
#     problem.addConstraint("E1['entity'] != E2['entity']")
#     # problem.addConstraint("F1['cardinality'] > F2['cardinality']")
#     # problem.addConstraint("F2['cardinality'] > 4")
#     # problem.addConstraint("F1['entity'] == F2['entity']== 'samples'")
#     s = problem.getSolutions()
#     names = [[{k: v["name"], "ent": v["entity"]} for k, v in x.items()] for x in s]
#     # result: [[['F1', 'B'], ['F2', 'C']]]
#     pprint(names)
#     return


if __name__ == "__main__":
    # test_custom_parser()
    # test_grammar_parser()
    # test_constraint_solver()
    # def expand_template(row, entity_options, field_options):
    # expand_template(
    #     row={
    #         "constraints": [],
    #         "query_template": "How many <E> are there <F:Q>?",
    #         "spec_template": "{ source: '<E>', '<E.url> rep: <F>'}",
    #     },
    #     entity_options=[
    #         {
    #             "url": "./data/fake_sample.csv",
    #             "entity": "fake_sample",
    #             "name": None,
    #             "data_type": None,
    #             "cardinality": None,
    #         },
    #         {
    #             "url": "./data/fake_donor.csv",
    #             "entity": "fake_donor",
    #             "name": None,
    #             "data_type": None,
    #             "cardinality": None,
    #         },
    #         {
    #             "url": "./data/fake_file.csv",
    #             "entity": "fake_file",
    #             "name": None,
    #             "data_type": None,
    #             "cardinality": None,
    #         },
    #     ],
    #     field_options=[
    #         {
    #             "entity": "fake_sample",
    #             "name": "organ",
    #             "data_type": "nominal",
    #             "cardinality": 6,
    #         },
    #         {
    #             "entity": "fake_donor",
    #             "name": "weight",
    #             "data_type": "quantitative",
    #             "cardinality": 27,
    #         },
    #         {
    #             "entity": "fake_donor",
    #             "name": "height",
    #             "data_type": "quantitative",
    #             "cardinality": 27,
    #         },
    #     ],
    # )

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
    
    with open('example_schema.json', 'r') as f:
        schema=json.load(f)
    df=pd.read_csv('spec_generation_test.tsv', sep='\t')

    expanded_df = expand(df, schema)
    print(expanded_df)
    expanded_df.to_csv("spec_generation_output.csv")
