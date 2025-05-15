import json
import os
import sys
import time
import pickle
from ast import literal_eval
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

import pandas as pd
from datasets import load_dataset
from dotenv import load_dotenv
from huggingface_hub import hf_hub_download
from langchain.chat_models import init_chat_model
from langchain_openai import AzureChatOpenAI
from langchain_core.prompts import PromptTemplate
from pydantic import BaseModel, Field
from rich import print

from typing import List, Tuple, Dict, Any, Optional
load_dotenv()

CACHE_FILE = "./datasets/multi_step_paraphrase_cache.pkl"

def get_by_path(d: Dict[str, Any], path: str) -> Any:
    """
    Traverse a nested dict `d` following a dot-separated `path` (e.g. "E.F.entity").
    Returns the value or None if any key is missing.
    """
    cur = d
    for key in path.split('.'):
        if not isinstance(cur, dict) or key not in cur:
            return None
        cur = cur[key]
    return cur

# assuming `df` is your DataFrame and df['solution'] contains JSON‐encoded strings
def parse_solution(sol):
    # if it's already a dict, leave it
    if isinstance(sol, dict):
        return sol
    # otherwise, try to load from JSON string
    try:
        return json.loads(sol)
    except (TypeError, ValueError):
        # fallback: if it's a Python‐style dict literal
        from ast import literal_eval
        return literal_eval(sol)



def build_linked_pairs(
    df: pd.DataFrame,
    link_templates: List[Dict[str, Any]],
    max_samples_per_group=50,
) -> List[Tuple[int, int, Dict[str, Any]]]:
    """
    For each link‐template L, extract the necessary match‐keys from
    `solution` into temporary columns, then inner‐merge on:
      1) core fields: dataset_schema, expertise, formality
      2) each match: left.solution[L.start][L.on] == right.solution[L.end][L.on]
    Truncate each merged result to the first `max_samples_per_group` rows,
    print its shape, and return (orig_idx_left, orig_idx_right, L) for each hit.
    """
    core_fields = ['dataset_schema', 'expertise', 'formality']
    linked_pairs: List[Tuple[int, int, Dict[str, Any]]] = []

    for L in link_templates:
        start_q = L['template']['start']
        end_q = L['template']['end']
        matches = L.get('match', [])

        # 1) take only the rows matching start/end queries
        left = df[df['query_template'] == start_q].copy()
        right = df[df['query_template'] == end_q].copy()

        # parse JSON strings once
        left['solution'] = left['solution'].map(parse_solution)
        right['solution'] = right['solution'].map(parse_solution)

        # preserve original indices
        left['orig_idx'] = left.index
        right['orig_idx'] = right.index

        # 2) build join‐keys from the solution dict per `matches`
        left_on = core_fields.copy()
        right_on = core_fields.copy()

        for i, m in enumerate(matches):
            # column names for this match
            col_l = f"_match_l_{i}"
            col_r = f"_match_r_{i}"

            # extract values
            path_l = f"{m['start']}.{m['on']}"
            path_r = f"{m['end']}.{m['on']}"

            left[col_l] = left['solution'].apply(lambda sol: get_by_path(sol, path_l))
            right[col_r] = right['solution'].apply(lambda sol: get_by_path(sol, path_r))

            left_on.append(col_l)
            right_on.append(col_r)

        # 3) ensure core_fields all exist (should already, but safe)
        for fld in core_fields:
            if fld not in left:
                left[fld] = pd.NA
            if fld not in right:
                right[fld] = pd.NA

        # 4) inner‐merge on all keys
        merged = left.merge(
            right,
            left_on=left_on,
            right_on=right_on,
            how='inner',
            suffixes=('_1', '_2'),
            copy=False,
        )

        print(merged.shape)

        # 5a) statistics: count per unique dataset_schema
        unique_schemas = ['hubmap_2025-05-05', 'SenNet', 'MetabolomicsWorkbench', 'MoTrPAC', '4DN']

        # after merge, dataset_schema is suffixed with _1
        counts = merged['dataset_schema_1'].value_counts().reindex(unique_schemas, fill_value=0)
        print(f"Dataset_schema counts: {counts.to_dict()}")

        # Group by dataset_schema and take up to max_samples_per_group rows from each
        truncated = pd.concat([
            group.head(max_samples_per_group) for _, group in merged.groupby('dataset_schema')
        ])

        # 6) report shape
        print(
            f"Template {start_q} → {end_q} merged shape: {truncated.shape}"
        )

        # 7) collect the linked pairs
        if truncated.shape[0] <= 250:
            for i1, i2 in zip(truncated['orig_idx_1'], truncated['orig_idx_2']):
                linked_pairs.append((int(i1), int(i2), L))

        # (optional) drop the temporary match cols to free memory
        drop_cols = [c for c in merged.columns if c.startswith('_match_')]
        merged.drop(columns=drop_cols, inplace=True, errors='ignore')

    return linked_pairs




def export_linked_to_csv(
    df: pd.DataFrame,
    linked: list[tuple[int, int, dict]],
    output_path: str = "linked_pairs.csv"
):
    """
    Build a DataFrame where each row contains:
      - the link-template info (start/end)
      - all columns of df for D1 (prefixed "D1_")
      - all columns of df for D2 (prefixed "D2_")
    Then save to CSV.
    """
    records = []
    for idx1, idx2, L in linked:
        row_dict = {
            "template_start": L["template"]["start"],
            "template_end":   L["template"]["end"],
        }
        # grab the two rows
        d1 = df.loc[idx1]
        d2 = df.loc[idx2]
        # add every column from df, prefixed
        for col in df.columns:
            row_dict[f"D1_{col}"] = d1[col]
            row_dict[f"D2_{col}"] = d2[col]
        records.append(row_dict)

    out_df = pd.DataFrame(records)
    out_df.to_csv(output_path, index=False)
    print(f"Wrote {len(out_df)} rows to {output_path}")


class ParaphrasedSentence(BaseModel):
    """A paraphrased sentence with metadata on the dimension of formality and expertise"""

    paraphrasedQ1: str = Field(description="The paraphrased Q1.")
    paraphrasedQ2: str = Field(description="The paraphrased Q2.")
    formality: int = Field(
        description="Colloquial (Score=1) language is informal and used in everyday conversation, while standard language (Score=5) follows established rules and conventions and is used in more formal situations."
    )
    expertise: int = Field(
        description="Technical language (Score=5) is often used by experts in a particular field and includes specialized terminology and jargon. Non-technical language (Score=1), on the other hand, is more accessible to a general audience and avoids the use of complex terms."
    )

class ParaphrasedSentencesList(BaseModel):
    """A class that contains a list of paraphrased sentences."""
    sentences: list[ParaphrasedSentence] = Field(
        default_factory=list,
        description="A list of paraphrased sentences with their metadata."
    )

def construct_prompt_template():
    template = '''
You will be given:
    • Q1 (Initial Question): Pose a clear question that would prompt a visualization. Use language appropriate to the specified Expertise and Formality scores.
	• Q2 (Follow-Up Question): Refer only to the new insight or metric revealed by that visualization. Maintain the same Expertise and Formality style as Q1.
	• Expertise Score (1–5): 1 = non-technical → 5 = highly technical
	• Formality Score (1–5): 1 = colloquial → 5 = very formal

Task:
Rewrite Q1 and Q2 as a natural two-step interaction around a data visualization:
	1.	Q1 should clearly request a visualization, using language matching the given Expertise and Formality.
    2.	Q2 should ask only about the new insight revealed by that visualization, in the same style.
-----

Guidlines:
- Consistency: Apply the same expertise and formality level to both Q1 and Q2.
- Formality → Field Names:
    - Low (1–2): use general terms or synonyms instead of exact field names.
    - High (4–5): preserve exact field names.
- Expertise → Terminology:
    - Low (1–2): use everyday, accessible wording.
    - High (4–5): use precise, domain-specific terms.
-----
Example:

Input:
  Q1: What is the range of tissue_weight_values?
  Q2: What is the range of tissue_weight_values for each assay_type?
  Expertise Score: 1, Formality Score: 1

Output:
  Q1: What is the range of tissue weight values?
  Q2: Break this down by assay type.

-----
Rewrite the following:
Q1: {q_1}
Q2: {q_2}


'''
    # constuct all possible score combinations
    for i in [1, 3, 5]:
        for j in [1, 3, 5]:
            template += f'Expertise Score: {i}, Formality Score: {j}\n'
    return template

def init_llm():
    # llm = init_chat_model("gpt-4o-mini", model_provider="openai")
    llm = AzureChatOpenAI(
        azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"),
        openai_api_version=os.getenv("AZURE_OPENAI_API_VERSION"),
        deployment_name="gpt-4o",
        api_key=os.getenv("AZURE_OPENAI_API_KEY"),
        temperature=1.0,
    )

    structured_llm = llm.with_structured_output(ParaphrasedSentencesList)

    prompt_template = PromptTemplate.from_template(construct_prompt_template())
    llm_chained = prompt_template | structured_llm
    return llm_chained


def paraphrase_query(
        cache_lock, 
        llm, 
        key, 
        question_1: str, 
        question_2: str, 
        dataset_schema: str, 
        cache: Dict[str, ParaphrasedSentencesList] = {}, 
        only_cached = False
    ) -> Tuple[ParaphrasedSentencesList, bool]:

    if key in cache:
        return cache[key], True
    
    if only_cached:
        not_paraphrased = ParaphrasedSentence(
            paraphrasedQ1=question_1,
            paraphrasedQ2=question_2,
            formality=-1,
            expertise=-1
        )
        response = ParaphrasedSentencesList(
            sentences=[not_paraphrased]
        )
        return response, True
    
    response = llm.invoke({
        "q_1": question_1,
        "q_2": question_2,
    })
    with cache_lock:
        try:
            cache[key] = response
        except Exception as e:
            print(f"Error updating cache object: {e}")
    return response, False

def get_cache():
    cache = {}
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, "rb") as f:
                cache = pickle.load(f)
        except Exception as e:
            print(f"Failed to load cache from file: {e}")
    return cache

def update_cache(cache):
    with open(CACHE_FILE, "wb") as f:
        pickle.dump(cache, f)
    return

def display_progress(df, index):
    total_rows = len(df)
    progress = (index / total_rows) * 100
    bar_length = 30
    filled_length = int(bar_length * index // total_rows)
    bar = '=' * filled_length + '-' * (bar_length - filled_length)
    sys.stdout.write(f"\rParaphrasing row {index}/{total_rows} [{bar}] {progress:.2f}%")
    sys.stdout.flush()



def multi_step_paraphrase(df, schema_list, only_cached: Optional[bool] = False) -> pd.DataFrame:
    '''
    Input dataframe will have the following relevant columnns:
    - query_base: the original query
    - dataset_schema: the name of the dataset schema
    
    Output dataframe will have the following relevant columns:
    - query: the paraphrased query
    - expertise: the expertise score of the paraphrased query
    - formality: the formality score of the paraphrased query
    '''
    # simplify the schema_list by removing long attributes that aren't needed in the prompt
    schema_list = [json.loads(json.dumps(schema)) for schema in schema_list]
    for dataset_schema in schema_list:
        resources = dataset_schema.get("resources", [])
        if not isinstance(resources, list):
            raise ValueError(f"Expected a list of resources, but got {type(resources)}")
        for resource in resources:
            resource_schema = resource.get("schema", {})
            if not isinstance(resource_schema, dict):
                raise ValueError(f"Expected a dict for schema, but got {type(resource_schema)}")
            fields = resource_schema.get("fields", [])
            for field in fields:
                field.pop("udi:overlapping_fields")

    cache = get_cache()
    index = 0
    llm = init_llm()
    cache_interval = 25
    interval_index = 0

    lock = threading.Lock()
    completed_rows = 0

    max_worker_count = 5

    def worker(row, row_index):
        nonlocal interval_index, completed_rows
        question_1 = row["D1_query_base"]
        question_2 = row["D2_query_base"]
        dataset_name = row["D1_dataset_schema"]
        dataset_schema = next((schema for schema in schema_list if schema['udi:name'] == dataset_name), None)
        # convert nexted dict into json string
        if dataset_schema is not None:
            dataset_schema = json.dumps(dataset_schema, indent=0)
        else:
            raise ValueError(f"Dataset schema '{dataset_name}' not found in schema list.")
        try:
            key = f"{dataset_name}¶{question_1}¶{question_2}"
            response, is_cached = paraphrase_query(
                lock, 
                llm, 
                key, 
                question_1, 
                question_2, 
                dataset_schema, 
                cache,
                only_cached
            )
        except Exception as e:
            print(f"Error in row {row_index}: {e}")
            time.sleep(5)
            return [], row_index
            
        if not is_cached and not only_cached:
            time.sleep(1.5)
        result_rows = []
        if response:
            for sentence in response.sentences:
                new_data = {
                    "D1_query": sentence.paraphrasedQ1,
                    "D2_query": sentence.paraphrasedQ2,
                    "expertise": sentence.expertise,
                    "formality": sentence.formality,
                }
                new_data.update(row)
                result_rows.append(new_data)
        
        with lock:
            try:
                completed_rows += 1
                display_progress(df, completed_rows)
            except Exception as e:
                print(f"Error updating progress: {e}")
        if not is_cached:
            with lock:
                try:
                    interval_index += 1
                    if interval_index % cache_interval == 0:
                        update_cache(cache)
                except Exception as e:
                    print(f"Error updating cache file: {e}")
        return result_rows, row_index


    total_rows = len(df)
    df = df.drop(columns=['D1_query', 'D2_query'])
    new_rows = [None] * total_rows

    with ThreadPoolExecutor(max_workers=max_worker_count) as executor:
        futures = {executor.submit(worker, row, idx): idx for idx, (_, row) in enumerate(df.iterrows())}

        for future in as_completed(futures):
            try:
                result_rows, index = future.result(timeout=90)
            except Exception as e:
                print(f"Timeout or error in future {futures[future]}: {e}")
                continue
            with lock:
                try:
                    new_rows[index] = result_rows
                except Exception as e:
                    print(f"Error updating new_rows list: {e}")

    # Flatten the list of lists
    new_rows = [item for sublist in new_rows if sublist is not None for item in sublist]

    update_cache(cache)
    df = pd.DataFrame(new_rows)
    return df


def print_header(message):
        print("\n" + "#" * 80)
        print("| " + message + " " * (77 - len(message)) + "|")
        print("#" * 80)


if __name__ == "__main__":

    dataset_schemas = "./datasets/multi_step_links.json"
    with open(dataset_schemas) as f:
        multi_step_links = json.load(f)
        print(f"[Links] Total: {len(multi_step_links)}")

    dataset = load_dataset(f"DevLan/DQVis", name="dqvis")
    df = dataset['train'].to_pandas()
    
    # filter to unique "unparaphrased" queries by query_base
    df_unique = df.drop_duplicates(subset=['query_base'], keep='first').reset_index(drop=True)
    
    print(f"Reduced from {len(df)} total rows to {len(df_unique)} unique bases")
    linked = build_linked_pairs(df_unique, multi_step_links)
    export_linked_to_csv(df_unique, linked)

    # Read the linked pairs with unique query_base
    df = pd.read_csv("linked_pairs.csv")
    template_question_count = df.shape[0]
    with open('./datasets/output_catalogue.json') as f:
        schema_list = json.load(f)
    
    df = multi_step_paraphrase(df=df, schema_list=schema_list)
    paraphrased_question_count = df.shape[0]

    # Sanity Check output
    print_header("4. Sanity Check output dimensions")
    print(f"Generated {template_question_count:,} templates and expanded and paraphrased to {paraphrased_question_count:,}.")
    
    print_header("exporting ./out/training_data.json...")
    df.to_csv('./out/multi_step_data.csv')





