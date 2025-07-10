import time
import pickle
import sys
import re
import pandas as pd
import os
import json
from typing import Dict, Optional, Tuple, List, Any
from enum import Enum
from langchain.chat_models import init_chat_model
from langchain_openai import OpenAI
from langchain_core.prompts import PromptTemplate
from pydantic import BaseModel, Field
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from dotenv import load_dotenv
from rich import print
load_dotenv()

CACHE_FILE = "./datasets/paraphrase_cache.pkl"

#class to store transition types -> extensible for future types
class TransitionType(Enum):
    COMPARATIVE_ADDITION = "comparative addition"
    # VISUAL_CHANGE = "visual change" 
    # DATA_STRATIFICATION = "data stratification"
    # OVERLAY = "overlay"
    # SCOPE_SPECIFICITY = "scope specificity"
    
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

def parse_solution(sol):
    """Parse solution field from JSON string or dict"""
    if isinstance(sol, dict):
        return sol
    try:
        return json.loads(sol)
    except (TypeError, ValueError):
        from ast import literal_eval
        try:
            return literal_eval(sol)
        except:
            return {}

#follow linked pairs similar logic as Devin's code
def build_linked_pairs_from_csv(
    df: pd.DataFrame,
    link_templates: List[Dict[str, Any]]
) -> List[Tuple[int, int, Dict[str, Any]]]:

    linked_pairs: List[Tuple[int, int, Dict[str, Any]]] = []
    
    for L in link_templates:
        start_template = L['template']['start']
        end_template = L['template']['end']  
        
        for idx, row in df.iterrows():
            if row['query_template'] == start_template:
               
                followup_question = end_template

                pair_info = L.copy()
                pair_info['followup_question'] = followup_question
                linked_pairs.append((int(idx), int(idx), pair_info))
    
    print(f"Created {len(linked_pairs)} linked pairs")
    return linked_pairs

#this template pairs is only tailored for comparative addition -> we can add more cases/scenarios for other transition types 
def export_template_pairs_to_csv(
    df: pd.DataFrame,
    linked: list[tuple[int, int, dict]],
    output_path: str = "template_pairs.csv"
):
    records = []
    for idx1, idx2, L in linked:
        d1 = df.loc[idx1]
        d2 = df.loc[idx2]
        row_dict = {
            "template_start": L["template"]["start"],
            "template_end":   L["template"]["end"],
        }
        
        entity_match = re.search(r'Map the (.*?) data at', d1['query_base'])
        location_match = re.search(r'at (.*?)$', d1['query_base'])  
        d1_entity = entity_match.group(1).strip() if entity_match else "sv" #this we can change to match other entities for our original question
        location = location_match.group(1).strip() if location_match else "region"
        solution = parse_solution(d1['solution'])
        
        
        #determine both entity and location-> randomize the choosing of follow up e and l, haven't incorporated S yet. 
        comparative_entity, comparative_location = determine_comparative_entity_and_location(
            d1_entity, location, solution, idx1
        )  
        
        followup_question = L["template"]["end"].replace("<E>", comparative_entity).replace("<L>", comparative_location)
        
        row_dict["D1_query_base"] = d1['query_base']
        row_dict["D2_query_base"] = followup_question
        row_dict["D1_dataset_schema"] = d1['dataset_schema']
        row_dict["D2_dataset_schema"] = d2['dataset_schema']
        row_dict["transition_type"] = L.get("transition_type", "comparative addition") #chane for others
    
        for col in df.columns:
            if col not in ['query_base', 'dataset_schema']:
                row_dict[f"D1_{col}"] = d1[col]
        
        if L.get("transition_type") == TransitionType.COMPARATIVE_ADDITION.value: 
            row_dict["D2_query_template"] = L["template"]["end"]  
            row_dict["D2_constraints"] = f"[\"E2['format'] == '{comparative_entity}'\"" #constraint for comparative entity
            
            d2_spec_template = generate_comparative_addition_spec(
                d1['spec'], 
                d1['query_base'], 
                followup_question, 
                d1['dataset_schema'], 
                solution
            )
            row_dict["D2_spec_template"] = json.dumps(d2_spec_template.get("spec_template", {}))
            
            row_dict["D2_query_type"] = "utterance"
            
            row_dict["D2_creation_method"] = "template_expansion"
            row_dict["D2_chart_type"] = "comparative_addition"
        #the if else ladder would extend to different types of transitions

        d1_complexity = d1.get('chart_complexity', 'medium') #complexity can be changed for others
        complexity_map = {'low': 'medium', 'medium': 'high', 'high': 'high'}
        row_dict["D2_chart_complexity"] = complexity_map.get(d1_complexity, 'high') #default setting for now 
        
        same_columns = ['solution', 'spec_key_count']
        for col in same_columns:
            if col in df.columns:
                row_dict[f"D2_{col}"] = d1[col]
                
        records.append(row_dict)
    
    out_df = pd.DataFrame(records)
    out_df.to_csv(output_path, index=False)
    return out_df

class ParaphrasedQuestionPair(BaseModel): #aim: get rid of these colloquial-technical scores and assume a new system eventually. 
    """A paraphrased question pair with metadata"""
    paraphrasedQ1: str = Field(description="The paraphrased initial question.")
    paraphrasedQ2: str = Field(description="The paraphrased follow-up utterance.")
    formality: int = Field(
        description="Colloquial (Score=1) language is informal and used in everyday conversation, while standard language (Score=5) follows established rules and conventions and is used in more formal situations."
    )
    expertise: int = Field(
        description="Technical language (Score=5) is often used by experts in a particular field and includes specialized terminology and jargon. Non-technical language (Score=1), on the other hand, is more accessible to a general audience and avoids the use of complex terms."
    )

class ParaphrasedPairsList(BaseModel):
    """A class that contains a list of paraphrased question pairs."""
    pairs: List[ParaphrasedQuestionPair] = Field(
        default_factory=list,
        description="A list of paraphrased question pairs with their metadata."
    )

def construct_paraphrase_prompt_template(): #currently latches onto the word mapping but we can fix with template generation or rewording question
    template = '''
You will be given:
    • Q1 (Initial Question): Pose a clear question that would prompt a visualization. Use language appropriate to the specified Expertise and Formality scores. Please do not latch onto words like map.
    • Q2 (Follow-Up Utterance): This should be a statement or request for additional analysis, not a question. Maintain the same Expertise and Formality style as Q1.
    • Expertise Score (1–5): 1 = non-technical → 5 = highly technical
    • Formality Score (1–5): 1 = colloquial → 5 = very formal

Task:
Rewrite Q1 and Q2 as a natural two-step interaction around a data visualization:
    1.	Q1 should clearly request a visualization, using language matching the given Expertise and Formality.
    2.	Q2 should be an utterance (statement/request) asking for additional data, in the same style.

Guidelines:
- Consistency: Apply the same expertise and formality level to both Q1 and Q2.
- Don't start Q2 with: "Can you", "Could you", "Would it be possible", "Is it possible", "Would you", "Additionally", etc.
- Formality → Field Names:
    - Low (1–2): use general terms or synonyms instead of exact field names.
    - High (4–5): preserve exact field names.
- Expertise → Terminology:
    - Low (1–2): use everyday, accessible wording.
    - High (4–5): use precise, domain-specific terms.

Example:
Input:
  Q1: Create a visualization of the sv data at CCR2
  Q2: Add another visualization at CCR2 to compare
  Expertise Score: 1, Formality Score: 1
-----
Rewrite the following:
Q1: {q_1}
Q2: {q_2}
'''
    return template

def init_llm():
    llm = init_chat_model("gpt-4o", model_provider="openai")
    structured_llm = llm.with_structured_output(ParaphrasedPairsList)
    prompt_template = PromptTemplate.from_template(construct_paraphrase_prompt_template())
    llm_chained = prompt_template | structured_llm
    return llm_chained

def paraphrase_question_pair(
    cache_lock, 
    llm, 
    key, 
    question_1: str, 
    question_2: str, 
    transition_type: str,
    cache: Dict[str, ParaphrasedPairsList] = {}, 
) -> Tuple[ParaphrasedPairsList, bool]:
    
    if key in cache:
        return cache[key], True
    
    
    response = llm.invoke({
        "q_1": question_1,
        "q_2": question_2,
        "transition_type": transition_type,
    })
    
    with cache_lock:
        try:
            cache[key] = response
        except Exception as e:
            print(f"Error updating cache object: {e}")
    
    return response, False

#determine what comparative entity and location should be used based on available files -> try to vary
def determine_comparative_entity_and_location(
    d1_entity: str, 
    d1_location: str, 
    solution: Dict[str, Any], 
    idx: int = 0
) -> Tuple[str, str]:

    if not solution or 'S' not in solution:
        return d1_entity, d1_location
    
    files = solution['S'].get('files', [])
    has_sv = any(f.get('file') == 'sv' for f in files)
    has_cna = any(f.get('file') == 'cna' for f in files)
    has_vcf = any(f.get('file') == 'vcf' for f in files)
    
    available_entities = []
    if has_sv:
        available_entities.append('sv')
    if has_cna:
        available_entities.append('cna')
    if has_vcf:
        available_entities.append('vcf')
    
    sample_info = solution.get('S', {})
    genes_info = sample_info.get('udi:genes', [])
    available_genes = [gene['name'] for gene in genes_info if 'name' in gene]
    
    variation = idx % 3 
    #vary 1- different entity, same location
    #vary 2- same entity, different location  
    #vary 3- different entity, different location
    if variation == 0 and len(available_entities) > 1:
        other_entities = [e for e in available_entities if e != d1_entity]
        if other_entities:
            return other_entities[0], d1_location
    elif variation == 1 and len(available_genes) > 1:
        other_genes = [g for g in available_genes if g != d1_location]
        if other_genes:
            return d1_entity, other_genes[0]
    elif variation == 2 and len(available_entities) > 1 and len(available_genes) > 1:
        other_entities = [e for e in available_entities if e != d1_entity]
        other_genes = [g for g in available_genes if g != d1_location]
        if other_entities and other_genes:
            return other_entities[0], other_genes[0]


def generate_comparative_addition_spec(
    original_spec: str,
    question_1: str, 
    question_2: str, 
    dataset_schema: str,
    solution: Dict[str, Any],
) -> Dict[str, Any]:
    
    original_spec_dict = json.loads(original_spec)
    
    sample_info = solution.get('S', {})
    files = sample_info.get('files', [])
    
    sv_file = next((f for f in files if f.get('file') == 'sv'), None)
    cna_file = next((f for f in files if f.get('file') == 'cna'), None)
    vcf_file = next((f for f in files if f.get('file') == 'vcf'), None)
    
    entity_match = re.search(r'Add another (.*?) at', question_2)
    comparative_entity = entity_match.group(1).strip() if entity_match else "cna"
    
    #create comparative track with different data source
    #the aim was to call template expansion to generalize these templates
    #but still getting an SE_2 error
    
    if comparative_entity == "cna" and cna_file:
        comparative_add_track = {
            "data": {
                "url": cna_file.get('url', ''),
                "type": "csv",
                "separator": "\t",
                "genomicFieldsToConvert": [
                    {
                        "chromosomeField": "chromosome",
                        "genomicFields": ["start", "end"]
                    }
                ]
            },
            "mark": "rect", 
            "x": {"field": "start", "type": "genomic"},
            "xe": {"field": "end", "type": "genomic"},
            "y": {"field": "total_cn", "type": "quantitative"},
            "color": {"field": "total_cn", "type": "quantitative"},
            "opacity": {"value": 0.6},
            "strokeWidth": {"value": 0}
        }
    elif comparative_entity == "vcf" and vcf_file:
        comparative_add_track = {
            "data": {
                "url": vcf_file.get('url', ''),
                "type": "vcf",
                "indexUrl": vcf_file.get('url', '') + '.tbi'
            },
            "mark": "point",
            "x": {"field": "POS", "type": "genomic"},
            "color": {"field": "ALT", "type": "nominal"},
            "size": {"value": 3},
            "opacity": {"value": 0.8}
        }
    else:
        comparative_add_track = {
            "data": {
                "url": sv_file.get('url', '') if sv_file else '',
                "type": "csv",
                "separator": "\t",
                "genomicFieldsToConvert": [
                    {
                        "chromosomeField": "chrom1",
                        "genomicFields": ["start1", "end1"]
                    },
                    {
                        "chromosomeField": "chrom2", 
                        "genomicFields": ["start2", "end2"]
                    }
                ]
            },
            "mark": "withinLink",
            "x": {"field": "start1", "type": "genomic"},
            "xe": {"field": "end2", "type": "genomic"},
            "stroke": {"field": "svclass", "type": "nominal"}, 
            "strokeWidth": {"value": 2}, 
            "opacity": {"value": 0.5} 
        }
    
    #clone the original spec for combination
    combined_spec = json.loads(json.dumps(original_spec_dict))
    
    if "views" in combined_spec and len(combined_spec["views"]) > 0:
        if "tracks" in combined_spec["views"][0]:
            combined_spec["views"][0]["tracks"].append(comparative_add_track)
    elif "tracks" in combined_spec:
        combined_spec["tracks"].append(comparative_add_track)
    
    result = {
        "initial_question": question_1,
        "followup_question": question_2,
        "dataset_schema": dataset_schema,
        "spec_template": combined_spec
    }
    
    
    return result

def multi_step_paraphrase_with_specs(
    df: pd.DataFrame, 
    schema_list: List[Dict], 
) -> pd.DataFrame:
    
    cache = get_cache()
    llm = init_llm()
    lock = threading.Lock()
    completed_rows = 0
    max_worker_count = 5

    def worker(row, row_index):
        nonlocal completed_rows
        
        question_1 = row.get("D1_query_base")
        question_2 = row.get("D2_query_base")
        transition_type = row.get("transition_type")
        dataset_name = row.get("D1_dataset_schema")
        original_spec = row.get("D1_spec", "{}")
        solution_str = row.get("D1_solution", "{}")
        
        try:
            solution = eval(solution_str) if isinstance(solution_str, str) else solution_str
        except:
            solution = {}
        
        dataset_schema = next(
            (schema for schema in schema_list 
            if schema.get('udi:name', schema.get('name')) == dataset_name), 
            None
        )
        
        if dataset_schema is None:
            dataset_schema = {"name": dataset_name} 
        
        try:
            key = f"{dataset_name}¶{question_1}¶{question_2}¶{transition_type}"
            response, is_cached = paraphrase_question_pair(
                lock, llm, key, question_1, question_2, transition_type, 
                cache
            )
        except Exception as e:
            print(f"Error in row {row_index}: {e}")
            time.sleep(5)
            return [], row_index
        
        if not is_cached:
            time.sleep(1.5)
        
        result_rows = []
        if response:
            for pair in response.pairs:
                viz_spec_data = generate_comparative_addition_spec(
                        original_spec,
                        pair.paraphrasedQ1, 
                        pair.paraphrasedQ2, 
                        dataset_name,
                        solution
                ) #combine both specs... again, but will fix
                new_data = {
                    "D1_query": pair.paraphrasedQ1,
                    "D2_query": pair.paraphrasedQ2,
                    "expertise": pair.expertise,
                    "formality": pair.formality,
                    "spec": json.dumps(viz_spec_data.get("spec_template", {})),
                    "spec_template": json.dumps(viz_spec_data.get("spec_template", {})),
                }
                new_data.update(row)
                result_rows.append(new_data)
        
        with lock:
            completed_rows += 1
            display_progress(df, completed_rows)
        
        return result_rows, row_index

    total_rows = len(df)
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

    new_rows = [item for sublist in new_rows if sublist is not None for item in sublist]
    
    update_cache(cache)
    return pd.DataFrame(new_rows)

def display_progress(df, index):
    total_rows = len(df)
    progress = (index / total_rows) * 100
    bar_length = 30
    filled_length = int(bar_length * index // total_rows)
    bar = '=' * filled_length + '-' * (bar_length - filled_length)
    sys.stdout.write(f"\rParaphrasing row {index}/{total_rows} [{bar}] {progress:.2f}%")
    sys.stdout.flush()

def get_cache():
    cache = {}
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, "rb") as f:
                cache = pickle.load(f)
        except Exception as e:
            print(f"Failed to load cache from file: {e}")
            cache = {}
    return cache

def update_cache(cache):
    try:
        with open(CACHE_FILE, "wb") as f:
            pickle.dump(cache, f)
    except Exception as e:
        print(f"Failed to save cache: {e}")


if __name__ == "__main__":
    with open("multi_step_links.json", "r") as f:
        multi_step_links = json.load(f)
    print(f"Loaded {len(multi_step_links)} link templates")
    
    #our current dataset for now -> can be changed to longer .csv 
    df = pd.read_csv("paraphrasing_questions.csv")
    
    #filter to unique "unparaphrased" queries by query_base -> in original script
    if 'query_base' in df.columns:
        df_unique = df.drop_duplicates(subset=['query_base'], keep='first').reset_index(drop=True)
    
    
    #build linked pairs based on the multi_step_links.json templates
    linked_pairs = build_linked_pairs_from_csv(df_unique, multi_step_links)
    template_pairs_df = export_template_pairs_to_csv(df_unique, linked_pairs)
    

    with open("example_schema.json", "r") as f:
        schema_list = json.load(f)
   
    
    if not template_pairs_df.empty:
        result_df = multi_step_paraphrase_with_specs(template_pairs_df, schema_list)
        result_df.to_csv("multistepoutput.csv", index=False)

