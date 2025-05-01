import pickle
import sys
from typing import Dict, Optional, Tuple
import pandas as pd
import os
from langchain.chat_models import init_chat_model
from langchain_openai import AzureChatOpenAI
from langchain_core.prompts import PromptTemplate
from pydantic import BaseModel, Field
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import json

from dotenv import load_dotenv
from rich import print
load_dotenv()

CACHE_FILE = "./datasets/paraphrase_cache.pkl"


def paraphrase(df, schema_list, only_cached: Optional[bool] = False) -> pd.DataFrame:
    '''
    Input dataframe will have the following relevant columnns:
    - query_base: the original query
    - dataset_schema: the name of the dataset schema
    
    Output dataframe will have the following relevant columns:
    - query: the paraphrased query
    - expertise: the expertise score of the paraphrased query
    - formality: the formality score of the paraphrased query
    '''

    cache = get_cache()
    new_rows = []
    total_rows = len(df)
    index = 0
    llm = init_llm()
    cache_interval = 10
    interval_index = 0

    cache_lock = threading.Lock()
    result_lock = threading.Lock()
    progress_lock = threading.Lock()
    completed_rows = 0

    max_worker_count = 1

    def worker(row, row_index):
        nonlocal interval_index, completed_rows
        query_base = row["query_base"]
        role = 'Computational Biologist' # TODO: Vary this.
        dataset_name = row["dataset_schema"]
        dataset_schema = next((schema for schema in schema_list if schema['udi:name'] == dataset_name), None)
        # convert nexted dict into json string
        if dataset_schema is not None:
            dataset_schema = json.dumps(dataset_schema, indent=0)
            dataset_schema = "UNKNOWN" # TODO: Remove after done debugging
        else:
            raise ValueError(f"Dataset schema '{dataset_name}' not found in schema list.")
        try:
            response, is_cached = paraphrase_query(llm, query_base, role, dataset_schema, cache, only_cached)
            result_rows = []
            if response:
                for sentence in response.sentences:
                    new_data = {
                        "query": sentence.paraphrasedSentence,
                        "expertise": sentence.expertise,
                        "formality": sentence.formality
                    }
                    new_data.update(row)
                    result_rows.append(new_data)
            with progress_lock:
                completed_rows += 1
                display_progress(df, completed_rows)
            if not is_cached:
                with cache_lock:
                    interval_index += 1
                    if interval_index % cache_interval == 0:
                        update_cache(cache)
            return result_rows
        except Exception as e:
            print(f"Error in row {row_index}: {e}")
            return []


    with ThreadPoolExecutor(max_workers=max_worker_count) as executor:
        futures = {executor.submit(worker, row, idx): idx for idx, (_, row) in enumerate(df.iterrows())}

        for future in as_completed(futures):
            result_rows = future.result()
            with result_lock:
                new_rows.extend(result_rows)

    update_cache(cache)
    df = pd.DataFrame(new_rows)
    return df

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
    return cache

def update_cache(cache):
    with open(CACHE_FILE, "wb") as f:
        pickle.dump(cache, f)
    return


class ParaphrasedSentence(BaseModel):
    """A paraphrased sentence with metadata on the dimension of formality and expertise"""

    paraphrasedSentence: str = Field(description="The paraphrased sentence.")
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
You are a paraphrasing assistant. Your task is to rewrite a given sentence with various styles of language usage.
The sentence will either be a question about data, or request to construct a data visualization.

The input sentence will include entity names and fields names from the data.
The dataset schema will also be provided to you to enable better paraphrasing of the field and entity names.
Dataset schema: {dataset_schema}

Since the people interacting with the data have different roles, please paraphrase the sentence in a way that is appropriate for the given role.
Role: {role}

Score-A of 1 indicates a higher tendency to use {dim1_1} language and a Score-A of 5 indicates a higher tendency to use {dim1_5} language.
Score-B of 1 indicates a higher tendency to use {dim2_1} language and a Score-B of 5 indicates a higher tendency to use {dim2_5} language.
Rewrite the following sentence as if it were spoken by a person with a given score for language usage.

Sentence: {sentence}

##
'''
    # constuct all possible score combinations
    for i in range(1, 6):
        for j in range(1, 6):
            template += f'Score-A {i}, Score-B {j}:\n'
    return template


def init_llm():
    # llm = init_chat_model("gpt-4o-mini", model_provider="openai")
    llm = AzureChatOpenAI(
        azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"),
        openai_api_version=os.getenv("AZURE_OPENAI_API_VERSION"),
        deployment_name="o1",
        api_key=os.getenv("AZURE_OPENAI_API_KEY"),
    )

    structured_llm = llm.with_structured_output(ParaphrasedSentencesList)

    prompt_template = PromptTemplate.from_template(construct_prompt_template())
    llm_chained = prompt_template | structured_llm
    return llm_chained

def paraphrase_query(llm, query: str, role: str, dataset_schema: str, cache: Dict[str, ParaphrasedSentencesList] = {}, only_cached = False) -> Tuple[ParaphrasedSentencesList, bool]:
    if query in cache:
        # print('FROM CACHE')
        return cache[query], True
    if only_cached:
        not_paraphrased = ParaphrasedSentence(
            paraphrasedSentence=query,
            formality=-1,
            expertise=-1
        )
        response = ParaphrasedSentencesList(
            sentences=[not_paraphrased]
        )
        return response, True
    
    response = llm.invoke({
        "sentence": query,
        "role": role,
        "dataset_schema": dataset_schema,
        "dim1_1": "Colloquial",
        "dim1_5": "Standard",
        "dim2_1": "Non-technical",
        "dim2_5": "Technical"
    })
    cache[query] = response
    return response, False