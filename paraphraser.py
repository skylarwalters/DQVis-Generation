import pickle
import sys
from typing import Dict, Optional, Tuple
import pandas as pd
import os
from langchain.chat_models import init_chat_model
from langchain_core.prompts import PromptTemplate
from pydantic import BaseModel, Field

from dotenv import load_dotenv
from rich import print
load_dotenv()

CACHE_FILE = "./datasets/paraphrase_cache.pkl"


def paraphrase(df, only_cached: Optional[bool] = False) -> pd.DataFrame:
    '''
    Input dataframe will have the following relevant columnns:
    - query_base: the original query
    
    Output dataframe will have the following relevant columns:
    - query: the paraphrased query
    - expertise: the expertise score of the paraphrased query
    - formality: the formality score of the paraphrased query
    '''

    cache = get_cache()
    new_rows = []
    index = 0
    llm = init_llm()
    cache_interval = 10
    interval_index = 0
    for _, row in df.iterrows():
        index += 1
        display_progress(df, index)
        query_base = row["query_base"]
        response, is_cached = paraphrase_query(llm, query_base, cache, only_cached)
        if response:
            for sentence in response.sentences:
                new_data = {
                    "query": sentence.paraphrasedSentence,
                    "expertise": sentence.expertise,
                    "formality": sentence.formality
                }
                new_data.update(row)
                new_rows.append(new_data)
        if not is_cached:
            interval_index += 1
            if interval_index % cache_interval == 0:
                update_cache(cache)

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


class ParaphrasedSententence(BaseModel):
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
    sentences: list[ParaphrasedSententence] = Field(
        default_factory=list,
        description="A list of paraphrased sentences with their metadata."
    )

def construct_prompt_template():
    template = '''
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
    llm = init_chat_model("gpt-4o-mini", model_provider="openai")
    structured_llm = llm.with_structured_output(ParaphrasedSentencesList)

    prompt_template = PromptTemplate.from_template(construct_prompt_template())
    llm_chained = prompt_template | structured_llm
    return llm_chained

def paraphrase_query(llm, query: str, cache: Dict[str, ParaphrasedSentencesList] = {}, only_cached = False) -> Tuple[ParaphrasedSentencesList, bool]:
    if query in cache:
        # print('FROM CACHE')
        return cache[query], True
    if only_cached:
        not_paraphrased = ParaphrasedSententence(
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
        "dim1_1": "Colloquial",
        "dim1_5": "Standard",
        "dim2_1": "Non-technical",
        "dim2_5": "Technical"
    })
    cache[query] = response
    return response, False