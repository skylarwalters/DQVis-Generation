import json
import os
import sys
from datasets import load_dataset, Dataset, DatasetDict, load_from_disk
from huggingface_hub import HfApi, HfFolder


def display_progress(df, index):
    total_rows = len(df)
    progress = (index / total_rows) * 100
    bar_length = 30
    filled_length = int(bar_length * index // total_rows)
    bar = '=' * filled_length + '-' * (bar_length - filled_length)
    sys.stdout.write(f"\rFormat For fintuning row {index}/{total_rows} [{bar}] {progress:.2f}%")
    sys.stdout.flush()

def convert(df, dataset_schema_list, grammar_schema, output_path, huggingface_path, push_to_hub=False, pretty=False):
    """
    converts the input data frame with expected columnms:
        query - the user query
        spec - the UDI Grammar Specification
        dataset_schema - the key of the relevant dataset schema
    Into a list of "converstions" each conversation is a list of objects with the following keys:
        content - text typed by user/assistant or sent to chatbot
        role - user | assistant | system
        tool_calls - null | list of tool calls
        tools - list of tools available, typically provided in the first point in the conversation,
                and in follow ups it will be null 
    """
    print('converting to finetuning format')
    dataset_schema_map = {schema["name"]: schema for schema in dataset_schema_list}
    # for each row in the data frame, create a conversation that consists of three messages
    # 1. system prompt, 2. user query, 3. assistant response
    conversations = []
    index = 0
    subsample_every = 1
    max_chunk_size = 50000
    chunk = 0
    for _, row in df.iterrows():
        display_progress(df, index)
        index += 1
        dataset_schema = dataset_schema_map[row["dataset_schema"]]
        system_prompt = create_system_prompt(dataset_schema, grammar_schema)
        user_query = create_user_query(row["query"])
        assistant_response = create_assistant_response(row["spec"], grammar_schema)
        if index % subsample_every == 0:
            conversations.append([system_prompt, user_query, assistant_response])
        if len(conversations) > max_chunk_size:
            print('max chunk size reached, saving to disk')
            # write the conversations to the output path
            # with open(output_path, "w") as output_file:
            #     indent = 2 if pretty else None
            #     json.dump(conversations, output_file, indent=indent)
            # print('done')

            # split converstations into train and test sets
            train_size = len(conversations) - 10
            train_conversations = conversations[:train_size]
            test_conversations = conversations[train_size:]
            save_huggingface_dataset(new_dataset=train_conversations, test_dataset=test_conversations, dataset_path=huggingface_path, push_to_hub=push_to_hub, chunk=chunk)
            chunk += 1
            conversations = []

    # print(f'saving conversation to json: {len(conversations)} conversations')
    # # write the conversations to the output path
    # with open(output_path, "w") as output_file:
    #     indent = 2 if pretty else None
    #     json.dump(conversations, output_file, indent=indent)
    # print('done')

    # split converstations into train and test sets
    train_size = len(conversations) - 10
    train_conversations = conversations[:train_size]
    test_conversations = conversations[train_size:]
    save_huggingface_dataset(new_dataset=train_conversations, test_dataset=test_conversations, dataset_path=huggingface_path, push_to_hub=push_to_hub, chunk=chunk)

    return

def create_system_prompt(dataset_schema, grammar_schema):
    dataset_schema_string = json.dumps(dataset_schema, indent=0)
    return {
        "content": f"You are a helpful assistant that will explore, and analyze datasets with visualizations. The following defines the available datasets:\n{dataset_schema_string}\n Typically, your actions will use the provided functions. You have access to the following functions.",
        "role": "system",
        "tool_calls": json.dumps(None),
        "tools": json.dumps([
            {
                "name": "RenderVisualization",
                "description": "Render a visualization with a provided visualization grammar of graphics style specification.",
                "parameter": {
                    "type": "object",
                    "properties": {
                        "spec": {
                            "type": "object",
                            "description": "The UDI Grammar Specification for the visualization.",
                            "required": True,
                            "properties": grammar_schema
                        }
                    }
                }
            }
        ])
    }


def create_user_query(query):
    return {
        "content": query,
        "role": "user",
        "tool_calls": json.dumps(None),
        "tools": json.dumps(None)
    }

def create_assistant_response(spec, grammar_schema):
    return {
        "content": "",
        "role": "assistant",
        "tool_calls": json.dumps([
            {
                "name": "RenderVisualization",
                "arguments": {
                    "spec": spec
                }
            }
        ]),
        "tools": json.dumps(None)
    }

def save_huggingface_dataset(dataset_path, new_dataset=None, test_dataset=None, push_to_hub=False, chunk=0):
    """
    Save the dataset in a format recognized by Hugging Face's datasets library.


    Args:
        new_dataset (list): List of training examples.
        test_dataset (list): List of test examples.
        dataset_path (str): Path to save the dataset.
    """
    print("saving to huggingface format")
    dataset_path = os.path.join(dataset_path, f"chunk_{chunk}")
    os.makedirs(dataset_path, exist_ok=True)

    if new_dataset is not None:
        print("creating test dataset")
        train_path = os.path.join(dataset_path, "train")
        os.makedirs(train_path, exist_ok=True)
        train_dataset = Dataset.from_dict({"messages": new_dataset})
        print("done")
        train_dataset.save_to_disk(train_path)

    if test_dataset is not None:
        test_path = os.path.join(dataset_path, "test")
        os.makedirs(test_path, exist_ok=True)
        test_dataset = Dataset.from_dict({"messages": test_dataset})
        test_dataset.save_to_disk(test_path)

    print('creating dataset_dict')
    # Save a dataset_dict file for metadata
    if new_dataset is not None and test_dataset is not None:
        dataset_dict = DatasetDict(
            {"train": train_dataset, "test": test_dataset})
    elif new_dataset is not None and test_dataset is None:
        dataset_dict = DatasetDict({"train": train_dataset})
    elif new_dataset is None and test_dataset is not None:
        dataset_dict = DatasetDict({"test": test_dataset})
    print('writing dataset_dict')
    dataset_dict.save_to_disk(dataset_path) 
    print('done')
    if push_to_hub:
        print('uploading to huggingface hub')
        api = HfApi()
        api.upload_folder(folder_path=dataset_path, repo_id=f"agenticx/UDI-VIS-{chunk}", repo_type="dataset")
        print('DONE')