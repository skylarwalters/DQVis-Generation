import os
import sys
from datasets import  Dataset
from huggingface_hub import  upload_file

def display_progress(df, index):
    total_rows = len(df)
    progress = (index / total_rows) * 100
    bar_length = 30
    filled_length = int(bar_length * index // total_rows)
    bar = '=' * filled_length + '-' * (bar_length - filled_length)
    sys.stdout.write(f"\rFormat For fintuning row {index}/{total_rows} [{bar}] {progress:.2f}%")
    sys.stdout.flush()


def save(main_df, reviewed_df, dataset_schema_list_filename, grammar_schema_filename, local_path, repo_id, save_local=False, push_to_hub=False):
    """
    Save DQVis dataset to Hugging Face and or locally.
    """

    # Convert the DataFrame to a Dataset
    main_dataset = Dataset.from_pandas(main_df)
    reviewed_dataset = Dataset.from_pandas(reviewed_df)
    # dataset = DatasetDict({"data": dataset1, "testing_fake_data": dataset2})
    if push_to_hub:
        main_dataset.push_to_hub(repo_id, config_name="dqvis")

        reviewed_dataset.push_to_hub(repo_id, config_name="reviewed")

        upload_file(
            path_or_fileobj=grammar_schema_filename,
            path_in_repo="UDIGrammarSchema.json",
            repo_id=repo_id,
            repo_type="dataset"
        )

        upload_file(
            path_or_fileobj=dataset_schema_list_filename,
            path_in_repo="dataset_schema_list.json",
            repo_id=repo_id,
            repo_type="dataset"
        )

    if save_local:
        main_dataset.save_to_disk(os.path.join(local_path, "dqvis"))
        reviewed_dataset.save_to_disk(os.path.join(local_path, "reviewed"))

