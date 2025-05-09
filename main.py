from udi_grammar_py import Chart, Op, rolling
import pandas as pd
import sys
import template_generation
import process_datapackage
import insert_reference_values
import template_expansion
import paraphraser
import upload_to_huggingface
import export_sqlite
import json

sys.path.append('.')

# default all to False, use command line args to set them to True
UPDATE_SCHEMA = False # Set to True to update the data package schema
SAVE_HUGGINGFACE_LOCAL = False # Saves the data locally in a format similar to the HF upload
UPLOAD_TO_HUGGINGFACE = False # Set to True if you want to upload the training data to Hugging Face
PERFORM_PARAPHRASING = False # paraphrasing is time consuming, so skipping makes it easier to test the rest of the pipeline
ONLY_CACHED = False # if True, only cached data for paraphrasing will be used only matters if PERFORM_PARAPHRASING is True
GENERATE_SQLITE = False # Set to True if you want to export the data to SQLite DB

def main():

    print_header("1. Generate templates")
    df = template_generation.generate()
    template_question_count = df.shape[0]

    # update data schema based on files in ./datasets folder and export updated data packages
    if UPDATE_SCHEMA:
        print('Updating data schema')
        process_datapackage.main()
        insert_reference_values.main()


    print_header("2. Contextualize templates with real entity names and fields")
    # Contextualize the template training data by putting in real entity names and fields if they satisfy the constraints.
    with open('./datasets/output_catalogue.json') as f:
        schema_list = json.load(f)
        df = template_expansion.expand(df, schema_list)

    print_header("3. Paraphrase the contextualized templates")
    # The paraphraser will use LLM to paraphrase the query_base into several options
    expanded_question_count = df.shape[0]
    if PERFORM_PARAPHRASING:
        if ONLY_CACHED:
            print('Using only cached data for paraphrasing, will not call LLM.')
        df = paraphraser.paraphrase(df, schema_list, ONLY_CACHED)
    else:
        print('Skipping paraphrasing, using only the original query_base.')
        df['query'] = df['query_base']
        df['expertise'] = -1
        df['formality'] = -1

    paraphrased_question_count = df.shape[0]

    # Sanity Check output
    print_header("4. Sanity Check output dimensions")
    print(f"Generated {template_question_count:,} templates and expanded to {expanded_question_count:,} questions and paraphrased to {paraphrased_question_count:,}.")

    print_header("5. Export data")
    print("exporting ./out/training_data.json...")
    df.to_json('./out/training_data.json', orient='records')


    # ## Upload data to Huggging Face 
    if UPLOAD_TO_HUGGINGFACE or SAVE_HUGGINGFACE_LOCAL:
        if UPLOAD_TO_HUGGINGFACE and SAVE_HUGGINGFACE_LOCAL:
            print('6. Uploading to Hugging Face and saving locally')
        elif UPLOAD_TO_HUGGINGFACE:
            print_header('6. Uploading to Hugging Face')
        elif SAVE_HUGGINGFACE_LOCAL:
            print_header('6. Saving data locally in format for Hugging Face')

        placeholder_df = pd.DataFrame([
            {'testing': 'test1', 'development': 'dev1', 'process': 'proc1'},
            {'testing': 'test2', 'development': 'dev2', 'process': 'proc2'},
            {'testing': 'test3', 'development': 'dev3', 'process': 'proc3'}
        ])
        upload_to_huggingface.save(
            df,
            placeholder_df,
            './datasets/output_catalogue.json',
            './datasets/UDIGrammarSchema.json',
            './out/huggingface/',
            'DevLan/DQVis',
            save_local=SAVE_HUGGINGFACE_LOCAL,
            push_to_hub=UPLOAD_TO_HUGGINGFACE
    )


    if GENERATE_SQLITE:
        print_header('7. Exporting data to SQLite DB')
        # ## Export as SQLite DB
        export_sqlite.export('./out/database.sqlite', './out/training_data.json',)

def print_header(message):
    print("\n" + "#" * 80)
    print("| " + message + " " * (77 - len(message)) + "|")
    print("#" * 80)

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Generate training data for UDI Grammar')
    parser.add_argument('--update_schema', action='store_true', help='Update the data package schema based on files in ./datasets folder')
    parser.add_argument('--upload', action='store_true', help='Upload the training data to Hugging Face')
    parser.add_argument('--hf_local', action='store_true', help='Save the training data locally in a format similar to the HF upload')
    parser.add_argument('--paraphrase', action='store_true', help='Perform paraphrasing')
    parser.add_argument('--only_cached', action='store_true', help='Use only cached data for paraphrasing')
    parser.add_argument('--generate_sqlite', action='store_true', help='Export the data to SQLite DB')
    args = parser.parse_args()
    UPDATE_SCHEMA = args.update_schema
    UPLOAD_TO_HUGGINGFACE = args.upload
    SAVE_HUGGINGFACE_LOCAL = args.hf_local
    PERFORM_PARAPHRASING = args.paraphrase
    GENERATE_SQLITE = args.generate_sqlite
    ONLY_CACHED = args.only_cached
    main()