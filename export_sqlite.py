import sqlite3
import json

def export(db_path, json_path):
    """
    Load data from a JSON file into an SQLite database.

    Args:
        db_path (str): Path to the SQLite database file.
        json_path (str): Path to the JSON file containing the data.
    """

    text_columns = [
        "query_template",
        "constraints",
        "spec_template",
        "query_type",
        "creation_method",
        "query_base",
        "spec",
        "solution",
        "dataset_schema",
        "query",
    ]

    number_columns = [
        "expertise",
        "formality",
    ]

    all_columns = text_columns + number_columns

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    # Define table schema
    columns = ["id INTEGER PRIMARY KEY"] + \
              ["combined_id TEXT UNIQUE"] + \
              ["template_id INTEGER"] + \
              ["expanded_id INTEGER"] + \
              ["paraphrased_id INTEGER"] + \
              [f"{col} TEXT" for col in text_columns] + \
              [f"{col} REAL" for col in number_columns]

    create_table_query = f"CREATE TABLE data ({', '.join(columns)});"
    cur.execute(create_table_query)

    # Load data from JSON file
    with open(json_path, "r") as f:
        data = json.load(f)
        template_ID = 0
        expanded_ID = 0
        paraphrased_ID = 0
        for index, row in enumerate(data):
            row['constraints'] = json.dumps(row.get('constraints', None))
            row['solution'] = json.dumps(row.get('solution', None))
            # Print basic loading bar
            if index % 1000 == 0:
                print(f"Loaded {index} rows of {len(data)}", end="\r")

            if index > 0:
                if row["query_template"] != data[index - 1]["query_template"] or row["constraints"] != data[index - 1]["constraints"]:
                    template_ID += 1
                    expanded_ID = 0
                    paraphrased_ID = 0
                elif row["query_base"] != data[index - 1]["query_base"] or row["dataset_schema"] != data[index - 1]["dataset_schema"]:
                    expanded_ID += 1
                    paraphrased_ID = 0
                else:
                    paraphrased_ID += 1

            combined_ID = f"{template_ID}_{expanded_ID}_{paraphrased_ID}"

            values = [index, combined_ID, template_ID, expanded_ID, paraphrased_ID] + [
                row.get(col, None)
                for col in all_columns
            ]
            placeholders = ", ".join(["?"] * len(values))
            execute_command = f"INSERT INTO data (id, combined_id, template_id, expanded_id, paraphrased_id, {', '.join(all_columns)}) VALUES ({placeholders});"
            cur.execute(execute_command, values)

    # Create index
    index_template_expanded = "CREATE INDEX idx_template_expanded ON data(template_id, expanded_id);"
    cur.execute(index_template_expanded)

    conn.commit()
    conn.close()
