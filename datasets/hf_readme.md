**DQVis** is a large-scale dataset designed to support research in natural language queries for data visualization. It consists of 1.08 million natural language queries of biomedical data portal datasets paired with structured visualization specifications.

The dataset also includes optional user reviews and multi-step interaction links for studying more complex workflows.

---

## 📦 Repository Contents

### `dqvis`

A DataFrame (`test`) containing **1.08 million rows** of query-visualization pairs.

#### Columns:

- `query`: The natural language query, that has been paraphrased form query_base.
- `query_base`: A query with entities and fields resolved from query_template.
- `query_template`: Abstract question with placeholders for entity and field names.
- `spec_template`: Template for the visualization spec.
- `spec`: A visualization specification (defined by UDIGrammarSchema.json).
- `expertise`: The expertise of the paraphrased query between 1-5.
- `formality`: The formality of the paraphrased query between 1-5.
- `constraints`: Constraints that limit how the query_template is reified into query_base.
- `query_type`: Type of query (question|utterance).
- `creation_method`: How the query/spec pair was created (template).
- `solution`: A nested object that contains the entities and fields that resolved the query_template into query_base.
- `dataset_schema`: A reference to the schema of the dataset being queried. Matches `udi:name` in dataset_schema_list.json
- `chart_type`: The type of chart specified (scatterplot | barchart | stacked_bar | stacked_bar | stacked_bar | circular | table | line | area | grouped_line | grouped_area | grouped_scatter | heatmap | histogram | dot | grouped_dot).
- `chart_complexity`: A value representing the complexity of the chart (simple|medium|complex|extra complex).
- `spec_key_count`: The number of keys present in the `spec` field, which is used to calculate the `chart_complexity`.

---

### `reviews.json`

A version of the dataset with additional human reviews.

- Similar schema to `dqvis` but in json format. In addition
- `reviewer`: A uuid to identify the different reviewers.
- `review_status`: The score for the data point (good|improve|bad)
- `review_categories`: A list of predefined issues (Bad Question|Malformed Visualization|Question Not Answered|Other) or "[null]".

---

### `UDIGrammarSchema.json`

Defines the formal **grammar schema** used for the `spec` field. This enables downstream systems to parse, validate, and generate visualization specs programmatically.

---

### `dataset_schema_list.json`

A JSON list of dataset schemas used across the DQVis entries. Each schema defines the structure, field types, and entity relationships via foreignKey constraints.

---

---

### `data_packages/`

The folder containing the data referenced by the dataset_schema_list.json. `udi:name` for a single data package will match the name of the subfolder inside `data_packages`

---

### `multi_step_links.json`

Links between entries in `dqvis/` that can be grouped into **multi-turn or multi-step interactions**, useful for studying dialog-based or iterative visualization systems.

### `multi_step_data.csv`

Example data generated with the multi_step_links.json. Contains the following columns:

- `D1_query`: The query text for dataset 1.
- `D2_query`: The query text for dataset 2.
- `expertise`: The level of expertise required or associated with the queries.
- `formality`: The degree of formality in the queries.
- `template_start`: The starting template used for generating or structuring the queries.
- `template_end`: The ending template used for generating or structuring the queries.
- `D1_expertise`: The expertise level specific to dataset 1.
- `D2_expertise`: The expertise level specific to dataset 2.
- `D1_formality`: The formality level specific to dataset 1.
- `D2_formality`: The formality level specific to dataset 2.
- `D1_query_template`: The template used for generating queries in dataset 1.
- `D2_query_template`: The template used for generating queries in dataset 2.
- `D1_constraints`: Constraints or limitations applied to dataset 1 queries.
- `D2_constraints`: Constraints or limitations applied to dataset 2 queries.
- `D1_spec_template`: The specification template for dataset 1.
- `D2_spec_template`: The specification template for dataset 2.
- `D1_query_type`: The type or category of queries in dataset 1.
- `D2_query_type`: The type or category of queries in dataset 2.
- `D1_creation_method`: The method used to create or generate dataset 1 queries.
- `D2_creation_method`: The method used to create or generate dataset 2 queries.
- `D1_query_base`: The base or foundational query for dataset 1.
- `D2_query_base`: The base or foundational query for dataset 2.
- `D1_spec`: The specification details for dataset 1.
- `D2_spec`: The specification details for dataset 2.
- `D1_solution`: The solution or expected output for dataset 1 queries.
- `D2_solution`: The solution or expected output for dataset 2 queries.
- `D1_dataset_schema`: The schema or structure of dataset 1.
- `D2_dataset_schema`: The schema or structure of dataset 2.

---

## 🛠️ Usage Recipes

### Load the Main Dataset

```python
import pandas as pd
from datasets import load_dataset
dataset = load_dataset(f"HIDIVE/DQVis")
df = dataset['train'].to_pandas()
print(df.shape)
# output: (1075190, 15)
```

### Load Dataset Schemas

```python
import json
from huggingface_hub import hf_hub_download

# download the dataset schema list
dataset_schemas = hf_hub_download(
    repo_id="HIDIVE/DQVis",
    filename="dataset_schema_list.json",
    repo_type="dataset"
)

# combine with the dqvis df to place the full dataset schema into `dataset_schema_value` column.
with open(dataset_schemas, "r") as f:
    dataset_schema_list = json.load(f)
    dataset_schema_map = {schema["udi:name"]: schema for schema in dataset_schema_list}
    df['dataset_schema_value'] = df['dataset_schema'].map(dataset_schema_map)
```

### Load Multi-step Interaction Links

```python
import json
from huggingface_hub import hf_hub_download

dataset_schemas = hf_hub_download(
    repo_id="HIDIVE/DQVis",
    filename="multi_step_links.json",
    repo_type="dataset"
)

with open(dataset_schemas) as f:
    multi_step_links = json.load(f)

```

### Load Multi-step Data

```python
from huggingface_hub import hf_hub_download

dataset_schemas = hf_hub_download(
    repo_id="HIDIVE/DQVis",
    filename="multi_step_data.csv",
    repo_type="dataset"
)

multi_step_df = pd.read_csv(dataset_schemas)
print(multi_step_df.shape)
# output: (11448, 29)
```

<!-- ### Placeholder: Get the subset query_base table

```python
# TODO:
```

### Placeholder: Get the subset query_template table

```python
# TODO:
``` -->

---

<!-- ## 📚 Citation

_TODO: Add a citation if you plan to publish or release a paper._

--- -->

## 🔗 Related Project GitHub Links

- [Data Creation Framework (DQVis-Generation)](https://github.com/hms-dbmi/DQVis-Generation)
- [Data Review Interface (DQVis-review)](https://github.com/hms-dbmi/DQVis-review)
- [Visualization Rendering Library (udi-grammar)](https://github.com/hms-dbmi/udi-grammar)

## 📝 Changelog

### Initial Release

- corresponds to version: 0.0.24 of the udi-toolkit: https://www.npmjs.com/package/udi-toolkit
- Added the `dqvis` dataset with 1.08 million query-visualization pairs.
- Included `reviews.json` dataset with user review metadata.
- Provided `UDIGrammarSchema.json` for visualization spec grammar.
- Added `dataset_schema_list.json` for dataset schema definitions.
- Added `data_packages/` folder of data packages.
- Introduced `multi_step_links.json` and `multi_step_data.csv` for multi-step interaction studies.

---
