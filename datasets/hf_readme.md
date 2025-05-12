# DQVis: A Dataset of Natural Language Queries and Visualization Specifications

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

---

### `reviewed`

A version of the dataset with additional human reviews.

- Similar schema to `dqvis/`
- Includes columns with **user review metadata** (e.g., clarity, correctness, usefulness).
- _Note: This file is incomplete. Final schema to be documented upon release._

---

### `UDIGrammarSchema.json`

Defines the formal **grammar schema** used for the `spec` field. This enables downstream systems to parse, validate, and generate visualization specs programmatically.

---

### `dataset_schema_list.json`

A JSON list of dataset schemas used across the DQVis entries. Each schema defines the structure, field types, and entity relationships via foreignKey constraints.

---

### `multi_step_links.json`

Links between entries in `dqvis/` that can be grouped into **multi-turn or multi-step interactions**, useful for studying dialog-based or iterative visualization systems.

---

## 🛠️ Usage Recipes

### Load the Main Dataset

```python
import pandas as pd
from datasets import load_dataset
dataset = load_dataset(f"HIDIVE/DQVis", name="dqvis")
df = dataset['train'].to_pandas()
print(df.shape)
# output: 1075190, 12)
```

### Load Dataset Schemas

```python
from huggingface_hub import hf_hub_download

# download the dataset schema list
dataset_schemas = hf_hub_download(
    repo_id="HIDIVE/DQVis",
    filename="dataset_schema_list.json",  # use exact filename
    repo_type="dataset"
)

# combine with the dqvis df to place the full dataset schema into `dataset_schema_value` column.
with open(dataset_schemas, "r") as f:
    dataset_schema_list = json.load(f)
    dataset_schema_map = {schema["udi:name"]: schema for schema in dataset_schema_list}
    df['dataset_schema_value'] = df['dataset_schema'].map(dataset_schema_map)
```

<!-- ### Placeholder: Load Multi-step Interaction Links

```python
with open('multi_step_links.json') as f:
    multi_step_links = json.load(f)

# Example: link a sequence of related rows for a multi-turn use case
``` -->

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

- [Data Creation Framework (udi-training-data)](https://github.com/hms-dbmi/udi-training-data)
- [Data Review Interface (udi-dataset-review)](https://github.com/hms-dbmi/udi-dataset-review)
- [Visualization Rendering Library (udi-grammar)](https://github.com/hms-dbmi/udi-grammar)
