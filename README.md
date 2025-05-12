--
dataset_info:

- config_name: dqvis
  features:
  - name: query
    dtype: string
  - name: expertise
    dtype: int64
  - name: formality
    dtype: int64
  - name: query_template
    dtype: string
  - name: constraints
    dtype: string
  - name: spec_template
    dtype: string
  - name: query_type
    dtype: string
  - name: creation_method
    dtype: string
  - name: query_base
    dtype: string
  - name: spec
    dtype: string
  - name: solution
    dtype: string
  - name: dataset_schema
    dtype: string
    splits:
  - name: train
    num_bytes: 11565605773
    num_examples: 1075190
    download_size: 94286825
    dataset_size: 11565605773
- config_name: reviewed
  features:
  - name: testing
    dtype: string
  - name: development
    dtype: string
  - name: process
    dtype: string
    splits:
  - name: train
    num_bytes: 78
    num_examples: 3
    download_size: 1440
    dataset_size: 78
    configs:
- config_name: dqvis
  data_files:
  - split: train
    path: dqvis/train-\*
- config_name: reviewed
  data_files:
  - split: train
    path: reviewed/train-\*

# udi-training-data

This repository contains code for generating training data consisting of natural language prompts and responses in the form of a [Universal Discovery Interface](https://github.com/hms-dbmi/udi-grammar) (UDI) specification.

The overall pipeline can be run from `main.pynb` and consists of a few high-level steps.

1. **Template Generation** will create questions and specifications with placeholders for entities and fields as well as constraints for those entities/fields.
2. **Schema Generation** will create dataset schemas based on provided datasets.
3. **Template Expansion** will expand the template questions/specifications given the provided schemas for all possibilities that satify the constraints.
4. **Paraphraser** will use an LLM framework to paraphrase input questions to cover different styles of expertise and formality in the input. _Note:_ this is currently a placeholder and not implemented yet.
5. **Export** The data will export the data as a list of data points, each data point will have the following attributes.

- `query_template`: The original query template with <E> and <F> placeholders.
- `constraints`: The list of constraints that data entities and fields must satisfy.
- `spec_template`: The template UDI specification with <E> and <F> placeholders.
- `query_type`: The form of the query, either `question` or `utterance`.
- `creation_method`: Will always output `template` from this script.
- `query_base`: The query with placeholders satisfied. e.g. 'donors' and 'sex' instead of <E> and <F>.
- `spec`: The UDI's specification with entities and fields satisfied.
- `dataset_schema`: The dataset schema name.
- `query`: The paraphrased version of `query_base`
- `expertise`: The expertise score [1-5] of the paraphrased query.
- `formality`: The formality score [1-5] of the paraphrased query.
