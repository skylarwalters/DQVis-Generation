import json

def convert(df, dataset_schema_list, grammar_schema, output_path, pretty=False):
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

    dataset_schema_map = {schema["name"]: schema for schema in dataset_schema_list}
    # for each row in the data frame, create a conversation that consists of three messages
    # 1. system prompt, 2. user query, 3. assistant response
    conversations = []
    for index, row in df.iterrows():
        dataset_schema = dataset_schema_map[row["dataset_schema"]]
        system_prompt = create_system_prompt(dataset_schema, grammar_schema)
        user_query = create_user_query(row["query"])
        assistant_response = create_assistant_response(row["spec"], grammar_schema)
        conversations.append([system_prompt, user_query, assistant_response])

    # write the conversations to the output path
    with open(output_path, "w") as output_file:
        indent = 2 if pretty else None
        json.dump(conversations, output_file, indent=indent)

    return

def create_system_prompt(dataset_schema, grammar_schema):
    dataset_schema_string = json.dumps(dataset_schema, indent=0)
    return {
        "content": f"You are a helpful assistant that will explore, and analyze datasets with visualizations. The following defines the available datasets:\n{dataset_schema_string}\n Typically, your actions will use the provided functions. You have access to the following functions.",
        "role": "system",
        "tool_calls": None,
        "tools": [
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
        ]
    }


def create_user_query(query):
    return {
        "content": query,
        "role": "user",
        "tool_calls": None,
        "tools": None
    }

def create_assistant_response(spec, grammar_schema):
    return {
        "content": "",
        "role": "assistant",
        "tool_calls": [
            {
                "tool": "RenderVisualization",
                "parameters": {
                    "spec": spec
                }
            }
        ],
        "tools": None
    }