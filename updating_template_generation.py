import pandas as pd
#from udi_grammar_py import Chart, Op, rolling
from enum import Enum
#import gosling as gos

class QueryType(Enum):
    QUESTION = "question"
    UTTERANCE = "utterance"

class ChartType(Enum):
    #SCATTERPLOT = "scatterplot"
    BARCHART = "barchart"
    #GROUPED_BAR = "stacked_bar"
    #STACKED_BAR = "stacked_bar"
    #NORMALIZED_BAR = "stacked_bar"
    #CIRCULAR = "circular"
    #TABLE = "table"
    #LINE = "line"
    #AREA = "area"
    #GROUPED_LINE = "grouped_line"
    #GROUPED_AREA = "grouped_area"
    #GROUPED_SCATTER = "grouped_scatter"
    #HEATMAP = "heatmap"
    #HISTOGRAM = "histogram"
    #DOT = "dot"
    #GROUPED_DOT = "grouped_dot"


def add_row(df, query_template, spec, constraints, query_type: QueryType, chart_type: ChartType):
    spec_key_count = get_total_key_count(spec.to_dict())
    if spec_key_count <= 12:
        complexity = "simple"
    elif spec_key_count <= 24:
        complexity = "medium"
    elif spec_key_count <= 36:
        complexity = "complex"
    else:
        complexity = "extra complex"
    df.loc[len(df)] = {
        "query_template": query_template,
        "constraints": constraints,
        "spec_template": spec.to_json(),
        "query_type": query_type.value,
        "creation_method": "template",
        "chart_type": chart_type.value,
        "chart_complexity": complexity,
        "spec_key_count": spec_key_count
    }
    return df

def get_total_key_count(nested_dict):
    if isinstance(nested_dict, dict):
        return sum(get_total_key_count(value) for value in nested_dict.values())
    elif isinstance(nested_dict, list):
        return sum(get_total_key_count(item) for item in nested_dict)
    else:
        return 1

def generate():
    df = pd.DataFrame(
        columns=[
            "query_template",
            "constraints",
            "spec_template",
            "query_type",
            "creation_method",
            "chart_type",
            "chart_complexity",
            "spec_key_count",
        ]
    )

    # Define recurring constraints
    overlap = "F1['name'] in F2['udi:overlapping_fields'] or F2['udi:overlapping_fields'] == 'all'"

    # Example query: Where are point mutations in chromosome 1?
    # Visual: dots overlaid on genome track.

    '''
    Constraints:
        S is some genomic data --> ie., sample
    '''
    df = add_row(
        df,
        query_template="Where are <F:p.q> in <S>?",
        spec=(
            {
                'mark':'point',
                'x':'position:G',
                'y':'peak:Q',
            }
        ),
        constraints=[
            # show a max of 1000 points
            '<F:p.q>.c < 1000',
        ],
        query_type=QueryType.QUESTION,
        chart_type=ChartType.POINT,
    )

    df = add_row(
        df,
        # can represent with pipe character1
        query_template="Is the <F:p.q|s.q> at <L> a peak or a valley?",
        spec=(
            { 
             'mark':'line',
             'x':'position:G',
             'y':'peak:Q'
            }
        ),
        constraints=[
            # location
            'F.c > 20' #have at least 20 marks to make a line 
            
        ],
        query_type=QueryType.QUESTION,
        chart_type=ChartType.POINT,
    )

    return df


if __name__ == "__main__":
    df = generate()
    print(df.head())
