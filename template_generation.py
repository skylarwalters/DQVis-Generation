import pandas as pd
#from udi_grammar_py import Chart, Op, rolling
from enum import Enum
import json
#import gosling as gos

class QueryType(Enum):
    QUESTION = "question"
    UTTERANCE = "utterance"

class ChartType(Enum):
    #SCATTERPLOT = "scatterplot"
    BARCHART = "barchart"
    POINT = 'point'
    LINE = 'line',
    CONNECTIVITY = 'connectivity',
    COMPARATIVE_STACK = 'comparative_stack',
    RECTANGLE = 'rectangle'
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
    spec_key_count = get_total_key_count(spec)
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
        "spec_template": json.dumps(spec),
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
    sample_assembly = "S1['udi:assembly] == S2['udi:assembly]"
    
    entities_have_same_sample="E1['sample'] == E2['sample']"
    fields_have_same_type="F1['udi:data_type'] == F2['udi:data_type']"
    different_genes="L1['gene'] != L2['gene']"
    different_samples="S1['sample'] != S2['sample']"
    
    
    
    # USE THIS ONE !! YAYYYYYYY
    df = add_row(
        df,
        query_template="How do the <E> appear?",
        spec=({
            "tracks":[{
                "title": "Copy Number Variants",
                "data": {
                    "separator": "\t",
                    "url": "<E.url>",
                    "type": "csv",
                    "chromosomeField": "<E.chr1>",
                    "genomicFields": ["<E.start>", "<E.end>"]
                },
                "mark": "rect",
                "x": {
                    "field": "<E.start>",
                    "type": "genomic"
                },
                "xe": {
                    "field": "<E.end>",
                    "type": "genomic"
                },
                "y": {
                    "field": "total_cn",
                    "type": "quantitative",
                    "axis": "right",
                    "range": [10, 50]
                  },
                "width": 1400,
                "height": 60
            }
            ]}
        ),
        constraints=[
            "E['use'] == 'cna'",
        ],
        query_type=QueryType.UTTERANCE,
        chart_type=ChartType.RECTANGLE,
    )
        
    # ---------------------------------------------------------
    # Mapping entities
    # ---------------------------------------------------------
    
    
    df = add_row(
        df,
        query_template="What is the <E> data?",
        spec=(
            {
                "title": "Structural variants on whole genome",
                "tracks": [
                    {
                    "data": {
                        "url": "<E.url>",
                        "type": "csv",
                        "separator":"\t",
                        "genomicFieldsToConvert": [
                            {"chromosomeField": "<E.chr1>", "genomicFields":["<E.start1>", "<E.end1>"]},
                            {"chromosomeField": "<E.chr2>", "genomicFields":["<E.start2>", "<E.end2>"]},
                        ]
                    },
                    "mark": "withinLink",
                    "x": {"field": "<E.start1>", "type": "genomic"},
                    "xe": {"field": "<E.end2>", "type": "genomic"},
                    "strokeWidth": {"value": 1},
                    "opacity": {"value": 0.7}
                    }
                ]
            }
        ),
        constraints=[
            "E['use'] == 'sv'",
        ],
        query_type=QueryType.UTTERANCE,
        chart_type=ChartType.CONNECTIVITY,
    )
    
    df = add_row(
        df,
        query_template="What are <E> at <L>?",
        spec=(
            {
            "title": "Structural variants at <L>",
            "tracks": [
                {
                "data": {
                    "url": "<E.url>",
                    "type": "csv",
                    "separator": "\t",
                    "genomicFieldsToConvert": [
                    {
                        "chromosomeField": "<E.chr1>",
                        "genomicFields": ["<E.start1>", "<E.end1>"]
                    },
                    {
                        "chromosomeField": "<E.chr2>",
                        "genomicFields": ["<E.start2>", "<E.end2>"]
                    }
                    ]
                },
                "mark": "withinLink",
                "x": {
                    "field": "<E.start1>",
                    "type": "genomic",
                    "domain": {
                    "chromosome": "<L.geneChr>",
                    "interval": ["<L.geneStart>", "<L.geneEnd>"]
                    }
                },
                "xe": {
                    "field": "<E.end2>",
                    "type": "genomic"
                },
                "strokeWidth": {
                    "value": 1
                },
                "opacity": {
                    "value": 0.7
                }
                }
            ]
            }
        ),
        constraints=[
            "E['use'] == 'sv'",
        ],
        query_type=QueryType.UTTERANCE,
        chart_type=ChartType.CONNECTIVITY,
    )
    

    
    
    df = add_row(
        df,
        query_template="Map the <E> data at <L>.",
        spec=({
            "tracks":[{
                "title": "Copy Number Variants",
                "data": {
                    "separator": "\t",
                    "url": "<E.url>",
                    "type": "csv",
                    "chromosomeField": "<E.chr1>",
                    "genomicFields": ["<E.start>", "<E.end>"]
                },
                "mark": "rect",
                "x": {
                    "field": "<E.start>",
                    "type": "genomic",
                    "domain": {
                    "chromosome": "<L.geneChr>",
                    "interval": ["<L.geneStart>", "<L.geneEnd>"]
                    }
                },
                "xe": {
                    "field": "<E.end>",
                    "type": "genomic"
                },
                "width": 1400,
                "height": 60
            }
            ]}
        ),
        constraints=[
            "E['use'] == 'cna'",
        ],
        query_type=QueryType.UTTERANCE,
        chart_type=ChartType.RECTANGLE,
    )
    
    df = add_row(
        df,
        query_template="Map the <E> data at <L>.",
        spec=({
            "tracks":[{
                "title": "Copy Number Variants",
                "data": {
                    "separator": "\t",
                    "url": "<E.url>",
                    "type": "csv",
                    "chromosomeField": "<E.chr1>",
                    "genomicFields": ["<E.start>", "<E.end>"]
                },
                "mark": "rect",
                "x": {
                    "field": "<E.start>",
                    "type": "genomic",
                    "domain": {
                    "chromosome": "<L.geneChr>",
                    "interval": ["<L.geneStart>", "<L.geneEnd>"]
                    }
                },
                "xe": {
                    "field": "<E.end>",
                    "type": "genomic"
                },
                "width": 1400,
                "height": 60
            }
            ]}
        ),
        constraints=[
            "E['use'] == 'cna'",
        ],
        query_type=QueryType.UTTERANCE,
        chart_type=ChartType.RECTANGLE,
    )
    # point mutations + indels
    df = add_row(
        df,
        query_template="Map the <E> data.",
        spec=({
            "title": "Point Mutations",
            'tracks': [{
                
                "data": {
                    "type": "vcf",
                    "url": "<E.url>",
                    "indexUrl": "<E.index-file>",
                },
                "mark": "point",
                "x": {
                    "field": "POS",
                    "type": "genomic"
                },
                "tooltip": [
                    {
                    "field": "POS",
                    "type": "genomic"
                    },
                    {
                    "field": "REF",
                    "type": "nominal"
                    },
                    {
                    "field": "ALT",
                    "type": "nominal"
                    }
                ]
            }]}
        ),
        constraints=[
            "E['use'] == 'point-mutation'",
        ],
        query_type=QueryType.UTTERANCE,
        chart_type=ChartType.RECTANGLE,
    )
    
    df = add_row(
        df,
        query_template="Map the <E> data at <L>.",
        spec=({
            "title": "Point Mutations",
            'tracks': [{
                
                "data": {
                    "type": "vcf",
                    "url": "<E.url>",
                    "indexUrl": "<E.index-file>",
                },
                "mark": "point",
                "x": {
                    "field": "POS",
                    "type": "genomic",
                    "domain": {
                        "chromosome": "<L.geneChr>",
                        "interval": ["<L.geneStart>", "<L.geneEnd>"]
                    }
                },
                "tooltip": [
                    {
                    "field": "POS",
                    "type": "genomic"
                    },
                    {
                    "field": "REF",
                    "type": "nominal"
                    },
                    {
                    "field": "ALT",
                    "type": "nominal"
                    }
                ]
                }],
            }
        ),
        constraints=[
            "E['use'] == 'point-mutation'",
        ],
        query_type=QueryType.UTTERANCE,
        chart_type=ChartType.RECTANGLE,
    )
    
    
    # df = add_row(
    #     df,
    #     query_template="Map the <E> data.",
    #     spec=({
    #         "title": "Indels",
    #         'tracks': [{
                
    #             "data": {
    #                 "type": "vcf",
    #                 "url": "<E.url>",
    #                 "indexUrl": "<E.index-file>",
    #             },
    #             "dataTransform": [
    #                 {
    #                     "type": "concat",
    #                     "fields": [
    #                         "REF",
    #                         "ALT"
    #                     ],
    #                     "separator": " → ",
    #                     "newField": "LAB"
    #                 },
    #                 {
    #                     "type": "replace",
    #                     "field": "MUTTYPE",
    #                     "replace": [
    #                         {
    #                         "from": "insertion",
    #                         "to": "Insertion"
    #                         },
    #                         {
    #                         "from": "deletion",
    #                         "to": "Deletion"
    #                         }
    #                     ],
    #                     "newField": "MUTTYPE"
    #                 }
    #             ],
    #             "mark": "rect",
    #             "x": {
    #                 "field": "POS",
    #                 "type": "genomic",
                    
    #             },
               
    #             "color":{
    #                 "field": "MUTTYPE",
    #                 "type": "nominal",
    #                 "legend": True,
    #                 "domain": [
    #                     "Insertion",
    #                     "Deletion"
    #                 ]
    #             },
    #             "tooltip": [
    #                 {
    #                 "field": "POS",
    #                 "type": "genomic"
    #                 },
    #                 {
    #                 "field": "REF",
    #                 "type": "nominal"
    #                 },
    #                 {
    #                 "field": "ALT",
    #                 "type": "nominal"
    #                 }
    #             ]}],
    #         }
    #     ),
    #     constraints=[
    #         "E['use'] == 'indel'",
    #     ],
    #     query_type=QueryType.UTTERANCE,
    #     chart_type=ChartType.RECTANGLE,
    # )
    
    
    # df = add_row(
    #     df,
    #     query_template="Map the <E> data at <L>.",
    #     spec=({
    #         "title": "Indels",
    #         'tracks': [{
                
    #             "data": {
    #                 "type": "vcf",
    #                 "url": "<E.url>",
    #                 "indexUrl": "<E.index-file>",
    #             },
    #             "dataTransform": [
    #                 {
    #                     "type": "concat",
    #                     "fields": [
    #                         "REF",
    #                         "ALT"
    #                     ],
    #                     "separator": " → ",
    #                     "newField": "LAB"
    #                 },
    #                 {
    #                     "type": "replace",
    #                     "field": "MUTTYPE",
    #                     "replace": [
    #                         {
    #                         "from": "insertion",
    #                         "to": "Insertion"
    #                         },
    #                         {
    #                         "from": "deletion",
    #                         "to": "Deletion"
    #                         }
    #                     ],
    #                     "newField": "MUTTYPE"
    #                 }
    #             ],
    #             "mark": "rect",
    #             "x": {
    #                 "field": "POS",
    #                 "type": "genomic",
    #                 "domain": {
    #                     "chromosome": "<L.geneChr>",
    #                     "interval": ["<L.geneStart>", "<L.geneEnd>"]
    #                 }
    #             },
               
    #             "color":{
    #                 "field": "MUTTYPE",
    #                 "type": "nominal",
    #                 "legend": True,
    #                 "domain": [
    #                     "Insertion",
    #                     "Deletion"
    #                 ]
    #             },
    #             "tooltip": [
    #                 {
    #                 "field": "POS",
    #                 "type": "genomic"
    #                 },
    #                 {
    #                 "field": "REF",
    #                 "type": "nominal"
    #                 },
    #                 {
    #                 "field": "ALT",
    #                 "type": "nominal"
    #                 }
    #             ]}],
    #         }
    #     ),
    #     constraints=[
    #         "E['use'] == 'indel'",
    #     ],
    #     query_type=QueryType.UTTERANCE,
    #     chart_type=ChartType.RECTANGLE,
    # )
    
    df = add_row(
        df,
        query_template="What is the <E> of the data?",
        spec=(
            {
                "title": "Bar Graph Using BAM Data",
                "layout": "linear",
                "tracks": [
                    {
                        "data": {
                            "url": "<E.url>",
                            "type": "bam",
                            "indexUrl": "<E.index-file>",
                        },
                    "mark": "bar",
                    "dataTransform": [
                            {"type": "coverage", "startField": "<E.start>", "endField": "<E.end>"}
                        ],
                    "x": {"field": "<E.start>", "type": "genomic"},
                    "xe": {"field": "<E.end>", "type": "genomic"},
                    "y": {"field": "coverage", "type": "quantitative", "axis": "right"},
                    },
                ]     
            }       
        ),
        constraints=[
            "E['use'] == 'coverage'",
        ],
        query_type=QueryType.QUESTION,
        chart_type=ChartType.POINT,
    )
        
    
    df = add_row(
        df,
        query_template="Where are <F:p&q> at <L> for <S>",
        spec=(
            {
                "tracks": [
                    {
                    "data": {
                        "url": "<F.url>",
                        "type":"vcf",
                        "indexUrl":"https://somatic-browser-test.s3.amazonaws.com/PCAWG/Cervix-AdenoCA/b9d1a64e-d445-4174-a5b4-76dd6ea69419.sorted.vcf.gz.tbi",
                        "sampleLength":1000 
                    },
                    "mark": "point",
                    "x": {"field": "<F.field>", "type": "genomic", "axis":"bottom"},
                    }
                ]
            }
        ),
        constraints=[
            "F['field'] == 'POS'",
        ],
        query_type=QueryType.QUESTION,
        chart_type=ChartType.POINT,
    )
    
    
    
    
    
    
    df = add_row(
        df,
        query_template="Where are <F:p&q> at <L> for <S>",
        spec=(
            {
                "tracks": [
                    {
                    "data": {
                        "url": "<F.url>",
                        "type":"vcf",
                        "indexUrl":"https://somatic-browser-test.s3.amazonaws.com/PCAWG/Cervix-AdenoCA/b9d1a64e-d445-4174-a5b4-76dd6ea69419.sorted.vcf.gz.tbi",
                        "sampleLength":1000 
                    },
                    "mark": "point",
                    "x": {"field": "<F.field>", "type": "genomic", "axis":"bottom"},
                    }
                ]
            }
        ),
        constraints=[
            "F['field'] == 'POS'",
        ],
        query_type=QueryType.QUESTION,
        chart_type=ChartType.POINT,
    )
    
    
    # Analytical queries
    
    df = add_row(
        df,
        query_template="What is the frequency of <E> across <S>?",
        spec=(
            {
                "title": "<E> Frequency",
                "layout": "linear",
                "arrangement": "vertical",
                "centerRadius": 0.8,
                "views": [
                    {
                    "tracks": [
                        {
                        "id": "track-1",
                        "data": {
                            "url": "<E.url>",
                            "type": "vcf",
                            "indexUrl": "<E.index-file>",
                        },
                        "dataTransform": [
                            {
                            "type": "coverage",
                            "startField": "POS",
                            "endField": "POS",
                            "newField": "depth"
                            }
                        ],
                        "mark": "bar",
                        "x": {"field": "POS", "type": "genomic", "axis": "top"},
                        "y": {"field": "depth", "type": "quantitative"},
                        "tooltip": [
                            {
                            "field": "POS",
                            "type": "genomic"
                            },
                            {
                            "field": "depth",
                            "type": "quantitative"
                            }
                        ],
                        "opacity": {"value": 1},
                        "width": 600,
                        "height": 130
                        }
                    ]
                    }
                ]
            }
    
    
        ),
        constraints=[
            "E['use'] == 'point-mutation'", 
        ],
        query_type=QueryType.QUESTION,
        chart_type=ChartType.POINT,
    )
    
    df = add_row(
        df,
        query_template="How is <E> distributed on <S>?",
        spec=(
            {
                "title": "<E> Distribution",
                "layout": "linear",
                "arrangement": "vertical",
                "centerRadius": 0.8,
                "views": [
                    {
                    "tracks": [
                        {
                        "id": "track-1",
                        "data": {
                            "url": "<E.url>",
                            "type": "vcf",
                            "indexUrl": "<E.index-file>",
                        },
                        "dataTransform": [
                            {
                            "type": "coverage",
                            "startField": "POS",
                            "endField": "POS",
                            "newField": "depth"
                            }
                        ],
                        "mark": "bar",
                        "x": {"field": "POS", "type": "genomic", "axis": "top"},
                        "y": {"field": "depth", "type": "quantitative"},
                        "tooltip": [
                            {
                            "field": "POS",
                            "type": "genomic"
                            },
                            {
                            "field": "depth",
                            "type": "quantitative"
                            }
                        ],
                        "opacity": {"value": 1},
                        "width": 600,
                        "height": 130
                        }
                    ]
                    }
                ]
            }
    
        ),
        constraints=[
            "E['use'] == 'point-mutation'", 
        ],
        query_type=QueryType.QUESTION,
        chart_type=ChartType.POINT,
    )
    
    df = add_row(
        df,
        query_template="How is <E> distributed on <L>?",
        spec=(
            {
                "title": "<E> Distribution on <L>",
                "layout": "linear",
                "arrangement": "vertical",
                "centerRadius": 0.8,
                "views": [
                    {
                    "tracks": [
                        {
                        "id": "track-1",
                        "data": {
                            "url": "<E.url>",
                            "type": "vcf",
                            "indexUrl": "<E.index-file>",
                        },
                        "dataTransform": [
                            {
                            "type": "coverage",
                            "startField": "POS",
                            "endField": "POS",
                            "newField": "depth"
                            }
                        ],
                        "mark": "bar",
                        "x": {
                            "field": "POS", 
                            "type": "genomic", 
                            "axis": "top",
                            "domain":
                                {
                                    "chromosome": "<L.geneChr>",
                                    "interval": ["<L.geneStart>", "<L.geneEnd>"]
                                }
                              },
                        "y": {"field": "depth", "type": "quantitative"},
                        "tooltip": [
                            {
                            "field": "POS",
                            "type": "genomic"
                            },
                            {
                            "field": "depth",
                            "type": "quantitative"
                            }
                        ],
                        "opacity": {"value": 1},
                        "width": 600,
                        "height": 130
                        }
                    ]
                    }
                ]
            }
        ),
        constraints=[
            "E['use'] == 'point-mutation'", 
        ],
        query_type=QueryType.QUESTION,
        chart_type=ChartType.POINT,
    )
    
    df = add_row(
        df,
        query_template="What is the frequency of <E> across <L>?",
        spec=(
            {
                "title": "<E> Frequency on <L>",
                "layout": "linear",
                "arrangement": "vertical",
                "centerRadius": 0.8,
                "views": [
                    {
                    "tracks": [
                        {
                        "id": "track-1",
                        "data": {
                            "url": "<E.url>",
                            "type": "vcf",
                            "indexUrl": "<E.index-file>",
                        },
                        "dataTransform": [
                            {
                            "type": "coverage",
                            "startField": "POS",
                            "endField": "POS",
                            "newField": "depth"
                            }
                        ],
                        "mark": "bar",
                        "x": {
                            "field": "POS", 
                            "type": "genomic", 
                            "axis": "top",
                            "domain":
                                {
                                    "chromosome": "<L.geneChr>",
                                    "interval": ["<L.geneStart>", "<L.geneEnd>"]
                                }
                              },
                        "y": {"field": "depth", "type": "quantitative"},
                        "tooltip": [
                            {
                            "field": "POS",
                            "type": "genomic"
                            },
                            {
                            "field": "depth",
                            "type": "quantitative"
                            }
                        ],
                        "opacity": {"value": 1},
                        "width": 600,
                        "height": 130
                        }
                    ]
                    }
                ]
            }
        ),
        constraints=[
            "E['use'] == 'point-mutation'", 
        ],
        query_type=QueryType.QUESTION,
        chart_type=ChartType.POINT,
    )
    
    df = add_row(
        df,
        query_template="Where are the most <E> in <L>?",
        spec=(
            {
                "title": "<E> on <L>",
                "layout": "linear",
                "arrangement": "vertical",
                "centerRadius": 0.8,
                "views": [
                    {
                    "tracks": [
                        {
                        "id": "track-1",
                        "data": {
                            "url": "<E.url>",
                            "type": "vcf",
                            "indexUrl": "<E.index-file>",
                        },
                        "dataTransform": [
                            {
                            "type": "coverage",
                            "startField": "POS",
                            "endField": "POS",
                            "newField": "depth"
                            }
                        ],
                        "mark": "bar",
                        "x": {
                            "field": "POS", 
                            "type": "genomic", 
                            "axis": "top",
                            "domain":
                                {
                                    "chromosome": "<L.geneChr>",
                                    "interval": ["<L.geneStart>", "<L.geneEnd>"]
                                }
                              },
                        "y": {"field": "depth", "type": "quantitative"},
                        "tooltip": [
                            {
                            "field": "POS",
                            "type": "genomic"
                            },
                            {
                            "field": "depth",
                            "type": "quantitative"
                            }
                        ],
                        "opacity": {"value": 1},
                        "width": 600,
                        "height": 130
                        }
                    ]
                    }
                ]
            }
        ),
        constraints=[
            "E['use'] == 'point-mutation'", 
        ],
        query_type=QueryType.QUESTION,
        chart_type=ChartType.POINT,
    )
    
    #--------------------------------------------------------------------------
    #Comparative queries
    #--------------------------------------------------------------------------
    
    df = add_row(
        df,
        query_template="How do <E1> and <E2> compare for <S>?",
        spec=(
            {
                "title": "<E1> and <E2>",
                "layout": "linear",
                "arrangement": "vertical",
                "centerRadius": 0.8,
                "views": [
                    {
                    "tracks": [
                        
                        {
                        "id": "track-1",
                        "title": "<E1>",
                        "data": {
                            "url": "<E1.url>",
                            "type": "vcf",
                            "indexUrl": "<E1.index-file>",
                        },
                        
                        "mark": "point",
                        "x": {
                            "field": "POS", 
                            "type": "genomic", 
                            "domain":
                                {
                                    "chromosome": "chr1",
                                    "interval": [1, 1000000]
                                }
                              },
                        #"y": {"field": "depth", "type": "quantitative"},
                        "tooltip": [
                            {
                            "field": "POS",
                            "type": "genomic"
                            },
                        ],
                        "opacity": {"value": 1},
                        "width": 600,
                        "height": 130
                        },
                    
                        {
                            "data": {
                                "url": "<E2.url>",
                                "type": "csv",
                                "separator":"\t",
                                "genomicFieldsToConvert": [
                                    {"chromosomeField": "<E2.chr1>", "genomicFields":["<E2.start1>", "<E2.end1>"]},
                                    {"chromosomeField": "<E2.chr2>", "genomicFields":["<E2.start2>", "<E2.end2>"]},
                                ]
                            },
                            "mark": "withinLink",
                            "x": {"field": "<E2.start1>", "type": "genomic"},
                            "xe": {"field": "<E2.end2>", "type": "genomic"},
                            "strokeWidth": {"value": 1},
                            "opacity": {"value": 0.7}
                        }
                    
                ]
            }]}),
        constraints=[
             "E1['use'] == 'point-mutation'",
             "E2['use'] == 'sv'",  
        ],
        query_type=QueryType.QUESTION,
        chart_type=ChartType.POINT,
    )
    
    df = add_row(
        df,
        query_template="How do <E> at <L1> and <L2> compare?",
        spec=(
            {
                "title": "<E> on <L1> and <L2>",
                "layout": "linear",
                "arrangement": "vertical",
                "centerRadius": 0.8,
                "views": [
                    {
                    "tracks": [
                        
                        {
                        "id": "track-1",
                        "title": "<E> on <L1>",
                        "data": {
                            "url": "<E.url>",
                            "type": "vcf",
                            "indexUrl": "<E.index-file>",
                        },
                        
                        "mark": "point",
                        "x": {
                            "field": "POS", 
                            "type": "genomic", 
                            "domain":
                                {
                                    "chromosome": "<L1.geneChr>",
                                    "interval": ["<L1.geneStart>", "<L1.geneEnd>"]
                                }
                              },
                        #"y": {"field": "depth", "type": "quantitative"},
                        "tooltip": [
                            {
                            "field": "POS",
                            "type": "genomic"
                            },
                        ],
                        "opacity": {"value": 1},
                        "width": 600,
                        "height": 130
                        }
                    ]
                    },
                    {"tracks": [
                        {
                        "id": "track-2",
                        "title": "<E> on <L2>",
                        "data": {
                            "url": "<E.url>",
                            "type": "vcf",
                            "indexUrl": "<E.index-file>",
                        },
                        "mark": "point",
                        "x": {
                            "field": "POS", 
                            "type": "genomic", 
                            "domain":
                                {
                                    "chromosome": "<L2.geneChr>",
                                    "interval": ["<L2.geneStart>", "<L2.geneEnd>"]
                                }
                              },
                        #"y": {"field": "depth", "type": "quantitative"},
                        "tooltip": [
                            {
                            "field": "POS",
                            "type": "genomic"
                            },
                        ],
                        "opacity": {"value": 1},
                        "width": 600,
                        "height": 130
                        }
                    ]}
                ]
            }
        ),
        constraints=[
             "E['use'] == 'point-mutation'",
             different_genes
        ],
        query_type=QueryType.QUESTION,
        chart_type=ChartType.POINT,
    )
    
    df = add_row(
        df,
        query_template="Are there more <E> at <L1> or <L2>?",
        spec=(
            {
                "title": "<E> on <L1> and <L2>",
                "layout": "linear",
                "arrangement": "vertical",
                "centerRadius": 0.8,
                "views": [
                    {
                    "tracks": [
                        
                        {
                        "id": "track-1",
                        "title": "<E> on <L1>",
                        "data": {
                            "url": "<E.url>",
                            "type": "vcf",
                            "indexUrl": "<E.index-file>",
                        },
                        "dataTransform": [
                            {
                            "type": "coverage",
                            "startField": "POS",
                            "endField": "POS",
                            "newField": "depth"
                            }
                        ],
                        "mark": "bar",
                        "x": {
                            "field": "POS", 
                            "type": "genomic", 
                            "axis": "top",
                            "domain":
                                {
                                    "chromosome": "<L1.geneChr>",
                                    "interval": ["<L1.geneStart>", "<L1.geneEnd>"]
                                }
                              },
                        "y": {"field": "depth", "type": "quantitative"},
                        "tooltip": [
                            {
                            "field": "POS",
                            "type": "genomic"
                            },
                            {
                            "field": "depth",
                            "type": "quantitative"
                            }
                        ],
                        "opacity": {"value": 1},
                        "width": 600,
                        "height": 130
                        }
                    ]
                    },
                    {"tracks": [
                        {
                        "id": "track-2",
                        "title": "<E> on <L2>",
                        "data": {
                            "url": "<E.url>",
                            "type": "vcf",
                            "indexUrl": "<E.index-file>",
                        },
                        "dataTransform": [
                            {
                            "type": "coverage",
                            "startField": "POS",
                            "endField": "POS",
                            "newField": "depth"
                            }
                        ],
                        "mark": "bar",
                        "x": {
                            "field": "POS", 
                            "type": "genomic", 
                            "axis": "top",
                            "domain":
                                {
                                    "chromosome": "<L2.geneChr>",
                                    "interval": ["<L2.geneStart>", "<L2.geneEnd>"]
                                }
                              },
                        "y": {"field": "depth", "type": "quantitative"},
                        "tooltip": [
                            {
                            "field": "POS",
                            "type": "genomic"
                            },
                            {
                            "field": "depth",
                            "type": "quantitative"
                            }
                        ],
                        "opacity": {"value": 1},
                        "width": 600,
                        "height": 130
                        }
                    ]}
                ]
            }
        ),
        constraints=[
             "E['use'] == 'point-mutation'",
             different_genes
        ],
        query_type=QueryType.QUESTION,
        chart_type=ChartType.POINT,
    )
    
    
    df = add_row(
        df,
        query_template="How do <E> at <L1> and <L2> compare?",
        spec=(
            {
                "title": "Coverage on <L1> and <L2>",
                "layout": "linear",
                "arrangement": "vertical",
                "centerRadius": 0.8,
                "views": [
                    {
                    "tracks": [
                        
                        {
                        "data": {
                            "url": "<E.url>",
                            "type": "bam",
                            "indexUrl": "<E.index-file>",
                        },
                        "mark": "bar",
                        "dataTransform": [
                                {"type": "coverage", "startField": "<E.start>", "endField": "<E.end>"}
                            ],
                        "x": {
                            "field": "POS", 
                            "type": "genomic", 
                            "axis": "top",
                            "domain":
                                {
                                    "chromosome": "<L.geneChr>",
                                    "interval": ["<L.geneStart>", "<L.geneEnd>"]
                                }
                              },
                        "y": {"field": "depth", "type": "quantitative"},
                        "tooltip": [
                            {
                            "field": "POS",
                            "type": "genomic"
                            },
                            {
                            "field": "depth",
                            "type": "quantitative"
                            }
                        ]
                        },
                        
                        {
                        "data": {
                            "url": "<E.url>",
                            "type": "bam",
                            "indexUrl": "<E.index-file>",
                        },
                        "mark": "bar",
                        "dataTransform": [
                                {"type": "coverage", "startField": "<E.start>", "endField": "<E.end>"}
                            ],
                        
                        
                        "x": {
                            "field": "POS", 
                            "type": "genomic", 
                            "axis": "top",
                            "domain":
                                {
                                    "chromosome": "<L.geneChr>",
                                    "interval": ["<L.geneStart>", "<L.geneEnd>"]
                                }
                              },
                        "y": {"field": "depth", "type": "quantitative"},
                        "tooltip": [
                            {
                            "field": "POS",
                            "type": "genomic"
                            },
                            {
                            "field": "depth",
                            "type": "quantitative"
                            }
                        ]
                        },
                        
                    ]
                    }
                ]
            }
        ),
        constraints=[
             "E['use'] == 'coverage'",
             different_genes
        ],
        query_type=QueryType.QUESTION,
        chart_type=ChartType.POINT,
    )
   
    # df = add_row(
    #     df,
    #     query_template="How is <S1.E.F:p&q> distributed in <S1> versus <S2.E.F:p&q> in <S2>?",
    #     spec=(
    #         {
    #             "title": "<F> in <S1> and <S2>",
    #             "layout": "linear",
    #             "arrangement": "vertical",
    #             "centerRadius": 0.8,
    #             "views": [
    #                 {
    #                 "tracks": [
    #                     {
    #                         "title": "<F> in <S1>",
    #                         "data": {
    #                             "url": "<F.url>",
    #                             "type": "vcf",
    #                             "indexUrl": "<F.index-file>",
    #                         },
    #                         "mark": "point",
    #                         "x": {
    #                             "field": "<F>", 
    #                             "type": "genomic", 
    #                             "axis": "top",
    #                         },    
    #                         "tooltip": [
    #                             {
    #                             "field": "<F>",
    #                             "type": "genomic"
    #                             },
    #                             {
    #                             "field": "depth",
    #                             "type": "quantitative"
    #                             }
    #                         ],
    #                         "opacity": {"value": 1},
    #                         "width": 600,
    #                         "height": 130
    #                     },
    #                     {
    #                         "title": "<F> in <S2>",
    #                         "data": {
    #                             "url": "<F.url>",
    #                             "type": "vcf",
    #                             "indexUrl": "<F.index-file>",
    #                         },
    #                         "mark": "point",
    #                         "x": {
    #                             "field": "<F>", 
    #                             "type": "genomic", 
    #                             "axis": "top",
    #                         },    
    #                         "tooltip": [
    #                             {
    #                             "field": "<F>",
    #                             "type": "genomic"
    #                             },
    #                             {
    #                             "field": "depth",
    #                             "type": "quantitative"
    #                             }
    #                         ],
    #                         "opacity": {"value": 1},
    #                         "width": 600,
    #                         "height": 130
    #                     }] 
    #                 }
    #             ]
    #         }
    #     ),
    #     constraints=[
    #         #"S1.E.F['field'] == 'start1' or S1.E.F['field'] == 'end1' or S1.E.F['field'] == 'start2' or S1.E.F['field'] == 'end2'", 
    #         #"S1.E.F['field'] == S2.E.F['field']",
    #         #different_samples
    #     ],
    #     query_type=QueryType.QUESTION,
    #     chart_type=ChartType.POINT,
    # )
    

    return df


if __name__ == "__main__":
    df = generate()
    df.to_csv('spec_generation_test.tsv', sep='\t')
    print(df.head())
