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


    # ---------------------------------------------------------
    # Mapping entities
    # ---------------------------------------------------------
    
    # df = add_row(
    #     df,
    #     query_template="Map the <E> data",
    #     spec=(
    #         {
    #             "title": "Structural variants on whole genome",
    #             "tracks": [
    #                 {
    #                 "data": {
    #                     "url": "<E.url>",
    #                     "type": "csv",
    #                     "separator":"\t",
    #                     "genomicFieldsToConvert": [
    #                         {"chromosomeField": "<E.chr1>", "genomicFields":["<E.start1>", "<E.end1>"]},
    #                         {"chromosomeField": "<E.chr2>", "genomicFields":["<E.start2>", "<E.end2>"]},
    #                     ]
    #                 },
    #                 "mark": "withinLink",
    #                 "x": {"field": "<E.start1>", "type": "genomic"},
    #                 "xe": {"field": "<E.end2>", "type": "genomic"},
    #                 "strokeWidth": {"value": 1},
    #                 "opacity": {"value": 0.7}
    #                 }
    #             ]
    #         }
    #     ),
    #     constraints=[
    #         "E['use'] == 'sv'",
    #     ],
    #     query_type=QueryType.UTTERANCE,
    #     chart_type=ChartType.CONNECTIVITY,
    # )
    
    # df = add_row(
    #     df,
    #     query_template="Map the <E> data at <L>",
    #     spec=(
    #         {
    #         "title": "Structural variants at <L>",
    #         "tracks": [
    #             {
    #             "data": {
    #                 "url": "<E.url>",
    #                 "type": "csv",
    #                 "separator": "\t",
    #                 "genomicFieldsToConvert": [
    #                 {
    #                     "chromosomeField": "<E.chr1>",
    #                     "genomicFields": ["<E.start1>", "<E.end1>"]
    #                 },
    #                 {
    #                     "chromosomeField": "<E.chr2>",
    #                     "genomicFields": ["<E.start2>", "<E.end2>"]
    #                 }
    #                 ]
    #             },
    #             "mark": "withinLink",
    #             "x": {
    #                 "field": "<E.start1>",
    #                 "type": "genomic",
    #                 "domain": {
    #                 "chromosome": "<L.geneChr>",
    #                 "interval": ["<L.geneStart>", "<L.geneEnd>"]
    #                 }
    #             },
    #             "xe": {
    #                 "field": "<E.end2>",
    #                 "type": "genomic"
    #             },
    #             "strokeWidth": {
    #                 "value": 1
    #             },
    #             "opacity": {
    #                 "value": 0.7
    #             }
    #             }
    #         ]
    #         }
    #     ),
    #     constraints=[
    #         "E['use'] == 'sv'",
    #     ],
    #     query_type=QueryType.UTTERANCE,
    #     chart_type=ChartType.CONNECTIVITY,
    # )
    
    # df = add_row(
    #     df,
    #     query_template="Map the <E> data.",
    #     spec=({
    #         "tracks":[{
    #             "title": "Copy Number Variants",
    #             "data": {
    #                 "separator": "\t",
    #                 "url": "<E.url>",
    #                 "type": "csv",
    #                 "chromosomeField": "<E.chr1>",
    #                 "genomicFields": ["<E.start>", "<E.end>"]
    #             },
    #             "mark": "rect",
    #             "x": {
    #                 "field": "<E.start>",
    #                 "type": "genomic"
    #             },
    #             "xe": {
    #                 "field": "<E.end>",
    #                 "type": "genomic"
    #             },
    #             "width": 1400,
    #             "height": 60
    #         }
    #         ]}
    #     ),
    #     constraints=[
    #         "E['use'] == 'cna'",
    #     ],
    #     query_type=QueryType.UTTERANCE,
    #     chart_type=ChartType.RECTANGLE,
    # )
    
    
    # df = add_row(
    #     df,
    #     query_template="Map the <E> data at <L>.",
    #     spec=({
    #         "tracks":[{
    #             "title": "Copy Number Variants",
    #             "data": {
    #                 "separator": "\t",
    #                 "url": "<E.url>",
    #                 "type": "csv",
    #                 "chromosomeField": "<E.chr1>",
    #                 "genomicFields": ["<E.start>", "<E.end>"]
    #             },
    #             "mark": "rect",
    #             "x": {
    #                 "field": "<E.start>",
    #                 "type": "genomic",
    #                 "domain": {
    #                 "chromosome": "<L.geneChr>",
    #                 "interval": ["<L.geneStart>", "<L.geneEnd>"]
    #                 }
    #             },
    #             "xe": {
    #                 "field": "<E.end>",
    #                 "type": "genomic"
    #             },
    #             "width": 1400,
    #             "height": 60
    #         }
    #         ]}
    #     ),
    #     constraints=[
    #         "E['use'] == 'cna'",
    #     ],
    #     query_type=QueryType.UTTERANCE,
    #     chart_type=ChartType.RECTANGLE,
    # )
    
    # df = add_row(
    #     df,
    #     query_template="Map the <E> data at <L>.",
    #     spec=({
    #         "tracks":[{
    #             "title": "Copy Number Variants",
    #             "data": {
    #                 "separator": "\t",
    #                 "url": "<E.url>",
    #                 "type": "csv",
    #                 "chromosomeField": "<E.chr1>",
    #                 "genomicFields": ["<E.start>", "<E.end>"]
    #             },
    #             "mark": "rect",
    #             "x": {
    #                 "field": "<E.start>",
    #                 "type": "genomic",
    #                 "domain": {
    #                 "chromosome": "<L.geneChr>",
    #                 "interval": ["<L.geneStart>", "<L.geneEnd>"]
    #                 }
    #             },
    #             "xe": {
    #                 "field": "<E.end>",
    #                 "type": "genomic"
    #             },
    #             "width": 1400,
    #             "height": 60
    #         }
    #         ]}
    #     ),
    #     constraints=[
    #         "E['use'] == 'cna'",
    #     ],
    #     query_type=QueryType.UTTERANCE,
    #     chart_type=ChartType.RECTANGLE,
    # )
     # point mutations + indels
    # df = add_row(
    #     df,
    #     query_template="Map the <E> data.",
    #     spec=({
    #         "title": "Point Mutations",
    #         'tracks': [{
                
    #             "data": {
    #                 "type": "vcf",
    #                 "url": "<E.url>",
    #                 "indexUrl": "<E.related>",
    #             },
    #             "mark": "point",
    #             "x": {
    #                 "field": "POS",
    #                 "type": "genomic"
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
    #             ]
    #         }
    #     ),
    #     constraints=[
    #         "E['use'] == 'point-mutation'",
    #     ],
    #     query_type=QueryType.UTTERANCE,
    #     chart_type=ChartType.RECTANGLE,
    # )
    
    # df = add_row(
    #     df,
    #     query_template="Map the <E> data at <L>.",
    #     spec=({
    #         "title": "Point Mutations",
    #         'tracks': [{
                
    #             "data": {
    #                 "type": "vcf",
    #                 "url": "<E.url>",
    #                 "indexUrl": "<E.related>",
    #             },
    #             "mark": "point",
    #             "x": {
    #                 "field": "POS",
    #                 "type": "genomic",
    #                 "domain": {
    #                     "chromosome": "<L.geneChr>",
    #                     "interval": ["<L.geneStart>", "<L.geneEnd>"]
    #                 }
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
    #             ]
    #             }],
    #         }
    #     ),
    #     constraints=[
    #         "E['use'] == 'point-mutation'",
    #     ],
    #     query_type=QueryType.UTTERANCE,
    #     chart_type=ChartType.RECTANGLE,
    # )
    
    
    # df = add_row(
    #     df,
    #     query_template="Map the <E> data.",
    #     spec=({
    #         "title": "Indels",
    #         'tracks': [{
                
    #             "data": {
    #                 "type": "vcf",
    #                 "url": "<E.url>",
    #                 "indexUrl": "<E.related>",
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
    #                 "indexUrl": "<E.related>",
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
        query_template="WHAT IS THE <E> here",
        spec=(
            {
                "title": "Pileup Track Using BAM Data",
                "layout": "linear",
                "tracks": [
                    {
                        "data": {
                            "url": "<E.url>",
                            "type": "bam",
                            "indexUrl": "<E.index-file",
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
            "F['use'] == 'bam'",
        ],
        query_type=QueryType.QUESTION,
        chart_type=ChartType.POINT,
    )
    
    
    
    
    # df = add_row(
    #     df,
    #     query_template="Where are <F:p&q> at <L> for <S>",
    #     spec=(
    #         {
    #             "tracks": [
    #                 {
    #                 "data": {
    #                     "url": "<F.url>",
    #                     "type":"vcf",
    #                     "indexUrl":"https://somatic-browser-test.s3.amazonaws.com/PCAWG/Cervix-AdenoCA/b9d1a64e-d445-4174-a5b4-76dd6ea69419.sorted.vcf.gz.tbi",
    #                     "sampleLength":1000 
    #                 },
    #                 "mark": "point",
    #                 "x": {"field": "<F.field>", "type": "genomic", "axis":"bottom"},
    #                 }
    #             ]
    #         }
    #     ),
    #     constraints=[
    #         "F['field'] == 'POS'",
    #     ],
    #     query_type=QueryType.QUESTION,
    #     chart_type=ChartType.POINT,
    # )
    
    
    
    
    
    
    # df = add_row(
    #     df,
    #     query_template="Where are <F:p&q> at <L> for <S>",
    #     spec=(
    #         {
    #             "tracks": [
    #                 {
    #                 "data": {
    #                     "url": "<F.url>",
    #                     "type":"vcf",
    #                     "indexUrl":"https://somatic-browser-test.s3.amazonaws.com/PCAWG/Cervix-AdenoCA/b9d1a64e-d445-4174-a5b4-76dd6ea69419.sorted.vcf.gz.tbi",
    #                     "sampleLength":1000 
    #                 },
    #                 "mark": "point",
    #                 "x": {"field": "<F.field>", "type": "genomic", "axis":"bottom"},
    #                 }
    #             ]
    #         }
    #     ),
    #     constraints=[
    #         "F['field'] == 'POS'",
    #     ],
    #     query_type=QueryType.QUESTION,
    #     chart_type=ChartType.POINT,
    # )
    
   
   
    

    return df


if __name__ == "__main__":
    df = generate()
    df.to_csv('spec_generation_test.tsv', sep='\t')
    print(df.head())
