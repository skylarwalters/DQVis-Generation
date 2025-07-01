
"""{
    "title": ""
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
        "x": {"field": "<E.start1>", "type": "genomic", "domain":[<E.pos1>, <E.pos2>]},
        "xe": {"field": "<E.end2>", "type": "genomic"},
        # CHeck how to acces sv method
        "stroke": {"field": "svmethod", "type": "nominal"},
        "strokeWidth": {"value": 1},
        "opacity": {"value": 0.7}
        }
    ]
}"""