import pandas as pd
from udi_grammar_py import Chart, Op, rolling
from enum import Enum


def generate():
    df = pd.DataFrame(
        columns=[
            "query_template",
            "constraints",
            "spec_template",
            "query_type",
            "creation_method",
        ]
    )

    # df = add_row(
    #     df,
    #     query_template="How many <E> are there, grouped by <F:n>?",
    #     spec=(
    #         Chart()
    #         .source("<E>", "<E.url>")
    #         .groupby("<F>")
    #         .rollup({"<E> count": Op.count()})
    #         .mark("bar")
    #         .x(field="<F>", type="nominal")
    #         .y(field="<E> count", type="quantitative")
    #     ),
    #     constraints=[
    #         "F.c * 2 < E.c",
    #         "F.c < 4"
    #     ],
    #     query_type=QueryType.QUESTION,
    # )

    # df = add_row(
    #     df,
    #     query_template="How many <E> are there, grouped by <F:n>?",
    #     spec=(
    #         Chart()
    #         .source("<E>", "<E.url>")
    #         .groupby("<F>")
    #         .rollup({"<E> count": Op.count()})
    #         .mark("bar")
    #         .x(field="<E> count", type="quantitative")
    #         .y(field="<F>", type="nominal")
    #     ),
    #     constraints=[
    #         "F.c * 2 < E.c",
    #         "F.c >= 4",
    #         "F.c < 25",
    #     ],
    #     query_type=QueryType.QUESTION,
    # )

    df = add_row(
        df,
        query_template="How many <E1> are there, grouped by <E2.F:n>?",
        spec=(
            Chart()
            .source("<E1>", "<E1.url>")
            .source("<E2>", "<E2.url>")
            .join(
                in_name=['<E1>', '<E2>'],
                on=['<E1.r.E2.id.from>', '<E1.r.E2.id.to>'],
                out_name='<E1>__<E2>',
            )
            .groupby("<E2.F>")
            .rollup({"<E1> count": Op.count()})
            .mark("bar")
            .x(field="<E2.F>", type="nominal")
            .y(field="<E1> count", type="quantitative")
        ),
        constraints=[
            "E2.F.c * 2 < E1.c",
            "E2.F.c < 4",
            "E1.r.E2.c.to == 'one'",
            "E2.F.name not in E1.fields"
        ],
        query_type=QueryType.QUESTION,
    )

    df = add_row(
        df,
        query_template="How many <E1> are there, grouped by <E2.F:n>?",
        spec=(
            Chart()
            .source("<E1>", "<E1.url>")
            .source("<E2>", "<E2.url>")
            .join(
                in_name=['<E1>', '<E2>'],
                on=['<E1.r.E2.id.from>', '<E1.r.E2.id.to>'],
                out_name='<E1>__<E2>',
            )
            .groupby("<E2.F>")
            .rollup({"<E1> count": Op.count()})
            .mark("bar")
            .x(field="<E1> count", type="quantitative")
            .y(field="<E2.F>", type="nominal")
        ),
        constraints=[
            "E2.F.c * 2 < E1.c",
            "E2.F.c >= 4",
            "E2.F.c < 25",
            "E1.r.E2.c.to == 'one'",
            "E2.F.name not in E1.fields"
        ],
        query_type=QueryType.QUESTION,
    ) 


    # df = add_row(
    #     df,
    #     query_template="Is there a correlation between <F1:q> and <F2:q>?",
    #     spec=(
    #         Chart()
    #         .source("<E>", "<E.url>")
    #         .mark("point")
    #         .x(field="<F1>", type="quantitative")
    #         .y(field="<F2>", type="quantitative")
    #     ),
    #     constraints=[
    #         "F1.c > 10",
    #         "F2.c > 10"
    #     ],
    #     query_type=QueryType.QUESTION,
    # )

    
    # df = add_row(
    #     df,
    #     query_template="What is the distribution of <F:q>?",
    #     spec=(
    #         Chart()
    #         .source("<E>", "<E.url>")
    #         .kde(
    #             field="<F>", 
    #             output={
    #                 "sample": "<F>",
    #                 "density": "density"},)
    #         .mark("area")
    #         .x(field="<F>", type="quantitative")
    #         .y(field="density", type="quantitative")
    #     ),
    #     constraints=["E.c > 20"],
    #     query_type=QueryType.QUESTION,
    # )

    # df = add_row(
    #     df,
    #     query_template="What is the distribution of <F:q>?",
    #     spec=(
    #         Chart()
    #         .source("<E>", "<E.url>")
    #         .mark("point")
    #         .x(field="<F>", type="quantitative")
    #     ),
    #     constraints=[
    #         "E.c < 20",
    #         "E.c > 3"
    #     ],
    #     query_type=QueryType.QUESTION,
    # )


    # # df = add_row(
    # #     df,
    # #     query_template="TODO",
    # #     spec=(
    # #         Chart()
    # #         .source("<E>", "<E.url>")
    # #         .mark("TODO")
    # #         .x(field="TODO", type="nominal")
    # #         .y(field="TODO", type="quantitative")
    # #     ),
    # #     constraints=[],
    # #     query_type=QueryType.QUESTION,
    # # )

    # df = add_row(
    #     df,
    #     query_template="Make a stacked bar chart of <F1:n> and <F2:n>?",
    #     spec=(
    #         Chart()
    #         .source("<E>", "<E.url>")
    #         .groupby(['<F1>', '<F2>'])
    #         .rollup({"count": Op.count()})
    #         .mark("bar")
    #         .x(field="<F1>", type="nominal")
    #         .y(field="count", type="quantitative")
    #         .color(field="<F2>", type="nominal")
    #     ),
    #     constraints=[
    #         "F1.c * 2 < E.c",
    #         "F1.c < 25",
    #         "F1.c > F2.c",
    #         "1 < F2.c",
    #         "F2.c < 8",
    #         "F1.c <= 4",
    #     ],
    #     query_type=QueryType.UTTERANCE,
    # )

    # df = add_row(
    #     df,
    #     query_template="Make a stacked bar chart of <F1:n> and <F2:n>?",
    #     spec=(
    #         Chart()
    #         .source("<E>", "<E.url>")
    #         .groupby(['<F1>', '<F2>'])
    #         .rollup({"count": Op.count()})
    #         .mark("bar")
    #         .x(field="count", type="quantitative")
    #         .y(field="<F1>", type="nominal")
    #         .color(field="<F2>", type="nominal")
    #     ),
    #     constraints=[
    #         "F1.c * 2 < E.c",
    #         "F1.c < 25",
    #         "F1.c > F2.c",
    #         "1 < F2.c",
    #         "F2.c < 8",
    #         "F1.c > 4",
    #     ],
    #     query_type=QueryType.UTTERANCE,
    # )

    return df


class QueryType(Enum):
    QUESTION = "question"
    UTTERANCE = "utterance"


def add_row(df, query_template, spec, constraints, query_type: QueryType):
    df.loc[len(df)] = {
        "query_template": query_template,
        "constraints": constraints,
        "spec_template": spec.to_json(),
        "query_type": query_type.value,
        "creation_method": "template",
    }

    return df


if __name__ == "__main__":
    df = generate()
    print(df.head())
