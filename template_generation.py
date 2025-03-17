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

    df = add_row(
        df,
        query_template="How many <E> are there, grouped by <F:N>?",
        spec=(
            Chart()
            .source("<E>", "<E.url>")
            .groupby("<F>")
            .rollup({"<E> count": Op.count()})
            .mark("bar")
            .x(field="<F>", type="nominal")
            .y(field="<E> count", type="quantitative")
        ),
        constraints=[
            "F.C * 2 < E.C",
            "F.C < 4"
        ],
        query_type=QueryType.QUESTION,
    )

    df = add_row(
        df,
        query_template="How many <E> are there, grouped by <F:N>?",
        spec=(
            Chart()
            .source("<E>", "<E.url>")
            .groupby("<F>")
            .rollup({"<E> count": Op.count()})
            .mark("bar")
            .x(field="<E> count", type="quantitative")
            .y(field="<F>", type="nominal")
        ),
        constraints=[
            "F.C * 2 < E.C",
            "F.C >= 4",
            "F.C < 25",
        ],
        query_type=QueryType.QUESTION,
    )

    df = add_row(
        df,
        query_template="Is there a correlation between <F1:Q> and <F2:Q>?",
        spec=(
            Chart()
            .source("<E>", "<E.url>")
            .mark("point")
            .x(field="<F1>", type="quantitative")
            .y(field="<F2>", type="quantitative")
        ),
        constraints=[
            "F1.C > 10",
            "F2.C > 10"
        ],
        query_type=QueryType.QUESTION,
    )

    
    df = add_row(
        df,
        query_template="What is the distribution of <F:Q>?",
        spec=(
            Chart()
            .source("<E>", "<E.url>")
            .kde(
                field="<F>", 
                output={
                    "sample": "<F>",
                    "density": "density"},)
            .mark("area")
            .x(field="<F>", type="quantitative")
            .y(field="density", type="quantitative")
        ),
        constraints=["E.C > 20"],
        query_type=QueryType.QUESTION,
    )

    df = add_row(
        df,
        query_template="What is the distribution of <F:Q>?",
        spec=(
            Chart()
            .source("<E>", "<E.url>")
            .mark("point")
            .x(field="<F>", type="quantitative")
        ),
        constraints=[
            "E.C < 20",
            "E.C > 3"
        ],
        query_type=QueryType.QUESTION,
    )


    # df = add_row(
    #     df,
    #     query_template="TODO",
    #     spec=(
    #         Chart()
    #         .source("<E>", "<E.url>")
    #         .mark("TODO")
    #         .x(field="TODO", type="nominal")
    #         .y(field="TODO", type="quantitative")
    #     ),
    #     constraints=[],
    #     query_type=QueryType.QUESTION,
    # )

    df = add_row(
        df,
        query_template="Make a stacked bar chart of <F1:N> and <F2:N>?",
        spec=(
            Chart()
            .source("<E>", "<E.url>")
            .groupby(['<F1>', '<F2>'])
            .rollup({"count": Op.count()})
            .mark("bar")
            .x(field="<F1>", type="nominal")
            .y(field="count", type="quantitative")
            .color(field="<F2>", type="nominal")
        ),
        constraints=[
            "F1.C * 2 < E.C",
            "F1.C < 25",
            "F1.C > F2.C",
            "1 < F2.C",
            "F2.C < 8",
            "F1.C <= 4",
        ],
        query_type=QueryType.UTTERANCE,
    )

    df = add_row(
        df,
        query_template="Make a stacked bar chart of <F1:N> and <F2:N>?",
        spec=(
            Chart()
            .source("<E>", "<E.url>")
            .groupby(['<F1>', '<F2>'])
            .rollup({"count": Op.count()})
            .mark("bar")
            .x(field="count", type="quantitative")
            .y(field="<F1>", type="nominal")
            .color(field="<F2>", type="nominal")
        ),
        constraints=[
            "F1.C * 2 < E.C",
            "F1.C < 25",
            "F1.C > F2.C",
            "1 < F2.C",
            "F2.C < 8",
            "F1.C > 4",
        ],
        query_type=QueryType.UTTERANCE,
    )

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
