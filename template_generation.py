import pandas as pd
from udi_grammar_py import Chart, Op, rolling
from enum import Enum


def generate():
    df = pd.DataFrame(
        columns=[
            "query_template",
            "constraints",
            "spec",
            "query_type",
            "creation_method",
        ]
    )

    df = add_row(
        df,
        query_template="How many <E> are there, grouped by <E.F:N>?",
        spec=(
            Chart()
            .source("<E>", "<E.S>")
            .groupby("<E.F:N>")
            .rollup({"<E> count": Op.count()})
            .mark("bar")
            .x(field="<E.F>", type="nominal")
            .y(field="count", type="quantitative")
        ),
        constraints="",
        query_type=QueryType.QUESTION,
    )

    df = add_row(
        df,
        query_template="Is there a correlation between <F1:Q> and <F2:Q>?",
        spec=(
            Chart()
            .source("<E>", "<E.S>")
            .mark("point")
            .x(field="<F1>", type="quantitative")
            .y(field="<F2>", type="quantitative")
        ),
        constraints="",
        query_type=QueryType.QUESTION,
    )

    # df = add_row(
    #     df,
    #     query_template="TODO",
    #     spec=(
    #         Chart()
    #         .source("<E>", "<E.S>")
    #         .mark("TODO")
    #         .x(field="TODO", type="nominal")
    #         .y(field="TODO", type="quantitative")
    #     ),
    #     constraints="",
    #     query_type=QueryType.QUESTION,
    # )

    return df


class QueryType(Enum):
    QUESTION = "question"
    UTTERANCE = "utterance"


def add_row(df, query_template, constraints, spec, query_type: QueryType):
    df.loc[len(df)] = {
        "query_template": query_template,
        "constraints": constraints,
        "spec": spec.to_json(),
        "query_type": query_type.value,
        "creation_method": "template",
    }

    return df


if __name__ == "__main__":
    df = generate_templates()
    print(df.head())
