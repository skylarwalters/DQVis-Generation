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
        query_template="How many <E> are there, grouped by <F:n>?",
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
            "F.c * 2 < E.c",
            "F.c <= 4",
            "F.c > 1",
        ],
        query_type=QueryType.QUESTION,
    )

    df = add_row(
        df,
        query_template="How many <E> are there, grouped by <F:n>?",
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
            "F.c * 2 < E.c",
            "F.c > 4",
            "F.c < 25",
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
            .x(field="<E2.F>", type="nominal")
            .y(field="<E1> count", type="quantitative")
        ),
        constraints=[
            "E2.F.c * 2 < E1.c",
            "E2.F.c <= 4",
            "E2.F.c > 1",
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
            "E2.F.c > 4",
            "E2.F.c < 25",
            "E1.r.E2.c.to == 'one'",
            "E2.F.name not in E1.fields"
        ],
        query_type=QueryType.QUESTION,
    )

    df = add_row(
        df,
        query_template="What is the most frequent <F:n>?",
        spec=(
            Chart()
            .source("<E>", "<E.url>")
            .filter("d.<F>")
            .groupby("<F>")
            .rollup({"count": Op.count()})
            .derive({"negativeCount": "-d.count"})
            .orderby("negativeCount")
            .derive({"rank": "rank()"})
            .filter("d.rank {lte} 5")
            .derive({"most frequent": "d.rank == 1 ? 'Most Frequent' : 'Other Frequent'"})
            .mark("bar")
            .x(field="count", type="quantitative")
            .y(field="<F>", type="nominal")
            .color(field="most frequent", type="nominal")
        ),
        constraints=[
            "F.c * 2 < E.c",
            "F.c > 1",
        ],
        query_type=QueryType.QUESTION,
    )

    df = add_row(
        df,
        query_template=f"How many <E1> are there, grouped by <E1.F1:n> and <E2.F2:n>?",
        spec=(
            Chart()
            .source("<E1>", "<E1.url>")
            .source("<E2>", "<E2.url>")
            .join(
                in_name=['<E1>', '<E2>'],
                on=['<E1.r.E2.id.from>', '<E1.r.E2.id.to>'],
                out_name='<E1>__<E2>',
            )
            .groupby(["<E2.F2>", "<E1.F1>"])
            .rollup({"count <E1>": Op.count()})
            .mark("bar")
            .y(field="count <E1>", type="quantitative")
            .color(field="<E2.F2>", type="nominal")
            .x(field="<E1.F1>", type="nominal")
        ),
        constraints=[
            "E1.F1.c * 2 < E1.c",
            "E2.F2.c * 2 < E2.c",
            "E1.F1.c > 1",
            "E2.F2.c > 1",
            "E1.F1.c <= 4",
            "E2.F2.c <= 4",
            "E1.F1.c >= E2.F2.c",
            "E1.r.E2.c.to == 'one'",
            "E2.F2.name not in E1.fields"
        ],
        query_type=QueryType.QUESTION,
    )

    df = add_row(
        df,
        query_template=f"How many <E1> are there, grouped by <E1.F1:n> and <E2.F2:n>?",
        spec=(
            Chart()
            .source("<E1>", "<E1.url>")
            .source("<E2>", "<E2.url>")
            .join(
                in_name=['<E1>', '<E2>'],
                on=['<E1.r.E2.id.from>', '<E1.r.E2.id.to>'],
                out_name='<E1>__<E2>',
            )
            .groupby(["<E2.F2>", "<E1.F1>"])
            .rollup({"count <E1>": Op.count()})
            .mark("bar")
            .x(field="count <E1>", type="quantitative")
            .color(field="<E1.F1>", type="nominal")
            .y(field="<E2.F2>", type="nominal")
        ),
        constraints=[
            "E1.F1.c * 2 < E1.c",
            "E2.F2.c * 2 < E2.c",
            "E1.F1.c > 1",
            "E1.F1.c < 25",
            "E2.F2.c > 1",
            "E2.F2.c < 25",
            "E1.F1.c > 4 or E2.F2.c > 4",
            "E1.F1.c <= E2.F2.c",
            "E1.r.E2.c.to == 'one'",
            "E2.F2.name not in E1.fields"
        ],
        query_type=QueryType.QUESTION,
    )

    df = add_row(
        df,
        query_template=f"How many <E> are there, grouped by <F1:n> and <F2:n>?",
        spec=(
            Chart()
            .source("<E>", "<E.url>")
            .groupby(["<F2>", "<F1>"])
            .rollup({"count <E>": Op.count()})
            .mark("bar")
            .y(field="count <E>", type="quantitative")
            .color(field="<F1>", type="nominal")
            .x(field="<F2>", type="nominal")
        ),
        constraints=[
            "F1.c * 2 < E.c",
            "F2.c * 2 < E.c",
            "F1.c > 1",
            "F2.c > 1",
            "F1.c <= 4",
            "F2.c <= 4",
            "F2.c >= F1.c",
        ],
        query_type=QueryType.QUESTION,
    )

    df = add_row(
        df,
        query_template=f"How many <E> are there, grouped by <F1:n> and <F2:n>?",
        spec=(
            Chart()
            .source("<E>", "<E.url>")
            .groupby(["<F1>", "<F2>"])
            .rollup({"count <E>": Op.count()})
            .mark("bar")
            .x(field="count <E>", type="quantitative")
            .color(field="<F1>", type="nominal")
            .y(field="<F2>", type="nominal")
        ),
        constraints=[
            "F1.c * 2 < E.c",
            "F2.c * 2 < E.c",
            "F1.c > 4 or F2.c > 4",
            "F1.c > 1",
            "F1.c < 25",
            "F2.c > 1",
            "F2.c < 25",
            "F2.c >= F1.c",
        ],
        query_type=QueryType.QUESTION,
    )

    df = add_row(
        df,
        query_template=f"What is the count of <F1:n> for each <F2:n>?",
        spec=(
            Chart()
            .source("<E>", "<E.url>")
            .groupby(["<F1>", "<F2>"])
            .rollup({"count <E>": Op.count()})
            .mark("bar")
            .y(field="count <E>", type="quantitative")
            .xOffset(field="<F1>", type="nominal")
            .color(field="<F1>", type="nominal")
            .x(field="<F2>", type="nominal")
        ),
        constraints=[
            "F1.c * 2 < E.c",
            "F2.c * 2 < E.c",
            "F1.c > 1",
            "F2.c > 1",
            "F1.c <= 4",
            "F2.c <= 4",
        ],
        query_type=QueryType.QUESTION,
    )

    df = add_row(
        df,
        query_template=f"What is the count of <F1:n> for each <F2:n>?",
        spec=(
            Chart()
            .source("<E>", "<E.url>")
            .groupby(["<F1>", "<F2>"])
            .rollup({"count <E>": Op.count()})
            .mark("bar")
            .x(field="count <E>", type="quantitative")
            .yOffset(field="<F1>", type="nominal")
            .color(field="<F1>", type="nominal")
            .y(field="<F2>", type="nominal")
        ),
        constraints=[
            "F1.c * 2 < E.c",
            "F2.c * 2 < E.c",
            "F1.c > 4 or F2.c > 4",
            "F1.c > 1",
            "F1.c < 5",
            "F2.c > 1",
            "F2.c < 25",
            "F2.c >= F1.c",
        ],
        query_type=QueryType.QUESTION,
    )

    df = add_row(
        df,
        query_template=f"What is the count of <F1:n> for each <F2:n>?",
        spec=(
            Chart()
            .source("<E>", "<E.url>")
            .groupby(["<F1>", "<F2>"])
            .rollup({"count <E>": Op.count()})
            .mark("bar")
            .x(field="count <E>", type="quantitative")
            .color(field="<F1>", type="nominal")
            .y(field="<F2>", type="nominal")
        ),
        constraints=[
            "F1.c * 2 < E.c",
            "F2.c * 2 < E.c",
            "F1.c > 4 or F2.c > 4",
            "F1.c > 1",
            "F1.c < 10",
            "F2.c > 1",
            "F2.c < 25",
            "F2.c >= F1.c",
        ],
        query_type=QueryType.QUESTION,
    )


    df = add_row(
        df,
        query_template=f"What is the frequency of <F1:n> for each <F2:n>?",
        spec=(
            Chart()
            .source("<E>", "<E.url>")
            .groupby("<F2>", out_name="groupCounts")
            .rollup({"<F2>_count": Op.count()})
            .groupby(["<F1>", "<F2>"], in_name="<E>")
            .rollup({"<F1>_and_<F2>_count": Op.count()})
            .join(
                in_name=["<E>", "groupCounts"],
                on="<F2>",
                out_name="datasets",
            )
            .derive({"frequency": "d.<F1>_and_<F2>_count / d.<F2>_count"})
            .mark("bar")
            .y(field="frequency", type="quantitative")
            .color(field="<F1>", type="nominal")
            .x(field="<F2>", type="nominal")
        ),
        constraints=[
            "F1.c * 2 < E.c",
            "F2.c * 2 < E.c",
            "F1.c > 1",
            "F2.c > 1",
            "F1.c <= 4",
            "F2.c <= 4",
        ],
        query_type=QueryType.QUESTION,
    )

    df = add_row(
        df,
        query_template=f"What is the frequency of <F1:n> for each <F2:n>?",
        spec=(
            Chart()
            .source("<E>", "<E.url>")
            .groupby("<F2>", out_name="groupCounts")
            .rollup({"<F2>_count": Op.count()})
            .groupby(["<F1>", "<F2>"], in_name="<E>")
            .rollup({"<F1>_and_<F2>_count": Op.count()})
            .join(
                in_name=["<E>", "groupCounts"],
                on="<F2>",
                out_name="datasets",
            )
            .derive({"frequency": "d.<F1>_and_<F2>_count / d.<F2>_count"})
            .mark("bar")
            .x(field="frequency", type="quantitative")
            .color(field="<F1>", type="nominal")
            .y(field="<F2>", type="nominal")
        ),
        constraints=[
            "F1.c * 2 < E.c",
            "F2.c * 2 < E.c",
            "F1.c > 4 or F2.c > 4",
            "F1.c > 1",
            "F1.c < 25",
            "F2.c > 1",
            "F2.c < 25",
        ],
        query_type=QueryType.QUESTION,
    )

    for name, op in [('minimum', Op.min), ('maximum', Op.max), ('average', Op.mean), ('median', Op.median), ('total', Op.sum)]:
        named_aggregate = f"{name} <F1>"
        df = add_row(
            df,
            query_template=f"What is the {name} <F1:q> for each <F2:n>?",
            spec=(
                Chart()
                .source("<E>", "<E.url>")
                .groupby("<F2>")
                .rollup({named_aggregate: op("<F1>")})
                .mark("bar")
                .x(field=named_aggregate, type="quantitative")
                .y(field="<F2>", type="nominal")
            ),
            constraints=[
                "F1.c > 10",
                "F2.c * 2 < E.c",
                "F2.c > 4",
                "F2.c < 25",
            ],
            query_type=QueryType.QUESTION,
        )

        df = add_row(
            df,
            query_template=f"What is the {name} <F1:q> for each <F2:n>?",
            spec=(
                Chart()
                .source("<E>", "<E.url>")
                .groupby("<F2>")
                .rollup({named_aggregate: op("<F1>")})
                .mark("bar")
                .x(field="<F2>", type="nominal")
                .y(field=named_aggregate, type="quantitative")
            ),
            constraints=[
                "F1.c > 10",
                "F2.c * 2 < E.c",
                "F2.c <= 4",
                "F2.c > 1",
            ],
            query_type=QueryType.QUESTION,
        )

    scatterplot_constraints=[
        "F1.c > 10",
        "F2.c > 10",
        "E.c < 100000"
    ]
    df = add_row(
        df,
        query_template="Is there a correlation between <F1:q> and <F2:q>?",
        spec=(
            Chart()
            .source("<E>", "<E.url>")
            .mark("point")
            .x(field="<F1>", type="quantitative")
            .y(field="<F2>", type="quantitative")
        ),
        constraints=scatterplot_constraints,
        query_type=QueryType.QUESTION,
    )

    df = add_row(
        df,
        query_template="Make a scatterplot of <F1:q> and <F2:q>?",
        spec=(
            Chart()
            .source("<E>", "<E.url>")
            .mark("point")
            .x(field="<F1>", type="quantitative")
            .y(field="<F2>", type="quantitative")
        ),
        constraints=scatterplot_constraints,
        query_type=QueryType.UTTERANCE,
    )

    
    df = add_row(
        df,
        query_template="What is the distribution of <F:q>?",
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
        constraints=[
            "E.c > 20",
            "F.c > 1",
            ],
        query_type=QueryType.QUESTION,
    )

    df = add_row(
        df,
        query_template="What is the distribution of <F:q>?",
        spec=(
            Chart()
            .source("<E>", "<E.url>")
            .mark("point")
            .x(field="<F>", type="quantitative")
        ),
        constraints=[
            "E.c <= 20",
            "E.c > 3",
            "F.c > 1",
        ],
        query_type=QueryType.QUESTION,
    )

    df = add_row(
        df,
        query_template="Make a stacked bar chart of <F1:n> and <F2:n>?",
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
            "F1.c * 2 < E.c",
            "F1.c < 25",
            "F1.c > F2.c",
            "1 < F2.c",
            "F2.c < 8",
            "F1.c <= 4",
        ],
        query_type=QueryType.UTTERANCE,
    )

    df = add_row(
        df,
        query_template="Make a stacked bar chart of <F1:n> and <F2:n>?",
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
            "F1.c * 2 < E.c",
            "F1.c < 25",
            "F1.c > F2.c",
            "1 < F2.c",
            "F2.c < 8",
            "F1.c > 4",
        ],
        query_type=QueryType.UTTERANCE,
    )

    df = add_row(
        df,
        query_template="Make a pie chart of <F:n>?",
        spec=(
            Chart()
            .source("<E>", "<E.url>")
            .groupby('<F>')
            .rollup({"frequency": Op.frequency()})
            .mark("arc")
            .theta(field="frequency", type="quantitative")
            .color(field="<F>", type="nominal")
        ),
        constraints=[
            "F.c * 2 < E.c",
            "F.c > 1",
            "F.c < 8",
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
