from dagster import op, OpExecutionContext, job, repository


@op
def generate() -> list[int]:
    return list(range(1_000))


@op
def square(xs: list[int]) -> list[int]:
    return [x * x for x in xs]


@op
def log_sum(context: OpExecutionContext, xs: list[int]) -> None:
    context.log.info(f"sum of squares: {sum(xs)}")


@job
def sum_of_squares():
    log_sum(square(generate()))


@repository
def repository():
    return [sum_of_squares]
