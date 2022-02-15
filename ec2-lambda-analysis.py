import asyncio

import pandas as pd

import meadowgrid.aws_integration

# from https://aws.amazon.com/lambda/pricing/
lambda_prices = pd.DataFrame.from_records(
    [
        (128, 0.0000000021),
        (512, 0.0000000083),
        (1024, 0.0000000167),
        (1536, 0.0000000250),
        (2048, 0.0000000333),
        (3072, 0.0000000500),
        (4096, 0.0000000667),
        (5120, 0.0000000833),
        (6144, 0.0000001000),
        (7168, 0.0000001167),
        (8192, 0.0000001333),
        (9216, 0.0000001500),
        (10240, 0.0000001667),
    ],
    columns = ["memory_mb", "price_per_ms"]
)
# from https://docs.aws.amazon.com/lambda/latest/dg/configuration-function-common.html
lambda_prices["cpu"] = lambda_prices["memory_mb"] / 1769
lambda_prices["memory_gb"] = lambda_prices["memory_mb"] / 1024


ec2_prices = asyncio.run(meadowgrid.aws_integration._get_ec2_instance_types("us-east-1"))


_EC2_MIN_SECONDS_BILLING = 60
_EC2_STARTUP_TIME_SECONDS = 45


def breakeven(lambda_price_per_ms, ec2_price_per_hour, wait_cost_per_hour, num_instances):
    # the graph of ec2 price per second vs lambda price per second looks something like
    # this:

    #        /        _/
    #       /       _/
    #      /      _/
    # ___________/
    #    /
    #   /
    #  /
    # /

    # The piece-wise linear function is the EC2 cost, and the simple linear function is
    # the lambda cost. They might intersect in the first segment of the EC2 (shown here)
    # function or in the second segment of the EC2 function

    # For up to 60 seconds (the minimum EC2 runtime), the EC2 price is constant, based
    # on the cost of running that EC2 instance for 60s, plus the human cost of waiting
    # for that instance to start. After 60s, the cost starts going up linearly.
    ec2_price_per_second = num_instances * ec2_price_per_hour / (60 * 60)
    wait_cost_for_startup_time = _EC2_STARTUP_TIME_SECONDS * wait_cost_per_hour / (60 * 60)
    min_ec2_cost = (ec2_price_per_second * _EC2_MIN_SECONDS_BILLING
                    + wait_cost_for_startup_time)

    # The lambda cost goes up linearly from 0 at a faster rate than the EC2 cost.
    lambda_price_per_second = num_instances * lambda_price_per_ms * 1000

    # So the ec2 cost for >60s is
    # = ec2_price_per_second * (seconds - _EC2_MIN_BILLING) + min_ec2_cost
    # = ec2_price_per_second * seconds + wait_cost_for_startup_time

    # While the lambda cost is
    # = lambda_price_per_second * seconds

    # So setting those equal and solving for seconds, the intersection will be at
    breakeven_seconds = wait_cost_for_startup_time / (lambda_price_per_second - ec2_price_per_second)

    # if the breakeven seconds is before 60s, we need to recalculate it using the actual
    # cost of running EC2 for less than 60s, by finding when lambda_price_per_second *
    # seconds = min_ec2_cost
    if breakeven_seconds <= _EC2_MIN_SECONDS_BILLING:
        breakeven_seconds = min_ec2_cost / lambda_price_per_second

    return breakeven_seconds / 60


def foo(lambda_prices_inner, wait_cost_per_hour):
    breakeven_ec2_rows = []
    breakeven_values = []

    for lambda_memory_gb, lambda_cpu, lambda_price_per_ms, num_instances in \
            lambda_prices_inner[[
        "memory_gb", "cpu", "price_per_ms", "num_instances"]].itertuples(
            index=False):

        # find equivalent EC2 instance
        df = ec2_prices[(ec2_prices["memory_gb"] >= lambda_memory_gb) & (ec2_prices[
            "logical_cpu"] >= lambda_cpu)]
        equivalent_ec2_price = df["price"].min()

        breakeven_value = breakeven(lambda_price_per_ms, equivalent_ec2_price,
                                    wait_cost_per_hour, num_instances)

        breakeven_ec2_rows.append(df[df["price"] == equivalent_ec2_price] \
                                  .sort_values(by="interruption_probability").iloc[0])
        breakeven_values.append(breakeven_value)

    return pd.concat([lambda_prices_inner, pd.DataFrame.from_records(breakeven_ec2_rows),
                      pd.Series(breakeven_values, name="breakeven_minutes")], axis=1)


def bar():
    df = foo(lambda_prices.assign(num_instances=1), 100)
    df.to_clipboard(index=False)

    multiples = [25, 50, 100, 200, 400]

    df2 = lambda_prices.iloc[[2] * len(multiples)].copy().reset_index(drop=True)
    df2["num_instances"] = multiples
    foo(df2, 100).to_clipboard(index=False)

    df3 = lambda_prices.iloc[[4] * len(multiples)].copy().reset_index(drop=True)
    df3["num_instances"] = multiples
    foo(df3, 100).to_clipboard(index=False)

    df4 = lambda_prices.iloc[[12] * len(multiples)].copy().reset_index(drop=True)
    df4["num_instances"] = multiples
    foo(df4, 100).to_clipboard(index=False)