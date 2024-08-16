import boto3
import time

QUERY_DATABASE_NAME = "your_database"
QUERY_OUTPUT_LOCATION = "s3://your_bucket/athena-output"

athena_client = boto3.client("athena")


def execute_athena_query(query_string):
    print(f"Executing Athena Query: {query_string[0:400]}...")

    start_query_execution_output = athena_client.start_query_execution(
        QueryString=query_string,
        QueryExecutionContext={"Database": QUERY_DATABASE_NAME},
        ResultConfiguration={"OutputLocation": QUERY_OUTPUT_LOCATION},
    )
    print(start_query_execution_output)

    # Wait for query to complete
    get_query_execution_output = athena_client.get_query_execution(
        QueryExecutionId=start_query_execution_output["QueryExecutionId"]
    )
    query_state = get_query_execution_output["QueryExecution"]["Status"]["State"]

    while query_state == "RUNNING" or query_state == "QUEUED":
        print("Query is running...")
        time.sleep(1)
        get_query_execution_output = athena_client.get_query_execution(
            QueryExecutionId=start_query_execution_output["QueryExecutionId"]
        )
        query_state = get_query_execution_output["QueryExecution"]["Status"]["State"]

    print(f"Query completed with state: {query_state}")

    if query_state == "FAILED":
        raise Exception(
            "Athena query failed"
            + get_query_execution_output["QueryExecution"]["Status"][
                "StateChangeReason"
            ]
        )

    return get_query_execution_output


def get_query_results(query_execution_id):
    get_query_results_output = athena_client.get_query_results(
        QueryExecutionId=query_execution_id, MaxResults=1000
    )

    header_row = []
    for column_info in get_query_results_output["ResultSet"]["Rows"][0]["Data"]:
        header_row.append(column_info["VarCharValue"])

    rows = []
    for row in get_query_results_output["ResultSet"]["Rows"][1:]:
        row_data = []
        for column_info in row["Data"]:
            row_data.append(column_info.get("VarCharValue", ""))
        rows.append(row_data)

    while "NextToken" in get_query_results_output:
        get_query_results_output = athena_client.get_query_results(
            QueryExecutionId=query_execution_id,
            NextToken=get_query_results_output["NextToken"],
            MaxResults=1000,
        )

        for row in get_query_results_output["ResultSet"]["Rows"]:
            row_data = []
            for column_info in row["Data"]:
                row_data.append(column_info.get("VarCharValue", None))
            rows.append(row_data)

    return header_row, rows


def query_result_to_dict(header_row, rows):
    results = []
    for row in rows:
        result = {}
        for i, column in enumerate(row):
            result[header_row[i]] = column
        results.append(result)
    return results
