from .athena import execute_athena_query, get_query_results, query_result_to_dict


def lambda_handler(event, context):
    result = execute_athena_query("SELECT * FROM all_projects LIMIT 20")
    query_execution_id = result["QueryExecution"]["QueryExecutionId"]
    query_result = get_query_results(query_execution_id)
    query_result_dict = query_result_to_dict(*query_result)
    return query_result_dict
