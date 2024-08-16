# Athena module

This module is used for integrating lambda with AWS Athena.

## How does it work?

This module provides some useful functions that can be used to query data from an S3 bucket using Athena and parse the result.

Here are the brief descriptions of the functions:

-   `execute_athena_query`: This function is used to execute a query in Athena and wait for the result. It returns the _query execution object_.
-   `get_query_results`: This function is used to parse the result of a query executed in Athena and return _a list of headers_ and _a list of rows_.
-   `query_result_to_dict`: This function is used to convert the result of a query executed in Athena to a list of dictionaries.

## How to use it?

The example is shown on the `lambda_function.py` file.
