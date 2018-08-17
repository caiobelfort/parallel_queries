import typing
import re
import sqlalchemy


def get_named_params(stmt: str) -> typing.List[str]:
    """
    Get named parameters from a sql statement.
    The named parameters are in the format (:param_x, :param_y) where the parameters are x and y.
    The preceeding 'param_' is a must have.
    Args:
        stmt: The SQL statement with the named parameters
    Returns:
        A list of named parameters
    """

    # Match the regex
    matchs = re.findall(r'\s*:param_[a-zA-Z_0-9]*\s*', stmt)

    # removes the ':' from start of each param name
    return [s.replace(':', '').strip() for s in matchs]


def get_parallel_hinted_params(parameters: typing.List[str]) -> typing.List[str]:
    """
    Returns a list of parallel hinted named params
    Args:
        stmt: The sql statement

    Returns:
        A list of parallel hinted params
    """

    return [p for p in parameters if p.endswith('_PARALLEL')]


def execute_query_in_parallel(connectable: sqlalchemy.engine.Connectable,
                              stmt: str,
                              parameters: dict
                              ) -> list:
    """
    Executes a query in parallel, choosing the best splitter parameter
    Args:
        connectable: A connectable object from sqlalchemy.
        stmt: The SQL statement, if the parallels hint are.
        parameters: A dictionary of parameters defined in the SQL statement.
    Returns:
        A list containing the rows retrived.
    """

    # Check if the *parameters* containing all values required by the query
    required_parameters = get_named_params(stmt)
    if set(required_parameters) != set(parameters.keys()):
        missing = set(required_parameters) - set(parameters.keys())
        raise ValueError(
            'Missing parameter values required by the statament. ' +
            'Missing: (' + ', '.join(list(missing)) + ')'
        )

    # Check paramenters hinted by the 'PARALLEL' tag
    parallel_parameters = get_parallel_hinted_params(required_parameters)

    # If none parameter is parallel, execute the query as is.
    if len(parallel_parameters) == 0:
        return connectable.execute(sqlalchemy.text(stmt), parameters).fetchall()

    # Choose the best for split the statement
    return []
