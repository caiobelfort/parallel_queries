import typing
import re
from itertools import chain

import sqlalchemy
from joblib import Parallel, delayed


def get_named_params(stmt: str) -> typing.List[dict]:
    """
    Get named parameters from a sql statement.
    The named parameters are in the format (:param_x, :param_y) where the parameters are x and y.
    The preceeding 'param_' is a must have.
    Args:
        stmt: The SQL statement with the named parameters
    Returns:
        A list of dictionaries each with the operator and named of param
    """

    # Match the regex
    regex = r"\s*(?P<op>in|IN|=|<|>|<=|>=)\s+:(?P<param>[a-zA-Z_0-9]*)\s*"

    matches = re.findall(regex, stmt)

    # removes the ':' from start of each param name
    group_list = []
    for group in matches:
        group_list.append({'op': group[0], 'param': group[1]})
    return group_list


def get_parallel_hinted_params(parameters: typing.List[str]) -> typing.List[str]:
    """
    Returns a list of parallel hinted named params
    Args:
        stmt: The sql statement

    Returns:
        A list of parallel hinted params
    """

    return [p for p in parameters if p.endswith('_PARALLEL')]


def execute_query_in_parallel(engine: sqlalchemy.engine.Engine,
                              stmt: str,
                              parameters: dict,
                              n_jobs: int = 4,
                              verbose=0
                              ) -> typing.List[typing.List[sqlalchemy.engine.RowProxy]]:
    """
    Executes a query in parallel, choosing the best splitter parameter
    Args:
        connectable: A connectable object from sqlalchemy.
        stmt: The SQL statement, if the parallels hint are.
        parameters: A dictionary of parameters defined in the SQL statement.
        n_jobs: The number of workers to use.
    Returns:
        A list containing the rows retrived.
    """
    parameters = parameters.copy()

    engine.dispose()  # Dispose all connections from engine first

    def run_query(param):
        """Closure to run the query inside delayed"""
        with engine.connect() as conn:
            return conn.execute(sqlalchemy.text(stmt), param).fetchall()

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
        query = sqlalchemy.text(stmt)
        with engine.connect() as conn:
            return conn.execute(sqlalchemy.text(stmt), parameters).fetchall()

    # Get values from parallel parameters keeping only list values
    parallel_param_values = {k: parameters[k] for k in parallel_parameters if isinstance(parameters[k], list)}

    # Choose the best for split the statement
    lengths = {k: len(parallel_param_values[k]) for k in parallel_param_values}

    # This choose the best param based on the list len
    best_param = max(lengths.keys(), key=lambda key: lengths[key])

    best_param_values = parameters[best_param]
    del parameters[best_param]  # remove best param from the dict

    with Parallel(n_jobs=n_jobs, backend='threading', verbose=verbose) as pwork:
        result = pwork(
            delayed(run_query)(dict(parameters, **{best_param: [v] if engine.dialect.name == 'mssql' else v}))
            for v in best_param_values
        )

    engine.dispose()
    return result
