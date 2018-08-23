import typing
import re
from itertools import chain

import sqlalchemy
from joblib import Parallel, delayed


def get_named_params(stmt: str) -> typing.List[typing.Tuple[str, str]]:
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
        group_list.append((group[0], group[1]))
    return group_list


def _check_missing_params(sql_params: typing.List[str],
                          user_params: typing.List[str]) -> typing.Tuple[int, typing.List[str]]:
    """
    Check if all required parameters of a SQL script are passed by the user

    Args:
        sql_params: A list of required params
        user_params: A list of params passed by the user

    Returns:
        The number of missing params and the list with the names
    """

    missing_params = set(sql_params) - set(user_params)
    return len(missing_params), list(missing_params)


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
    n_missing, missing = _check_missing_params([g[1] for g in required_parameters], list(parameters.keys()))
    if n_missing:
        raise ValueError(
            'Missing %d parameter(s) value(s) required by the statament.\n' % (n_missing) +
            'Missing: (' + ', '.join(list(missing)) + ')'
        )

    # Check paramenters hinted by the 'PARALLEL' tag
    parallel_parameters = []

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
