import typing
import re


def get_named_params(stmt: str) -> dict:
    """
    Get named parameters from a sql statement.
    The named parameters are in the format (:param_x, :param_y) where the parameters are x and y.
    The preceeding 'param_' is a must have.
    Args:
        stmt: The SQL statement with the named parameters
    Returns:
        A dictionary with operators by parameters
    """

    # Match the regex
    regex = r"\s*(?P<op>in|IN|=|<|>|<=|>=)\s+:(?P<param>[a-zA-Z_0-9]*)\s*"

    matches = re.findall(regex, stmt)

    # removes the ':' from start of each param name
    params = {g[1]: g[0] for g in matches}
    return params


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


def named_style_param_to_qmarks(stmt: str,
                                params: dict
                                ) -> typing.Tuple[str, typing.List[typing.Any]]:
    """
    Returns a SQL statment with qmark parameters

    Args:
        stmt: A named style param SQL statement
        params: A dict with

    Returns:
        A tuple with converted SQL statement and a list of parameters in the correct order
    """
    param_list = []
    for p in params:
        if params[p]['op'].lower() == 'in':
            if not isinstance(params[p]['val'], list):
                raise ValueError('Parameter %s must be a list' % p)

            # Creates a list of qmarks based on size of the list
            qmarks = '(' + ','.join(['?'] * len(params[p]['val'])) + ')'
            stmt = stmt.replace(':' + p, qmarks)
            param_list.extend(params[p]['val'])
        else:
            stmt = stmt.replace(':' + p, '?')
            param_list.append(params[p]['val'])

    return stmt, param_list


def make_statement_partitions(stmt: str,
                              parameters: dict,
                              ) -> typing.List[typing.Tuple[str, typing.List[typing.Any]]]:
    """
    Creates a split in the statement based on best splitter parameter
    Args:
        stmt: The SQL statement
        parameters: A dictionary of parameters defined in the SQL statement.
    Returns:
        A list containing a tuple of (statement, params)
    """
    # Check if the *parameters* containing all values required by the query
    required_parameters = get_named_params(stmt)
    if len(required_parameters) == 0:
        return [(stmt, [])]

    n_missing, missing = _check_missing_params(list(required_parameters.keys()), list(parameters.keys()))
    if n_missing:
        raise ValueError(
            'Missing %d parameter(s) value(s) required by the statament.\n' % (n_missing) +
            'Missing: (' + ', '.join(list(missing)) + ')'
        )

    # Final form of parameters, maintain the order of the SQL statement
    params = {g: {'op': required_parameters[g], 'val': parameters[g]} for g in required_parameters}

    # Check paramenters hinted by the 'PARALLEL' tag
    parallel_parameters = [p for p in params if p.endswith('PARALLEL')]

    # If none parameter is parallel, execute the query as is.
    if len(parallel_parameters) == 0:
        return [named_style_param_to_qmarks(stmt, params)]


    # Get values from parallel parameters keeping only list values
    parallel_param_values = {k: params[k]['val'] for k in parallel_parameters if isinstance(params[k]['val'], list)}

    # Choose the best for split the statement
    lengths = {k: len(parallel_param_values[k]) for k in parallel_param_values}

    # This choose the best param based on the list len
    best_param_key = max(lengths.keys(), key=lambda key: lengths[key])
    best_param = params[best_param_key]
    del params[best_param_key]  # remove best param from the dict

    result = []
    for val in best_param['val']:
        parted_dict = dict(params, **{best_param_key: {'op': best_param['op'], 'val': [val]}})
        result.append(named_style_param_to_qmarks(stmt, parted_dict))

    return result
