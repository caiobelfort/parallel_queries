import unittest
import parallel_queries as pqueries
import sqlalchemy as sa
import os


class Tests(unittest.TestCase):

    def test_correct_named_param_from_query(self):
        query = """
        SELECT 
            id,
            value
        FROM table 
        WHERE id = :param_id AND value < :param_value
        """

        computed_named_params = pqueries.get_named_params(query)
        expected_named_params = {'param_id': '=', 'param_value': '<'}

        self.assertEqual(computed_named_params, expected_named_params, 'Named params not equal expected.')

    def test_empty_named_params_from_query(self):
        query = "SELECT TOP 10 * FROM table WHERE x = 2"
        computed_named_params = pqueries.get_named_params(query)

        self.assertEqual(len(computed_named_params), 0)


    def test_check_required_params(self):

        required_params = ['x', 'y']
        user_params = ['y']

        n_missing, missing = pqueries._check_missing_params(required_params, user_params)

        self.assertEqual(n_missing, 1, 'Number of missing params are wrong')
        self.assertEqual(set(missing), {'x'}, 'Missing params are wrong')


        required_params = ['y']
        n_missing, _ = pqueries._check_missing_params(required_params, user_params)
        self.assertEqual(n_missing, 0, 'Number of missing params are wrong')




    def test_execute_query_in_parallel_raises_parameter_missing(self):
        """Tests if all parameters are defined for the query"""

        stmt = """
        SELECT * 
        FROM table 
        WHERE val1 > :param_1 AND val2 = :param_2
        """

        parameters = {'param_1': 2}

        with self.assertRaises(ValueError):
            pqueries.make_statement_partitions(stmt, parameters)

    def test_execute_query_in_parallel_run_without_hint(self):

        stmt_1 = """
        SELECT * FROM t
        """

        try:
            result_1 = pqueries.make_statement_partitions(stmt_1, {})
        except:
            self.fail('Code failed on stmt_1')

        stmt_2 = """
        SELECT * FROM t WHERE val1 < :param_1
        """

        try:
            result_2 = pqueries.make_statement_partitions(stmt_2, {'param_1': 4})
        except:
            self.fail('Code failed on stmt_2')

        self.assertEqual(len(result_1), 1)
        self.assertEqual(len(result_2), 1)

        self.assertEqual(result_2[0][0], stmt_2.replace(':param_1', '?'))
        self.assertEqual(set(result_2[0][1]), {4})

    def test_execute_query_in_parallel_run(self):

        stmt = """
        SELECT *
        FROM t 
        WHERE val1 IN :param_1_PARALLEL AND val2 < :param_2
        """

        try:
            result = pqueries.make_statement_partitions(stmt, {'param_1_PARALLEL': [1, 2, 5, 8], 'param_2': 8})
        except:
            self.fail('Code failed')


        self.assertEqual(len(result), 4)
        self.assertEqual(result[0][0], stmt.replace(':param_1_PARALLEL', '(?)').replace(':param_2', '?'))
        self.assertEqual(set(result[1][1]), {8, 2})
