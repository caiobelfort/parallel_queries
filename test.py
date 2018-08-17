import unittest
import parallel_queries as pqueries
import sqlalchemy as sa

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
        expected_named_params = ['param_id', 'param_value']

        self.assertEquals(set(computed_named_params), set(expected_named_params), 'Named params not equal expected.')

    def test_empty_named_params_from_query(self):
        query = "SELECT TOP 10 * FROM table WHERE x = 2"
        computed_named_params = pqueries.get_named_params(query)

        self.assertEqual(len(computed_named_params), 0)

    def test_parallel_hinted_params(self):
        params = ['param_x_PARALLEL', 'param_y', 'param_z_PARALLEL']

        parallel_params = pqueries.get_parallel_hinted_params(params)

        self.assertEqual(set(parallel_params), {'param_x_PARALLEL', 'param_z_PARALLEL'})


    def test_execute_query_in_parallel_raises_parameter_missing(self):
        """Tests if all parameters are defined for the query"""

        stmt = """
        SELECT * 
        FROM table 
        WHERE val1 > :param_1 AND val2 = :param_2
        """

        parameters = {'param_1': 2}
        conn = None

        with self.assertRaises(ValueError):
            pqueries.execute_query_in_parallel(conn, stmt, parameters)

    def test_execute_query_in_parallel_run_without_hint(self):


        eng = sa.create_engine('sqlite:///:memory:')

        eng.execute('CREATE TABLE t (val1 INT, val2 INT)')
        eng.execute('INSERT INTO t VALUES (1, 1), (2, 3), (5, 4)')

        stmt_1 = """
        SELECT * FROM t
        """

        try:
            eng.execute(stmt_1)
        except:
            self.fail('Code failed on stmt_1')


        stmt_2 = """
        SELECT * from t WHERE val1 < :param_1
        """

        try:
            eng.execute(stmt_2, {'param_1': 4})
        except:
            self.fail('Code failed on stmt_2')

