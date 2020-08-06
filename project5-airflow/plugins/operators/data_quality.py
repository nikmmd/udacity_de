from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators import PostgresOperator
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(PostgresOperator):
    """
    Operator to validate data quality

    :param expectation: Condition to evaluate, for example ``> 0``
    :type expectation: string

    """

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # response expectation. Could be string, could be condition to eval
                 expectation,
                 redshift_conn_id='redshift',
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.postgres_conn_id = redshift_conn_id
        self.expectation = expectation

    def execute(self, **context):
        self.log.info('Executing: %s', self.sql)
        self.hook = PostgresHook(postgres_conn_id=self.postgres_conn_id,
                                 schema=self.database)
        results = self.hook.get_records(self.sql)
        exp = f'{results[0][0]} {self.expectation}'
        # evaluating condition
        is_valid = eval(exp)
        if not is_valid:
            self.log.error(f"Data Quality ckeck FAIL: Unexpected resuts. {exp}")
            raise
        self.log.info(f"Data Quality check PASS: {exp}")
        return 
        
