from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators import PostgresOperator
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(PostgresOperator):
    """
    Operator to validate data quality

    :param table_info_dict: [{<table_name>, <not_null>}] 
    
    """

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # response expectation. Could be string, could be condition to eval
                 table_info_dict=[],
                 redshift_conn_id='redshift',
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.postgres_conn_id = redshift_conn_id
        self.expectation = expectation
        self.table_info_dict = table_info_dict

    def execute(self, **context):
        self.log.info('Executing: %s', self.sql)
        self.hook = PostgresHook(postgres_conn_id=self.postgres_conn_id,
                                 schema=self.database)

            # Test each table
        for table_dict in self.table_info_dict:
            table_name = table_dict["table_name"]
            column_that_should_not_be_null = table_dict["not_null"]
            # Check number of records (pass if > 0, else fail)
            records = self.hook.get_records(f"SELECT COUNT(*) FROM {table_name}")
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(
                    f"Data quality check failed. {table_name} returned no results")
            elif records[0][0] < 1:
                raise ValueError(
                    f"Data quality check failed. {table_name} contained 0 rows")
            else:
                # Now check is NOT NULL columns contain NULL
                null_records = self.hook.get_records(
                    f"SELECT COUNT(*) FROM {table_name} WHERE {column_that_should_not_be_null} IS NULL")
                if null_records[0][0] > 0:
                    col = column_that_should_not_be_null
                    raise ValueError(
                        f"Data quality check failed. {table_name} contained {null_records[0][0]} null records for {col}")
                else:
                    self.log.info(f"Data quality on table {table_name} check passed with {records[0][0]} records")
        
