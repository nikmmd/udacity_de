from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.operators import PostgresOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(PostgresOperator):
    """
    Extension to PostgresOperator to help load dimension tables

    :param redshift_conn_id: Airflow connection id for redshift
    :param mode: truncate or insert
    :param table_name: Table to truncate

    """

    ui_color = '#80BD9E'
    @apply_defaults
    def __init__(self,
                 mode='insert',
                 redshift_conn_id='redshift',
                 table_name="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.postgres_conn_id = redshift_conn_id
        self.delete = delete
        self.table_name = table_name
        self.mode = mode

        if delete and len(table_name) < 1:
            raise "'table_name' property required when 'delete=True'" 

    def execute(self, **context):
        self.log.info('Executing: %s', self.sql)
        self.hook = PostgresHook(postgres_conn_id=self.postgres_conn_id,
                                 schema=self.database)

        if self.mode.lower() == 'truncate':
            self.log.info(
                f"Truncate then insert {self.table_name}")

            query = f"""
                TRUNCATE {self.table_name}; {self.sql}
                """
            self.hook.run(query, self.autocommit,
                          parameters=self.parameters)
                          
        if self.mode.lower() == 'insert':
            self.hook.run(self.sql, self.autocommit, parameters=self.parameters)

        for output in self.hook.conn.notices:
            self.log.info(output)


