from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.operators import PostgresOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(PostgresOperator):
    """
    Extension to PostgresOperator to help load dimension tables

    :param redshift_conn_id: Airflow connection id for redshift
    :param delete: Should tables be truncated before insertion
    :param table_name: Table to truncate

    """

    ui_color = '#80BD9E'
    @apply_defaults
    def __init__(self,
                 redshift_conn_id='redshift',
                 delete=False,
                 table_name=""
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.postgres_conn_id = redshift_conn_id
        self.delete = delete
        self.table_name = table_name

        if delete and len(table_name) < 1:
            raise "'table_name' property required when 'delete=True'" 

    def execute(self, **context):
        self.log.info('Executing: %s', self.sql)
        self.hook = PostgresHook(postgres_conn_id=self.postgres_conn_id,
                                 schema=self.database)
        if self.delete:
            self.log.info(
                f"Delete load operation set to TRUE. Running DELETE on table {self.table_name}")

            self.hook.run(f'DELETE FROM {self.table_name}')

        self.hook.run(self.sql, self.autocommit, parameters=self.parameters)
        for output in self.hook.conn.notices:
            self.log.info(output)


