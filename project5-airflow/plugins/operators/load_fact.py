from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.operators import PostgresOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(PostgresOperator):

    ui_color = '#F98866'
    @apply_defaults
    def __init__(self,
                 sql,
                 redshift_conn_id='redshift',
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.postgres_conn_id = redshift_conn_id

    def execute(self):
        self.log.info('Executing: %s', self.sql)
        self.hook = PostgresHook(postgres_conn_id=self.postgres_conn_id,
                                 schema=self.database)
        self.hook.run(self.sql, self.autocommit, parameters=self.parameters)
        for output in self.hook.conn.notices:
            self.log.info(output)
