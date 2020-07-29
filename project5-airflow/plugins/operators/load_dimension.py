from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.operators import PostgresOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(PostgresOperator):

    ui_color = '#80BD9E'
    @apply_defaults
    def __init__(self,
                 redshift_conn_id='redshift',
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.postgres_conn_id = redshift_conn_id

