from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):

    """
    Operator to help move data from S3 to Redshift.
    It supports JSON data types and JSON paths.

    :param redshift_conn_id: Airflow connection id for redshift
    :param aws_conn_id: Airflow connection id for aws
    :param schema: Redshift schema
    :param table: Target Redshift table
    :param s3_bucket: Source S3 Bucket
    :param s3_key: Source S3 Key
    :param json_path: Json path file location in S3
    :param autocommit: Auto commit Redshift tx


    """

    ui_color = '#358140'
    template_fields = ("s3_key",)

    @apply_defaults
    def __init__(self,
                 schema,
                 table,
                 s3_bucket,
                 s3_key,
                 autocommit=False,
                 redshift_conn_id='redshift',
                 aws_conn_id='aws_credentials',
                 json_path="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)

        self.aws_conn_id = aws_conn_id
        self.redshift_conn_id = redshift_conn_id
        self.schema = schema
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.autocommit = autocommit
        self.json_path = json_path

    def execute(self, context):
        aws_hook = AwsHook(self.aws_conn_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        copy_query = """
            COPY {schema}.{table}
            FROM 's3://{s3_bucket}/{s3_key}'
            with credentials
            'aws_access_key_id={access_key};aws_secret_access_key={secret_key}'
            JSON {json_path};
        """.format(schema=self.schema,
                   table=self.table,
                   s3_bucket=self.s3_bucket,
                   s3_key=self.s3_key,
                   access_key=credentials.access_key,
                   secret_key=credentials.secret_key,
                   json_path="'auto'" if self.json_path == "" else f"'s3://{self.s3_bucket}/{self.json_path}'"
                   )

        self.log.info('Executing COPY command...')
        redshift.run(copy_query, self.autocommit)
        self.log.info("COPY command complete...")
