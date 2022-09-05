from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    template_fields = ("s3_key",)

    @apply_defaults
    def __init__(self,
                 s3_bucket='',
                 s3_key='',
                 table='',
                 redshift_conn_id='redshift',
                 aws_conn_id='aws_credentials',
                 copy_options='',
                 *args, **kwargs):
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.aws_conn_id = aws_conn_id
        self.copy_options = copy_options

    def execute(self, context):
        aws_hook = AwsHook(self.aws_conn_id)
        credentials = aws_hook.get_credentials()
        redshift_hook = PostgresHook(self.redshift_conn_id)

        rendered_key = self.s3_key.format(**context)

        self.log.info(f'out put {rendered_key}')
        self.log.info(f'Preparing to stage data from {self.s3_bucket}/{rendered_key} to {self.table} table...')

        copy_query = """
                    COPY {table}
                    FROM 's3://{s3_bucket}/{rendered_key}'
                    with credentials
                    'aws_access_key_id={access_key};aws_secret_access_key={secret_key}'
                    {copy_options};
                """.format(table=self.table,
                           s3_bucket=self.s3_bucket,
                           rendered_key=rendered_key,
                           access_key=credentials.access_key,
                           secret_key=credentials.secret_key,
                           copy_options=self.copy_options)

        self.log.info('Start copy command...')
        redshift_hook.run(copy_query)
        self.log.info("COPY command complete.")




