from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='redshift',
                 check_statements=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.check_statements = check_statements

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)

        for statement in self.check_statements:
            result = int(redshift_hook.get_first(sql=statement['sql'])[0])

            # check if equal
            if statement['op'] == 'eq':
                if result != statement['value']:
                    raise AssertionError(f"Check failed: {result} {statement['op']} {statement['value']}")
            # check if not equal
            elif statement['op'] == 'ne':
                if result == statement['value']:
                    raise AssertionError(f"Check failed: {result} {statement['op']} {statement['value']}")
            # check if greater than
            elif statement['op'] == 'gt':
                if result <= statement['value']:
                    raise AssertionError(f"Check failed: {result} {statement['op']} {statement['value']}")

            self.log.info(f"Passed check: {result} {statement['op']} {statement['value']}")