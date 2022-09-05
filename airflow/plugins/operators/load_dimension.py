from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 table,
                 redshift_conn_id='redshift',
                 select_sql='',
                 write_mode='append',
                 *args, **kwargs):
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.select_sql = select_sql
        self.write_mode = write_mode

    def execute(self, context):
        self.log.info('LoadDimensionOperator not implemented yet')
        redshift_hook = PostgresHook("redshift")

        if self.write_mode == 'truncate':
            self.log.info(f'Deleting data from {self.table} dimension table...')
            redshift_hook.run(f'DELETE FROM {self.table};')
            self.log.info("Deletion complete.")

        exec_sql = f"""
            INSERT INTO {self.table}
            {self.select_sql}
        """
        self.log.info(f'Loading data into {self.table} dim table')

        redshift_hook.run(exec_sql)
        self.log.info('Loading dim table complete.')
