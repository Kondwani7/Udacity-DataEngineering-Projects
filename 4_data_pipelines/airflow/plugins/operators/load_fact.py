from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#c434e0'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id="",
                 table="",
                 truncate_table=True,
                 query="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.table = table,
        self.redshift_conn_id = redshift_conn_id
        self.truncate_table = truncate_table
        self.query = query
        # Map params here
        # Example:
        # self.conn_id = conn_id

    def execute(self, context):
        self.log.info('Implementing LoadFactOperator')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if self.truncate_table:
            self.log.info(f"Truncating Table {self.table}")
            redshift.run("DELETE FROM {}".format(self.table))
        #running the query to insert the data to the fact table
        self.log.info(f"Running query {self.query}")
        redshift.run(f"insert into {self.table} {self.query}")
