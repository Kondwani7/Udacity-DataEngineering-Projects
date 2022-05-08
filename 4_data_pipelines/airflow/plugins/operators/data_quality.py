from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 redshift_conn_id="",
                 dq_checks=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.dq_checks =  dq_checks
        self.redshift_conn_id = redshift_conn_id
       

    def execute(self, context):
        self.log.info(' Implementing DataQualityOperator')
        redshift_hook = PostgresHook("redshift")
        #implementing checks
        for check in dq_checks:
            sql = check["check_sql"]
            exp = check["expected_result"]
            records = redshift_hook.get_records(sql)
            num_records = records[0][0]
            if num_records != exp:
                raise ValueError(f"Data quality check failed. Expected: {exp} | Got: {num_records}")
            else:
                self.log.info(f"Data quality on SQL {sql} check passed with {records[0][0]} records")
                
                    
                    
                    
                    