from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 quality_check_tables='',
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.quality_check_tables = quality_check_tables

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)      
        
        for check in self.quality_check_tables:
            records = redshift.get_records(check['sql'])[0]
            if check['type'] == 'eq' and records[0] != check['comparison']:
                raise ValueError(f"Data quality check failed! The query {check['sql']} expected {check['comparison']}, but received {records[0]}")
            elif check['type'] == 'gt' and records[0] <= check['comparison']:
                raise ValueError(f"Data quality check failed! The query {check['sql']} expected a value greater the  {check['comparison']}, but received {records[0]}")
        
        
        self.log.info("Data Quality Check Completed Succesfully")