from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)      
        
        quality_checks_number=[
            {'table': 'artists'}, {'table': 'songs'}, {'table': 'time'}, {'table': 'users'}
        ]
        
        for check in quality_checks_number:
            sql = f"SELECT COUNT(*) FROM {check['table']}"
            records = redshift_hook.get_records(sql)
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed! The {check['table']} table is empty")
                
        quality_checks_null_ids=[
            {'table': 'artists', 'id_name': 'artistid'},
            {'table': 'users',   'id_name': 'userid'},
            {'table': 'songs',   'id_name': 'songid'},
        ]
        
        for check in quality_checks_null_ids:
            sql = f"SELECT COUNT(*) FROM {check['table']} WHERE {check['id_name']} IS NULL"
            records = redshift_hook.get_records(sql)
            if len(records) == 0:
                raise ValueError(f"Data quality check failed. The table {check['table']} should not have an null id")
        
        
        self.log.info("Data Quality Check Completed Succesfully")