from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    copy_sql = '''
        copy {table} from '{s3_path}'
        access_key_id '{access_key_id}'
        secret_access_key '{secret_access_key}'
        json '{json_configuration}'
        region '{region}'
        timeformat as '{timeformat}';
    '''

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 aws_credentials_id='',
                 table='',
                 s3_bucket='',
                 s3_key='',
                 json_configuration='',
                 region='us-west-2',
                 timeformat='epochmillisecs',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json_configuration = json_configuration
        self.region = region
        self.timeformat = timeformat

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Clear the destination Redshift table")
        redshift.run("DELETE FROM {}".format(self.table))

        self.log.info("Copy data from S3 to Redshift")
        s3_path = "s3://{}/{}".format(self.s3_bucket, self.s3_key)
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            table=self.table,
            s3_path = s3_path,
            access_key_id=credentials.access_key,
            secret_access_key=credentials.secret_key,
            json_configuration=self.json_configuration,
            region=self.region,
            timeformat=self.timeformat
        )
        redshift.run(formatted_sql)





