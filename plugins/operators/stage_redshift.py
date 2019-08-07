from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    """Copy JSON files from S3 to a Redshift staging table
    
    Parameters
    ----------
    redshift_conn_id : name of Redshift connection defined in Airflow
    aws_iam_role : Redshift role ARN (Amazon Resource Name)
    table : name of the Redshift staging table to copy to
    s3_bucket : name of the bucket where JSON files reside
    s3_key : prefix of the JSON files
    json_path : dictionary mapping JSON object fields to table columns
    delete_insert : if True, table contents are deleted before copying
    """
    ui_color = '#358140'

    template_fields = ('s3_key',) 
    
    copy_sql = """
        COPY {}
        FROM '{}'
        credentials '{}'
        json '{}';
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 aws_iam_role='',
                 table='',
                 s3_bucket='',
                 s3_key='',
                 json_path='',
                 delete_insert=True,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.aws_iam_role=aws_iam_role
        self.table=table
        self.s3_bucket=s3_bucket
        self.s3_key=s3_key
        self.json_path=json_path
        self.delete_insert=delete_insert

    def execute(self, context):
        #self.log.info('StageToRedshiftOperator not implemented yet')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        rendered_key = self.s3_key.format(**context)
        s3_path = 's3://{}/{}'.format(self.s3_bucket, rendered_key)
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path, 
            self.aws_iam_role,
            self.json_path
            )
        if self.delete_insert:
            redshift.run("DELETE FROM {}".format(self.table))
        redshift.run(formatted_sql)