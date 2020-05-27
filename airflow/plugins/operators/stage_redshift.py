from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    
    ui_color = '#358140'
    
    copy_sql_template = """
	COPY {} FROM '{}' 
	IAM_ROLE '{}'
	TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
	TIMEFORMAT AS 'epochmillisecs'
	JSON '{}'
	COMPUPDATE OFF
	REGION '{}'
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 redshift_iam_role="",
                 table="",
                 s3_bucket="",
                 s3_prefix="",
                 json_location="auto",
                 region="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.redshift_iam_role = redshift_iam_role
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix
        self.json_location= json_location
        self.region = region
        
    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info("Clearing data from destination Redshift table")
        redshift.run("TRUNCATE TABLE {}".format(self.table))
        
        self.log.info("Copying Staging data to Redshift")
        s3_path = "s3://{}/{}".format(self.s3_bucket, self.s3_prefix)
        formatted_sql = StageToRedshiftOperator.copy_sql_template.format(
                self.table,
                s3_path,
                self.redshift_iam_role,
                self.json_location,
                self.region
        )
        redshift.run(formatted_sql)