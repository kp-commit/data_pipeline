from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    
    truncate_sql_template = """
        TRUNCATE TABLE {table}
    """
    
    insert_sql_template = """
        INSERT INTO {table}
            {sub_query}
    """ 

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 table='', 
                 truncate=True,
                 sub_query='',
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.truncate = truncate
        self.sub_query = sub_query

    def execute(self, context):
        self.log.info('LoadDimensionOperator running for table: \'{}\''
                      .format(self.table))
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        # Truncate Table
        if self.truncate:
            self.log.info('Truncating \'{}\''.format(self.table))
            redshift.run(LoadDimensionOperator.truncate_sql_template.format(
                         table=self.table))
        
        # Run Insert
        self.log.info('Loading data into \'{}\''.format(self.table))
        redshift.run(LoadDimensionOperator.insert_sql_template.format(
                     table=self.table,
                     sub_query=self.sub_query))
