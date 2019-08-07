from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    """Insert into a star schema dimension table from (mainly) staging ones
    
    Parameters
    ----------
    redshift_conn_id : name of Redshift connection defined in Airflow
    table : name of the Redshift star schema table to insert into
    insert_statement : predefined insert statement as SQL string
    delete_insert : if True, table contents are deleted before insert
    """

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 table='',
                 insert_statement='',
                 delete_insert=True,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.table=table
        self.insert_statement=insert_statement
        self.delete_insert=delete_insert

    def execute(self, context):
        #self.log.info('LoadDimensionOperator not implemented yet')
        redshift=PostgresHook(postgres_conn_id=self.redshift_conn_id)
        formatted_sql='INSERT INTO {} {}'.format(self.table, self.insert_statement)
        if self.delete_insert:
            redshift.run("DELETE FROM {}".format(self.table))
        redshift.run(formatted_sql)