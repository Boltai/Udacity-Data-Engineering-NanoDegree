from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    insert_sql = """
    INSERT INTO {} {}
    {}
    """
    
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table = "",
                 sql_query = "",
                 insert_statement = "",
                 delete_check = True,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_query = sql_query
        self.insert_statement = insert_statement
        self.delete_check = delete_check

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        self.log.info(f"Running delete on table {self.table}")
        
        if self.delete_check:
            self.log.info(f"Running delete on table {self.table}")      
            redshift_hook.run(f"DELETE FROM {self.table}")
        
        load_dim_sql = LoadDimensionOperator.insert_sql.format(
            self.table,
            self.insert_statement,
            self.sql_query
        )        
                      
        self.log.info(f"Running load on table {self.table}")                      
        redshift_hook.run(load_dim_sql)
