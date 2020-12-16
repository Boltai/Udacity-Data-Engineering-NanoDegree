from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    ui_color = '#F98866'
    insert_sql = """
    INSERT INTO {} {}
    {}
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table = "",
                 sql_query = "",
                 insert_statement = "",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_query = sql_query
        self.insert_statement = insert_statement

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        lead_fact_sql = LoadFactOperator.insert_sql.format(
            self.table,
            self.insert_statement,
            self.sql_query
        )
        self.log.info(f"Running load on table {self.table}")      
        redshift_hook.run(lead_fact_sql)
