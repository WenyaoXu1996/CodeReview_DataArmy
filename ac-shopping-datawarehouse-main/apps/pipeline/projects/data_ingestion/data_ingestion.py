import json
import sys
import os
from utils.tasks import BaseTask
from utils.s3_connector import S3Connector
from utils.postgressql_connector import PostgresSqlConnector
from projects.data_ingestion.data_ingestion_config import (
    DataIngestionConfig,
    PipelineConfig,
    TableConfig,
)
import pandas
from datetime import datetime
from utils.ssm import update_parameter_value


class DataIngestionTask(BaseTask):
    def __init__(
        self, task_name, config_path=None, table_name=None, config_group=None,
    ):
        self.config_path = config_path
        self.table_name = table_name
        self.config_group = config_group
        self.s3_con = S3Connector()
        BaseTask.__init__(self, task_name)

    def args(self, parser):
        task_config = parser.add_argument_group("Task Arguments")
        task_config.add_argument(
            "-c",
            "--config_path",
            type=str,
            help="YAML config file path.",
            required=False,
        )

        task_config.add_argument(
            "-t",
            "--table_name",
            type=str,
            help="""
                specific table to process in YAML (don't set parameter to run all)
            """,
            required=False,
        )

        args, _ = parser.parse_known_args()
        self.s3_con.configure()

    def configure(self, args):
        self.config_path = self.config_path or args.config_path
        self.table_name = self.table_name or args.table_name

    def _get_connection(self, credentials):

        return PostgresSqlConnector(credentials)

    def create_table(
        self,
        source_schema,
        source_table,
        destination_schema,
        destionation_table,
        source_connection,
        destination_connection,
    ):

        df = source_connection.get_column_metadata(source_table, source_schema)

        schema_sql = "create schema if not exists {};".format(destination_schema)
        destination_connection.execute_sql(schema_sql)

        table_sql = "create table if not exists {}.{} (".format(
            destination_schema, destionation_table
        )

        row_sql_list = []
        for row in df.itertuples(index=False):
            # apply source to Redshift data type mappings
            row_sql = self._map_to_redshift_column(
                column=row.column_name,
                data_type=row.data_type,
                col_type=row.column_type,
                char_max_length=row.character_maximum_length,
                num_precision=row.numeric_precision,
                num_scale=row.numeric_scale,
            )

            row_sql_list.append(row_sql)

        create_table_sql = table_sql + "\n" + ",\n".join(row_sql_list) + ")"
        print(create_table_sql)

        destination_connection.execute_sql(create_table_sql)

    def _map_to_redshift_column(
        self, column, data_type, col_type, char_max_length, num_precision, num_scale,
    ):
        # character data types
        if data_type in [
            "varchar",
            "char",
            "character varying",
            "character",
        ]:
            return f"{column} {data_type}({int(char_max_length)}) encode zstd"
        # supported data types that can use az64
        elif data_type in [
            "bigint",
            "timestamp without time zone",
            "integer",
            "smallint",
        ]:
            return f"{column} {data_type} encode az64"
        # other supported data types
        elif data_type in [
            "boolean",
            "double precision",
            "real",
            "timestamp with time zone",
            "date",
            "time",
            "timetz",
            "time without time zone",
            "time with time zone",
        ]:
            return f"{column} {data_type} encode zstd"
        # data types which require precision and scale
        elif data_type in ["numeric"]:
            return f"{column} {data_type}({int(num_precision)},{int(num_scale)}) encode az64"
        else:
            return f"{column} varchar(max) encode zstd"

    def extract_table_data(self, db_connection, table_config, pipe_config):
        extract_table_sql = "select {column_inclusions} from {schema_name}.{table_name}".format(
            column_inclusions=",".join(table_config.column_inclusions),
            schema_name=pipe_config.source_schema,
            table_name=table_config.source_table,
        )

        if table_config.update_method == "incremental_load":
            extract_table_sql = (
                extract_table_sql
                + "\n where "
                + " >= '{}' or ".format(table_config.incremental_load_parameter).join(
                    table_config.incremental_load_columns
                )
                + " >= '{}'".format(table_config.incremental_load_parameter)
            )
        ##print(extract_table_sql)
        db_connection.export_sql_to_csv(
            extract_table_sql, table_config.export_file_name, ","
        )

    def upload_to_s3(self, table_config):
        self.s3_con.upload_file(
            file_name=table_config.export_file_name,
            object_name="ac_shopping_crm/{}".format(table_config.export_file_name),
        )

    def copy_to_staging(self, table_config, pipe_config, redshift_conn):
        copy_cmd = """
            truncate {}.{};
            copy {}.{}
            from 's3://{}/ac_shopping_crm/{}'
            credentials 'aws_iam_role=arn:aws:iam::721495903582:role/redshift-admin'
            csv
            delimiter ','
            ignoreheader 1""".format(
            pipe_config.staging_schema,
            table_config.destination_table,
            pipe_config.staging_schema,
            table_config.destination_table,
            pipe_config.s3_bucket_name,
            table_config.export_file_name,
        )
        redshift_conn.execute_sql(copy_cmd)

    def upsert_into_destination_table(self, table_config, pipe_config, redshift_conn):
        update_keys_string = ""

        for column in table_config.update_keys:
            string = "st.{}".format(column) + "={}.{}.{}".format(
                pipe_config.destination_schema, table_config.destination_table, column
            )
            update_keys_string = update_keys_string + string + " and "[0:-4]

        upsert_sql = """
        delete from {}.{}
            where exists(select 1 from {}.{} st where ({}));
            insert into {}.{}
            select * from {}.{}
        """.format(
            pipe_config.destination_schema,
            table_config.destination_table,
            pipe_config.staging_schema,
            table_config.staging_table,
            update_keys_string,
            pipe_config.destination_schema,
            table_config.destination_table,
            pipe_config.staging_schema,
            table_config.staging_table,
        )
        print(upsert_sql)
        redshift_conn.execute_sql(upsert_sql)

    def update_incremental_parameter_value(
        self, table_config, pipe_config, redshift_conn
    ):
        union_list = []

        for incremental_load_column in table_config.incremental_load_columns:
            union_list.append(
                "select max({}) as last_loaded_date from {}.{}".format(
                    incremental_load_column,
                    pipe_config.destination_schema,
                    table_config.destination_table,
                )
            )
        get_parameter_value_sql = (
            "select dateadd(day,-2,min(last_loaded_date)) from ( "
            + " union all ".join(union_list)
            + ")"
        )

        parameter_value = str(
            redshift_conn.get_execute_sql_result(get_parameter_value_sql)[0][0]
        )
        update_parameter_value(table_config.parameter_name, parameter_value)

    def main(self):
        ##self.s3_con.list_bucket()
        main_config = DataIngestionConfig(self.config_path, table_id=self.table_name)
        pipe_config = main_config.get_pipeline_config()
        table_config_list = main_config.get_table_config()

        postgre_conn = self._get_connection(pipe_config.source_credentials)

        redshift_conn = self._get_connection(pipe_config.destination_credentials)

        # for table_config in table_config_list:
        #     print(table_config.incremental_load_parameter)

        for table_config in table_config_list:

            if not redshift_conn.table_exists(
                table_config.destination_table, pipe_config.staging_schema,
            ):
                self.create_table(
                    pipe_config.source_schema,
                    table_config.source_table,
                    pipe_config.staging_schema,
                    table_config.destination_table,
                    postgre_conn,
                    redshift_conn,
                )

            if not redshift_conn.table_exists(
                table_config.destination_table, pipe_config.destination_schema,
            ):
                self.create_table(
                    pipe_config.source_schema,
                    table_config.source_table,
                    pipe_config.destination_schema,
                    table_config.destination_table,
                    postgre_conn,
                    redshift_conn,
                )

            self.extract_table_data(postgre_conn, table_config, pipe_config)

            self.upload_to_s3(table_config)

            self.copy_to_staging(table_config, pipe_config, redshift_conn)

            self.upsert_into_destination_table(table_config, pipe_config, redshift_conn)

            self.update_incremental_parameter_value(
                table_config, pipe_config, redshift_conn
            )

