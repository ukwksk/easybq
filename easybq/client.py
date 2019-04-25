# -*- coding: utf-8 -*-
import os
from collections import OrderedDict
from logging import getLogger

from google.api_core import exceptions
from google.cloud import bigquery
from google.cloud.bigquery import TimePartitioning
from google.cloud.exceptions import NotFound, BadRequest

logger = getLogger('easybq')

CREDENTIALS = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')

WRITE_APPEND = 'WRITE_APPEND'
WRITE_TRUNCATE = 'WRITE_TRUNCATE'
WRITE_EMPTY = 'WRITE_EMPTY'


class Client:
    """
    scope:
        bigquery.tables.create
        bigquery.jobs.create
    """

    def __init__(self, credentials=None, default_location='US'):
        logger.debug(f"CREDENTIALS: {credentials}")
        self._credentials = credentials or CREDENTIALS
        self._client = bigquery.Client\
            .from_service_account_json(self._credentials)
        self.default_location = default_location

    @property
    def project(self):
        return self._client.project

    @classmethod
    def job_config_csv(cls, schema=None, autodetect=False, delimiter=',',
                       skip_leading_rows=0,
                       write_disposition=WRITE_APPEND, null_maker=''):
        job_config = bigquery.LoadJobConfig()
        job_config.autodetect = autodetect or not bool(schema)
        if not job_config.autodetect:
            job_config.schema = schema

        job_config.source_format = bigquery.SourceFormat.CSV
        job_config.field_delimiter = delimiter
        job_config.allow_quoted_newlines = True
        job_config.skip_leading_rows = skip_leading_rows
        job_config.null_marker = null_maker

        job_config.write_disposition = write_disposition

        return job_config

    def dataset_ref(self, dataset_id):
        return self._client.dataset(dataset_id)

    def dataset(self, dataset_id):
        return self._client.get_dataset(self.dataset_ref(dataset_id))

    def table_ref(self, dataset_id, table_id):
        return self.dataset_ref(dataset_id).table(table_id)

    def table(self, dataset_id, table_id):
        try:
            return self._client.get_table(self.table_ref(dataset_id, table_id))

        except NotFound:
            return None

    def query(self, query):
        job = self._client.query(query)

        for row in job.result():
            yield OrderedDict(row.items())

    def is_valid_query(self, query):
        job_config = bigquery.QueryJobConfig()
        job_config.dry_run = True
        job_config.use_query_cache = False

        try:
            job = self._client.query(query, job_config=job_config)
            return job.state == "DONE", job
        except BadRequest as e:
            return False, e

    def get_schema(self, dataset_id, table_id):
        return self.table(dataset_id, table_id).schema

    def upload_csv(self, filename, dataset, table,
                   schema=None,
                   location=None,
                   skip_leading_rows=0,
                   write_disposition=WRITE_APPEND,
                   null_maker=''):
        return self._upload_csv(filename, dataset, table,
                                schema=schema,
                                location=location,
                                skip_leading_rows=skip_leading_rows,
                                write_disposition=write_disposition,
                                null_maker=null_maker)

    def upload_tsv(self, filename, dataset, table,
                   schema=None,
                   location=None,
                   skip_leading_rows=0,
                   write_disposition=WRITE_APPEND,
                   null_maker=''):
        return self._upload_csv(filename, dataset, table, delimiter='\t',
                                schema=schema,
                                location=location,
                                skip_leading_rows=skip_leading_rows,
                                write_disposition=write_disposition,
                                null_maker=null_maker)

    def upload_csv_from_uri(self, uri, dataset, table,
                            schema=None,
                            location=None,
                            skip_leading_rows=0,
                            write_disposition=WRITE_APPEND,
                            null_maker=''):
        return self._upload_csv_from_uri(uri, dataset, table,
                                         schema=schema,
                                         location=location,
                                         skip_leading_rows=skip_leading_rows,
                                         write_disposition=write_disposition,
                                         null_maker=null_maker)

    def upload_tsv_from_uri(self, uri, dataset, table,
                            schema=None,
                            location=None,
                            skip_leading_rows=0,
                            write_disposition=WRITE_APPEND,
                            null_maker=''):
        return self._upload_csv_from_uri(uri, dataset, table, delimiter='\t',
                                         schema=schema,
                                         location=location,
                                         skip_leading_rows=skip_leading_rows,
                                         write_disposition=write_disposition,
                                         null_maker=null_maker)

    def _upload_csv(self, filename, dataset_id, table_id,
                    delimiter=',',
                    schema=None,
                    location=None,
                    skip_leading_rows=0,
                    write_disposition=WRITE_APPEND,
                    null_maker=''):
        autodetect = False
        if not schema:
            if self.table(dataset_id, table_id) is None:
                autodetect = True
            else:
                schema = self.get_schema(dataset_id, table_id)

        job_config = self.job_config_csv(
            schema, autodetect, delimiter,
            skip_leading_rows, write_disposition, null_maker)

        job = None
        try:
            with open(filename, 'rb') as f:
                job = self._client.load_table_from_file(
                    f, self.table_ref(dataset_id, table_id),
                    location=location or self.default_location,
                    job_config=job_config)  # API request

            job.result()  # Waits for table load to complete.
        except Exception as e:
            if job:
                logger.error(f"Errors: \n{job.errors}")
                return job

            raise

        logger.info(f'Loaded {job.output_rows} rows '
                    f'into {self.table_ref(dataset_id, table_id)}.')
        return job.output_rows

    def _upload_csv_from_uri(self, uri, dataset_id, table_id,
                             delimiter=',',
                             schema=None,
                             location=None,
                             skip_leading_rows=0,
                             write_disposition=WRITE_APPEND,
                             null_maker=''):
        autodetect = False
        if not schema:
            if self.table(dataset_id, table_id) is None:
                autodetect = True
            else:
                schema = self.get_schema(dataset_id, table_id)

        job_config = self.job_config_csv(
            schema, autodetect, delimiter,
            skip_leading_rows, write_disposition, null_maker)

        job = None
        try:
            job = self._client.load_table_from_uri(
                uri, self.table_ref(dataset_id, table_id),
                location=location or self.default_location,
                job_config=job_config)  # API request

            job.result()  # Waits for table load to complete.
        except Exception as e:
            if job:
                logger.error(f"Errors: \n{job.errors}")
                return job

            raise

        logger.info(f'Loaded {job.output_rows} rows '
                    f'into {tblrep(self.table_ref(dataset_id, table_id))}.')
        return job

    def create_update_table(self, dataset, table, schema,
                            time_partitioning=None, clustering_fields=None):
        """
        :param dataset:
        :param table:
        :param filename: Absolute path of csv file
        :param schema: List<SchemaField>
        :param time_partitioning:
        :param clustering_fields:
        :return:
        """
        ds_ref = self._client.dataset(dataset)
        ds = self._client.get_dataset(ds_ref)

        tbl_ref = ds.table(table)

        try:
            # https://cloud.google.com/bigquery/docs/managing-table-schemas
            tbl = self._client.get_table(tbl_ref)
            logger.warning(f"*** {dataset}.{table} already exists. "
                           f"Existing fields DO NOT updated. ***")
            add_scm = [s for s in schema
                       if s.name not in [e.name for e in tbl.schema]]

            if not add_scm:
                logger.info(f"Nothing to Append.")
                res = None

            else:
                logger.info(f"Append Field: {[f.name for f in add_scm]}")

                add_scm = tbl.schema + add_scm
                tbl.schema = add_scm

                logger.info(f"Update Table Schema: {tblrep(tbl_ref)}, "
                            f"Schema: {add_scm}")
                res = self._client.update_table(tbl, ['schema'])

        except exceptions.NotFound:
            # https://cloud.google.com/bigquery/docs/tables#creating_an_empty_table_with_a_schema_definition
            add_scm = schema
            tbl = bigquery.Table(tbl_ref, schema=add_scm)
            if time_partitioning:
                logger.info(f"TimePartitioning: {time_partitioning}")
                tbl.time_partitioning = TimePartitioning(
                    field=time_partitioning)

            if clustering_fields:
                if not time_partitioning:
                    logger.warning(f"Clustering needs TimePartitioning.")
                else:
                    logger.info(f"Clustering: {clustering_fields}")
                    tbl.clustering_fields = clustering_fields

            logger.info(f"Create Table: {tblrep(tbl)}")

            res = self._client.create_table(tbl)  # API request

        tbl = self._client.get_table(tbl_ref)
        if add_scm:
            logger.info(f"Renewed Table: {dataset}.{table}: "
                        f"{[(f.name, f.field_type) for f in tbl.schema]}")
        return res

    def create_view(self, dataset, table, sql, exist_ok=False):
        tbl = self.table(dataset, table)
        if tbl and not exist_ok:
            raise AttributeError(
                f"{self.project}.{dataset}.{table} already exists.")

        if not tbl:
            tbl = bigquery.Table(self.table_ref(dataset, table))
            tbl.view_query = sql
            tbl = self._client.create_table(tbl)
        else:
            tbl.view_query = sql
            self._client.update_table(tbl, ['view_query'])

        return self.table(dataset, table)

    def update_schema(self, dataset, table, schema):
        tbl = self.table(dataset, table)
        if not tbl:
            raise AttributeError(
                f"{self.project}.{dataset}.{table} is not found.")

        tbl.schema = schema
        return self._client.update_table(tbl, ['schema'])


def tblrep(tbl):
    return f'{tbl.project}.{tbl.dataset_id}.{tbl.table_id}'
