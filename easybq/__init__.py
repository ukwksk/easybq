# -*- coding: utf-8 -*-
import os
from collections import OrderedDict
from logging import getLogger

from google.api_core import exceptions
from google.cloud import bigquery
from google.cloud.bigquery import SchemaField, TimePartitioning

logger = getLogger('easybq.bq')

CREDENTIALS = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')


class Client:
    """
    scope:
        bigquery.tables.create
        bigquery.jobs.create
    """

    def __init__(self, credentials=None, default_location='US'):
        logger.debug(f"CREDENTIALS: {credentials}")
        self._credentials = credentials or CREDENTIALS
        self._client = bigquery.Client.from_service_account_json(credentials)
        self.default_location = default_location

    @property
    def project(self):
        return self._client.project

    def query(self, query):
        job = self._client.query(query)
        for row in job.result():
            yield OrderedDict(row.items())

    def get_schema(self, dataset, table, with_type=False):
        ds = self._client.dataset(dataset_id=dataset)
        tbl_ref = ds.table(table_id=table)
        tbl = self._client.get_table(tbl_ref)

        if with_type:
            return tbl.schema
        else:
            return [t.name for t in tbl.schema]

    def create_update_table(self, dataset, table, schema,
                            time_partitioning=None, clustering_fields=None):
        """
        :param dataset:
        :param table:
        :param filename: Absolute path of csv file
        :param schema: List<Dict>
            See: /path/to/site-packages/google/cloud/bigquery/schema.py
            name (str): the name of the field.
            field_type (str): STRING, INTEGER, FLOAT, BOOLEAN,
                                TIMESTAMP, DATE, DATETIME and more.
                See: https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#schema.fields.type
            mode (str): NULLABLE, REQUIRED and REPEATED
            description (Optional[str]):description for the field.
            fields (Tuple[:class:`~google.cloud.bigquery.schema.SchemaField`]):
                subfields (requires ``field_type`` of 'RECORD').
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
            add_scm = [SchemaField(**s) for s in schema
                       if s['name'] not in [e.name for e in tbl.schema]]

            if not add_scm:
                logger.info(f"Nothing to Append.")
                res = None

            else:
                logger.info(f"Append Field: {[f.name for f in add_scm]}")

                add_scm = tbl.schema + add_scm
                tbl.schema = add_scm

                logger.info(f"Update Table Schema: {self.tblrep(tbl_ref)}, "
                            f"Schema: {add_scm}")
                res = self._client.update_table(tbl, ['schema'])

        except exceptions.NotFound:
            # https://cloud.google.com/bigquery/docs/tables#creating_an_empty_table_with_a_schema_definition
            add_scm = [SchemaField(**s) for s in schema]
            tbl = bigquery.Table(tbl_ref, schema=add_scm)
            if time_partitioning:
                logger.info(f"TimePartitioning: {time_partitioning}")
                tbl.time_partitioning = TimePartitioning(field=time_partitioning)

            if clustering_fields:
                if not time_partitioning:
                    logger.warning(f"Clustering needs TimePartitioning.")
                else:
                    logger.info(f"Clustering: {clustering_fields}")
                    tbl.clustering_fields = clustering_fields

            logger.info(f"Create Table: {self.tblrep(tbl)}")

            res = self._client.create_table(tbl)  # API request

        tbl = self._client.get_table(tbl_ref)
        if add_scm:
            logger.info(f"Renewed Table: {dataset}.{table}: "
                        f"{[(f.name, f.field_type) for f in tbl.schema]}")
        return res

    def upload_csv(self, dataset, table, filename, location=None,
                   skip_leading_rows=1, autodetect=False,
                   write_disposition=None):
        """
        *Append* csv data into the table
        :param dataset:
        :param table:
        :param filename: Absolute path of csv file
        :param skip_leading_rows:
        :param location:
        :param autodetect:
        :param write_disposition:
        :return:
        """
        with open(filename, 'r') as f:
            n_records = len(f.readlines()) - skip_leading_rows
            logger.info(f"Import {n_records} records.")

        if not n_records:
            return 0

        location = location or self.default_location

        ds = self._client.dataset(dataset_id=dataset)
        tbl_ref = ds.table(table_id=table)

        job_config = bigquery.LoadJobConfig()

        job_config.source_format = bigquery.SourceFormat.CSV
        job_config.allow_quoted_newlines = True
        job_config.skip_leading_rows = skip_leading_rows

        if autodetect:
            job_config.autodetect = autodetect
        else:
            job_config.schema = self.get_schema(dataset, table, with_type=True)

        if write_disposition:
            job_config.write_disposition = write_disposition

        job = None
        try:
            if autodetect:
                logger.info(f'Upload csv {filename} into {self.tblrep(tbl_ref)}'
                            f' with autodetect.')
            else:
                logger.info(f'Upload csv {filename} into {self.tblrep(tbl_ref)}'
                            f' with schema: {job_config.schema}.')
            with open(filename, 'rb') as f:
                job = self._client.load_table_from_file(
                    f,
                    tbl_ref,
                    location=location,
                    job_config=job_config)  # API request

            job.result()  # Waits for table load to complete.
        except Exception as e:
            if job:
                logger.error(f"Errors: \n{job.errors}")

            logger.exception(e)
            raise

        logger.info(f'Loaded {job.output_rows} rows '
                    f'into {self.tblrep(tbl_ref)}.')
        return job.output_rows

    @classmethod
    def tblrep(cls, tbl):
        return f'{tbl.project}.{tbl.dataset_id}.{tbl.table_id}'