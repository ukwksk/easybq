# easybq | Easy BigQuery

## Install
```bash
pip install easybq
```

If you upload file via gcs, you may install such as
```bash
pip insrtall easybq[gcs]
``` 

## Usage
```python
import easybq

dataset = 'sample_dataset'
table = 'sample_table'
csvfile = 'sample.csv'

bq = easybq.Client()

schema = bq.get_schema(dataset, table)
bq.upload_csv(filename=csvfile, 
              dataset=dataset, table=table, schema=schema)
```
