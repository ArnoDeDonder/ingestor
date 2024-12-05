# Simple data ingestor from CSV to PostgreSQL

### Usage:
- create a `.env` file with your database credentials as shown below
- create a directory `data` in the project folder
- copy your `source_file.csv` to `data`
- create a `source_file.yml` config file with the same name as the source file
- include the config below (a missing config file or missing entries will get the default settings)
- run with: `python ingestor source_file`

### Example .env file
```
DB_HOST=...
DB_PORT=...
DB_NAME=...
DB_SCHEMA=...
DB_USER=...
DB_PASS=...
```

### Example config with the default value
```yaml
config:
  delimiter: ','
  table_name: source_file
  quoted: false
  chunk_size: 10000
```