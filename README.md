# Simple data ingestor from CSV to PostgreSQL

### Usage:
- create a directory 'data' in the project folder
- copy your source_file.csv to it
- create a source_file.yml config file with the same name as the source file
- include the config below (a missing config file or missing entries will get the default settings)
- run with: `python ingestor source_file`

### Example config with the default value
config:
  delimiter: ','
  table_name: source_file
  quoted: false
  chunk_size: 10000