import os
import sys

import sqlalchemy
import yaml
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from pathlib import Path
from thunder import citer


def load_config(source_file):
    config_file = Path(source_file).with_suffix('.yml')
    config = {
        'delimiter': ',',
        'table_name': Path(source_file).stem,
        'quoted': False,
        'chunk_size': 10000
    }
    if config_file.exists():
        with open(config_file) as f:
            user_config = yaml.safe_load(f).get('config', {})
            config.update(user_config)
    return config


def validate_csv(file_path, config):
    if not Path(file_path).exists():
        raise FileNotFoundError(f"source file {file_path} not found in data dir")
    try:
        pd.read_csv(file_path, nrows=1, sep=config['delimiter'], quoting=3 if not config['quoted'] else 1)
    except Exception as e:
        raise ValueError(f"invalid csv format: {str(e)}")


def get_db_engine():
    load_dotenv()
    required_env = ['DB_HOST', 'DB_PORT', 'DB_NAME', 'DB_SCHEMA', 'DB_USER', 'DB_PASS']
    if not all(os.getenv(var) for var in required_env):
        raise EnvironmentError("missing one or more env var")
    return create_engine(
        f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASS')}@"
        f"{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
    )


def main():
    if len(sys.argv) != 2:
        print("usage: python ingest.py <source_file>")
        sys.exit(1)
    source_file = 'data/' + sys.argv[1] + '.csv'
    config = load_config(source_file)
    validate_csv(source_file, config)
    total_rows = sum(1 for _ in open(source_file)) - 1  # -1 for header
    print(f'found {total_rows} rows')
    engine = get_db_engine()
    table_name = f"{os.getenv('DB_SCHEMA')}.{config['table_name']}"
    first_chunk = True
    loaded_rows = 0
    for chunk in pd.read_csv(source_file,
                             sep=config['delimiter'],
                             quoting=3 if not config['quoted'] else 1,
                             chunksize=config['chunk_size'],
                             dtype=str):
        chunk = chunk.replace('\xa0', ' ', regex=True).infer_objects(copy=False)
        chunk.to_sql(
            config['table_name'],
            engine,
            schema=os.getenv('DB_SCHEMA'),
            if_exists='replace' if first_chunk else 'append',
            index=False,
            dtype={col: sqlalchemy.types.Text for col in chunk.columns}
        )
        first_chunk = False
        loaded_rows += len(chunk)

    with engine.connect() as conn:
        result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
        db_count = result.scalar()
    if db_count != total_rows:
        raise ValueError(f"row count mismatch: CSV={total_rows}, DB={db_count}")


if __name__ == "__main__":
    main()
