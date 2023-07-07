import os
from pathlib import Path

ENV = 'debug'

if ENV == 'prod':
    DEFAULT_STORAGE_DIR = Path('/usr/local/pynosql/data')
else:
    DEFAULT_STORAGE_DIR = Path(__file__).parent.parent / 'data'

if not os.path.exists(DEFAULT_STORAGE_DIR):
    DEFAULT_STORAGE_DIR.mkdir(parents=True, exist_ok=True)
