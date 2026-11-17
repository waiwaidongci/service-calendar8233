import os, json
from pathlib import Path

def _config_path():
    configured = os.getenv('PROJECT_CONFIG')
    if configured:
        return Path(configured)
    container = Path('/app/project.json')
    if container.exists():
        return container
    return Path(__file__).resolve().parents[2] / 'project.json'

CONFIG = json.loads(_config_path().read_text())
DATABASE_URL = os.environ['DATABASE_URL']
REDIS_URL = os.getenv('REDIS_URL', 'redis://redis:6379/0')
SECRET = os.environ['JWT_SECRET']
