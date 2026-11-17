import time,json,traceback
from pathlib import Path
import redis
from src.config.database import Base,engine,Session
from src.config.settings import REDIS_URL
from src.models import identity,source,run,clock
from src.models.run import Run
from src.services.report import execute
from src.services.delivery import tick
from src.services.clock import process_due_jobs
Base.metadata.create_all(engine)
r=redis.Redis.from_url(REDIS_URL)
while True:
 Path('/tmp/worker-heartbeat').write_text(str(time.time()))
 try:
  item=r.lpop('report-jobs')
  if item:
   with Session() as db:
    job=db.get(Run,int(item))
    if job and job.status=='queued':
     job.status='running';db.commit()
     try: job.result=execute(job,job.snapshot);job.status='completed'
     except Exception as exc: job.status='failed';job.error=str(exc);traceback.print_exc()
     db.commit()
  process_due_jobs()
  with Session() as db: tick(db)
 except Exception: traceback.print_exc()
 time.sleep(1)
