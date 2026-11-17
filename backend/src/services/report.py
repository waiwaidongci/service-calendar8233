import hashlib,json,time
from fastapi import HTTPException
from src.config.database import Session
from src.models.source import Source,Policy
from src.models.run import Run
from src.services.audit import log
from src.services.clock import report_snapshot
from src.config.settings import REDIS_URL
import redis


def submit(db,user,body):
 policy=db.get(Policy,body.policy_id)
 if not policy: raise HTTPException(404,'口径版本不存在')
 kind=getattr(body,'kind','draft') or 'draft'
 if kind=='clock':
  cutoff=time.time()
  snapshot={'kind':'clock','policy':policy.data,'policy_version':policy.version,'period':body.period,'cutoff':cutoff}
  checksum=hashlib.sha256(json.dumps(snapshot,sort_keys=True,ensure_ascii=False).encode()).hexdigest()
  obj=Run(name=body.name,status='queued',snapshot=snapshot,checksum=checksum,created=time.time(),approved=False,kind='clock',cutoff=cutoff,period=body.period)
  db.add(obj);log(db,user.email,'clock.report.queue',body.name);db.commit()
  redis.Redis.from_url(REDIS_URL).rpush('report-jobs',obj.id)
  return obj
 sources=[x.data for x in db.query(Source).order_by(Source.id).all() if body.period=='全部' or x.data.get('period')==body.period]
 if not sources: raise HTTPException(422,'所选期间没有底稿')
 snapshot={'policy':policy.data,'sources':sources,'period':body.period}
 checksum=hashlib.sha256(json.dumps(snapshot,sort_keys=True,ensure_ascii=False).encode()).hexdigest()
 obj=Run(name=body.name,status='queued',snapshot=snapshot,checksum=checksum,created=time.time(),approved=False,kind='draft',period=body.period)
 db.add(obj);log(db,user.email,'run.queue',body.name);db.commit()
 redis.Redis.from_url(REDIS_URL).rpush('report-jobs',obj.id)
 return obj


def execute(run,snapshot):
 """worker 入口：底稿核算或时钟报告截止点重放。"""
 if (snapshot or {}).get('kind')=='clock':
  with Session() as db:
   return report_snapshot(db,snapshot['period'],snapshot['cutoff'],snapshot['policy'],snapshot.get('policy_version'))
 from src.services.calculation import calculate
 return calculate(snapshot['sources'],snapshot['policy'])


def approve(db,user,id,note):
 obj=db.get(Run,id)
 if not obj: raise HTTPException(404,'报告不存在')
 if obj.status!='completed': raise HTTPException(409,'仅可复核已完成报告')
 if obj.approved: raise HTTPException(409,'报告已经复核')
 obj.approved=True;obj.review=note;log(db,user.email,'run.approve',id);db.commit();return obj
