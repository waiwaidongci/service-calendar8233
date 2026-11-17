import json,random
from src.config.database import Base,engine,Session
from src.config.settings import CONFIG
from src.models.identity import User
from src.models.source import Source,Policy,Period
from src.models.clock import ClockRequest
from src.models import run,clock
from src.utils.security import password_hash
from src.services.validation import validate_source
from src.services.clock import receive_events

def demo_rows():
 rng=random.Random(42);rows=[]
 for i in range(240):
  day=1+i%20;rows.append({'period':'2026-11','department':['数据建模组','数据采样组','数据核验组'][i%3],'opened':f'2026-11-{day:02}T10:30:00+08:00','closed':f'2026-11-{day:02}T16:30:00+08:00','priority':['普通','紧急'][i%2]})
 return rows

def demo_events():
 """六条具名事件流：跨周末假日、午休暂停、晚到补齐、撤销。"""
 base={'period':'2026-11','department':'数据建模组'}
 def ev(request_no,event_no,revision,kind,occurred_at,payload=None):
  return {'request_no':request_no,'event_no':event_no,'revision':revision,'kind':kind,'occurred_at':occurred_at,'payload':payload or {}}
 return [
  ev('REQ-2026-1101','E1',1,'open','2026-11-13T16:30:00+08:00',{**base,'priority':'普通'}),
  ev('REQ-2026-1101','E2',1,'pause','2026-11-13T17:00:00+08:00'),
  ev('REQ-2026-1101','E3',1,'resume','2026-11-16T09:00:00+08:00'),
  ev('REQ-2026-1101','E4',1,'close','2026-11-16T10:00:00+08:00'),
  ev('REQ-2026-1102','E1',1,'open','2026-11-17T09:10:00+08:00',{**base,'department':'数据采样组','priority':'紧急'}),
  ev('REQ-2026-1102','E2',1,'pause','2026-11-17T11:30:00+08:00'),
  ev('REQ-2026-1102','E3',1,'resume','2026-11-17T13:00:00+08:00'),
  ev('REQ-2026-1102','E4',1,'close','2026-11-17T14:20:00+08:00'),
  ev('REQ-2026-1103','E1',1,'open','2026-11-18T09:00:00+08:00',{**base,'department':'数据核验组','priority':'普通'}),
  ev('REQ-2026-1103','E2',1,'close','2026-11-18T11:00:00+08:00'),
  ev('REQ-2026-1104','E1',1,'open','2026-11-19T09:30:00+08:00',{**base,'priority':'紧急'}),
  ev('REQ-2026-1104','E2',1,'close','2026-11-19T10:00:00+08:00'),
  ev('REQ-2026-1105','E1',1,'open','2026-11-20T10:00:00+08:00',{**base,'department':'数据采样组','priority':'普通'}),
  ev('REQ-2026-1105','E2',1,'revoke','2026-11-20T10:30:00+08:00'),
 ]

def seed():
 Base.metadata.create_all(engine)
 with Session() as db:
  for role in ('admin','analyst','viewer'):
   if not db.query(User).filter_by(email=role+'@demo.local').first():db.add(User(email=role+'@demo.local',role=role,password=password_hash('Demo2026!')))
  rows=demo_rows()
  for name in sorted(set(r['period'] for r in rows)):
   if not db.query(Period).filter_by(name=name).first():db.add(Period(name=name,description='合成演示业务期间，seed=42'))
  for i,data in enumerate(rows):
   validate_source(data);key='demo-'+str(i+1)
   if not db.query(Source).filter_by(natural_key=key).first():db.add(Source(natural_key=key,data=data))
  if not db.query(Policy).filter_by(version=CONFIG['policy']['version']).first():db.add(Policy(version=CONFIG['policy']['version'],data=CONFIG['policy']))
  db.commit()
  if not db.query(ClockRequest).filter_by(request_no='REQ-2026-1101').first():
   events=[
    {**x,'payload':{**x['payload'],'policy_version':CONFIG['policy']['version']}} if x['kind']=='open' else x
    for x in demo_events()
   ]
   receive_events(db,type('SeedUser',(),{'email':'seed@demo.local'})(),events)
  db.commit()
  print(json.dumps({'users':db.query(User).count(),'sources':db.query(Source).count(),'policies':db.query(Policy).count(),'periods':db.query(Period).count(),'clock_requests':db.query(ClockRequest).count()},ensure_ascii=False))
if __name__=='__main__':seed()
