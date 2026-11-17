from fastapi import APIRouter,Depends,HTTPException
from fastapi.responses import Response
from src.config.database import database
from src.controllers.auth import current,editor,admin
from src.schemas.requests import ClockEventsInput
from src.services.clock import (
 list_jobs,
 list_requests,
 receive_events,
 request_detail,
 process_due_jobs,
 clock_csv,
)
from src.services.audit import log
router=APIRouter()


def _serialize(item,anchor=None):
 return {
  'request_no':item.request_no,'event_no':item.event_no,'revision':item.revision,
  'kind':item.kind,'occurred_at':item.occurred_at,'payload':item.payload,
  'received_at':item.received_at,'status':item.status,'reason':item.reason,
 }


@router.get('/clock/requests')
def requests(db=Depends(database),u=Depends(current)):
 return list_requests(db)


@router.get('/clock/requests/{request_no}')
def request_one(request_no:str,db=Depends(database),u=Depends(current)):
 return request_detail(db,request_no)


@router.post('/clock/events')
def ingest(b:ClockEventsInput,db=Depends(database),u=Depends(editor)):
 items=[x.model_dump() for x in b.events]
 try:
  return {'received':receive_events(db,u,items)}
 except ValueError as exc:
  raise HTTPException(422,str(exc))


@router.post('/clock/requests/{request_no}/rebuild')
def rebuild(request_no:str,db=Depends(database),u=Depends(editor)):
 from src.services.clock import run_job,_enqueue
 import time
 request_detail(db,request_no)   # 不存在则404
 now=time.time()
 _enqueue(db,request_no,now);log(db,u.email,'clock.rebuild',request_no);db.commit()
 run_job(request_no,now)
 db.expire_all()
 return request_detail(db,request_no)


@router.post('/clock/run-jobs')
def run_jobs(u=Depends(admin)):
 return {'processed':process_due_jobs()}


@router.get('/clock/jobs')
def jobs(db=Depends(database),u=Depends(current)):
 return list_jobs(db)


@router.get('/clock/requests/{request_no}/csv')
def request_csv(request_no:str,db=Depends(database),u=Depends(current)):
 detail=request_detail(db,request_no)
 latest=detail.get('latest') or {}
 rows=[]
 for seg in latest.get('segments') or []:
  rows.append({
   '请求号':request_no,'版本':latest.get('version'),'段号':seg.get('seq'),'段类型':seg.get('kind_label'),
   '开始':seg.get('start_iso'),'结束':seg.get('end_iso'),'业务分钟':seg.get('minutes'),
   '起点事件':f"{seg.get('start_event')}@{seg.get('start_revision')}",
   '终点事件':f"{seg.get('end_event')}@{seg.get('end_revision')}",
  })
 return Response(clock_csv({'trace':rows}),media_type='text/csv',headers={'Content-Disposition':f'attachment; filename="clock-{request_no}.csv"'})
