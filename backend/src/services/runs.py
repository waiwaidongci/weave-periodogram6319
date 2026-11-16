import copy
from sqlalchemy import select,func,update
from sqlalchemy.exc import IntegrityError
from fastapi import HTTPException
from src.models import SpectrumRun,SpectrumComputation,FrequencyPlan
from src.utils.serialize import row
from .source import source_data
from .validation import validate_points,validate_plan
from .fingerprint import fingerprint,compute_key
from .audit import record

ACTIVE=('queued','running')
REUSABLE=('queued','running','completed')

def live_computation(db,key,lock=False):
 query=select(SpectrumComputation).where(SpectrumComputation.fingerprint==key,SpectrumComputation.status.in_(REUSABLE)).order_by(SpectrumComputation.id.desc())
 return db.scalar(query.with_for_update() if lock else query)

def deliver_payload(run,result):
 payload=copy.deepcopy(result);study=run.source_snapshot['study'];points=run.source_snapshot['points'];plan=run.plan_snapshot
 payload['source_study']=study;payload['plan']=plan;payload['fingerprint']=fingerprint(study,points,plan)
 payload['shared_computation']={'computation_id':run.computation_id,'compute_key':compute_key(points,plan)}
 return payload

def create_run(db,data,actor):
 study,points=source_data(db,data.study_id);plan=db.get(FrequencyPlan,data.plan_id)
 if not plan or plan.study_id!=study['id']:raise HTTPException(422,'频率计划不属于该测线')
 snapshot=row(plan);plan_id=plan.id
 try:validate_points(points);validate_plan(snapshot)
 except ValueError as error:raise HTTPException(422,str(error))
 if db.scalar(select(SpectrumRun).where(SpectrumRun.name==data.name)):raise HTTPException(409,'运行名已存在')
 key=compute_key(points,snapshot);comp=live_computation(db,key,lock=True);fresh=comp is None
 if fresh:
  comp=SpectrumComputation(fingerprint=key);db.add(comp)
  try:db.flush()
  except IntegrityError:
   db.rollback();comp=live_computation(db,key,lock=True);fresh=False
   if comp is None:raise HTTPException(503,'共享计算登记冲突，请重试')
 run=SpectrumRun(study_id=study['id'],plan_id=plan_id,name=data.name,source_snapshot={'study':study,'points':points},plan_snapshot=snapshot,created_by=actor,computation_id=comp.id)
 reused=comp.status=='completed'
 if reused:
  run.status='completed';run.result=deliver_payload(run,comp.result);run.peak_frequency=comp.peak_frequency;run.max_power=comp.max_power;run.global_p=comp.global_p
 db.add(run);db.commit()
 if fresh:
  try:
   from src.worker import analyze_run
   analyze_run.delay(comp.id)
  except Exception:
   comp.status='failed';comp.error='队列不可用，请稍后重建';run.status='failed';run.error=comp.error;db.commit();raise HTTPException(503,comp.error)
 elif not reused:
  db.refresh(comp)
  if comp.status=='completed':db.execute(update(SpectrumRun).where(SpectrumRun.id==run.id,SpectrumRun.status.in_(ACTIVE)).values(status='completed',result=deliver_payload(run,comp.result),peak_frequency=comp.peak_frequency,max_power=comp.max_power,global_p=comp.global_p))
  elif comp.status!='queued' and comp.status!='running':db.execute(update(SpectrumRun).where(SpectrumRun.id==run.id,SpectrumRun.status.in_(ACTIVE)).values(status='failed',error=comp.error or '共享计算已终止'))
  db.commit();db.refresh(run)
 record(db,actor,'run.queued',{'run_id':run.id,'computation_id':comp.id,'shared_compute':not fresh,'reused_result':run.status=='completed'});return row(run)

def cancel_run(db,id,actor):
 run=db.get(SpectrumRun,id)
 if not run:raise HTTPException(404,'运行不存在')
 comp=db.scalar(select(SpectrumComputation).where(SpectrumComputation.id==run.computation_id).with_for_update()) if run.computation_id else None
 run=db.scalar(select(SpectrumRun).where(SpectrumRun.id==id).with_for_update())
 if run.status=='cancelled':raise HTTPException(409,'运行已取消')
 if run.status not in ACTIVE:raise HTTPException(409,'已结束的运行不能取消')
 run.status='cancelled';run.error=None;db.flush();stopped=False
 if comp and comp.status in ACTIVE:
  remaining=db.scalar(select(func.count()).select_from(SpectrumRun).where(SpectrumRun.computation_id==comp.id,SpectrumRun.status.in_(ACTIVE)))
  if not remaining:comp.status='cancelled';stopped=True
 db.commit();record(db,actor,'run.cancelled',{'run_id':id,'computation_id':run.computation_id,'computation_stopped':stopped});return row(run)
