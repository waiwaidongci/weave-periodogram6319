import json
from pathlib import Path
from celery import Celery
from sqlalchemy import select,update
from src.config.database import SessionLocal
from src.config.settings import REDIS_URL,ARTIFACT_DIR
from src.models import SpectrumRun,SpectrumComputation
from src.services.analysis import analyze
from src.services.permutation import ComputationCancelled
from src.services.runs import deliver_payload,ACTIVE
from src.services.audit import record
app=Celery('weave_periodogram',broker=REDIS_URL,backend=REDIS_URL)
app.conf.update(task_track_started=True,worker_prefetch_multiplier=1,task_acks_late=True)
@app.task(name='weave.analyze',soft_time_limit=120,time_limit=150)
def analyze_run(id):
 with SessionLocal() as db:
  comp=db.get(SpectrumComputation,id)
  if not comp or comp.result is not None or comp.status not in ACTIVE:return
  run=db.scalar(select(SpectrumRun).where(SpectrumRun.computation_id==id,SpectrumRun.status.in_(ACTIVE)).order_by(SpectrumRun.id))
  if not run:
   db.execute(update(SpectrumComputation).where(SpectrumComputation.id==id,SpectrumComputation.status.in_(ACTIVE)).values(status='cancelled'));db.commit();return
  transitioned=db.execute(update(SpectrumComputation).where(SpectrumComputation.id==id,SpectrumComputation.status=='queued').values(status='running'));db.commit()
  if not transitioned.rowcount and db.scalar(select(SpectrumComputation.status).where(SpectrumComputation.id==id))!='running':return
  def stopped():return db.scalar(select(SpectrumComputation.status).where(SpectrumComputation.id==id))!='running'
  try:
   snapshot=run.source_snapshot;result=analyze(snapshot['study'],snapshot['points'],run.plan_snapshot,should_stop=stopped)
   claimed=db.execute(update(SpectrumComputation).where(SpectrumComputation.id==id,SpectrumComputation.status.in_(ACTIVE)).values(status='completed',result=result,error=None,peak_frequency=result['best']['frequency'],max_power=result['observed_max_power'],global_p=result['global_test']['global_p']))
   if not claimed.rowcount:db.rollback();return
   Path(ARTIFACT_DIR).mkdir(parents=True,exist_ok=True);Path(ARTIFACT_DIR,f'weave-computation-{id}.json').write_text(json.dumps(result,ensure_ascii=False,allow_nan=False))
   delivered=[]
   for target in db.scalars(select(SpectrumRun).where(SpectrumRun.computation_id==id,SpectrumRun.status.in_(ACTIVE))).all():
    updated=db.execute(update(SpectrumRun).where(SpectrumRun.id==target.id,SpectrumRun.status.in_(ACTIVE)).values(status='completed',result=deliver_payload(target,result),peak_frequency=result['best']['frequency'],max_power=result['observed_max_power'],global_p=result['global_test']['global_p'],error=None))
    if updated.rowcount:delivered.append(target.id)
   db.commit();record(db,'worker','computation.completed',{'computation_id':id,'run_ids':delivered,'global_p':result['global_test']['global_p']})
  except ComputationCancelled:
   db.rollback();record(db,'worker','computation.stopped',{'computation_id':id})
  except Exception as error:
   db.rollback();message=str(error)[:1000]
   db.execute(update(SpectrumComputation).where(SpectrumComputation.id==id,SpectrumComputation.status.in_(ACTIVE)).values(status='failed',error=message))
   db.execute(update(SpectrumRun).where(SpectrumRun.computation_id==id,SpectrumRun.status.in_(ACTIVE)).values(status='failed',error=message))
   db.commit();record(db,'worker','computation.failed',{'computation_id':id,'error':message})
