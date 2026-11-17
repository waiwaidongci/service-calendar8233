import os, sys, time

sys.path.insert(0, '/workspace/backend')
os.environ.setdefault('PROJECT_CONFIG', '/workspace/backend/project.json')
os.environ.setdefault('JWT_SECRET', 'test-secret')

import pytest  # noqa: E402
from sqlalchemy import create_engine  # noqa: E402
from sqlalchemy.orm import sessionmaker  # noqa: E402

from src.config.database import Base  # noqa: E402
from src.models import identity, source, run, clock  # noqa: E402
from src.models.clock import ClockEvent, ClockJob, ClockRelease, ClockRequest, ClockSegment  # noqa: E402
from src.models.source import Policy  # noqa: E402
from src.models.run import Run  # noqa: E402
from src.config.settings import CONFIG  # noqa: E402
from src.services.clock import receive_events  # noqa: E402


class User:
    email = 'tester@demo.local'
    role = 'admin'


@pytest.fixture()
def db(tmp_path, monkeypatch):
    import src.services.clock as clock_service
    import src.services.report as report_service

    engine = create_engine(f'sqlite:///{tmp_path}/clock.db', connect_args={'check_same_thread': False})
    Base.metadata.create_all(engine)
    session_factory = sessionmaker(bind=engine, expire_on_commit=False)

    class Ctx:
        def __enter__(self):
            self.session = session_factory()
            return self.session

        def __exit__(self, *exc):
            self.session.close()

    monkeypatch.setattr(clock_service, 'Session', Ctx)
    monkeypatch.setattr(report_service, 'Session', Ctx)

    class FakeRedis:
        def rpush(self, *args, **kwargs):
            return 1

        @classmethod
        def from_url(cls, *args, **kwargs):
            return cls()

    monkeypatch.setattr(report_service.redis, 'Redis', FakeRedis)
    session = session_factory()
    session.add(Policy(version=CONFIG['policy']['version'], data=CONFIG['policy']))
    session.commit()
    yield session
    session.close()


def detail_of(db, request_no='R1'):
    db.expire_all()
    from src.services.clock import request_detail
    return request_detail(db, request_no)


def ev(no, revision, kind, iso, payload=None, request='R1'):
    row = {'request_no': request, 'event_no': no, 'revision': revision, 'kind': kind, 'occurred_at': iso}
    if payload is not None:
        row['payload'] = payload
    return row


OPEN = {'period': '2026-11', 'department': '数据建模组', 'priority': '普通'}


def full_stream(request='R1'):
    return [
        ev('E1', 1, 'open', '2026-11-17T09:00:00+08:00', OPEN, request),
        ev('E2', 1, 'pause', '2026-11-17T10:00:00+08:00', None, request),
        ev('E3', 1, 'resume', '2026-11-17T11:00:00+08:00', None, request),
        ev('E4', 1, 'close', '2026-11-17T14:00:00+08:00', None, request),
    ]


def test_receive_publishes_complete_version(db):
    from src.services.clock import receive_events, request_detail

    results = receive_events(db, User(), full_stream())
    assert [r['status'] for r in results] == ['accepted'] * 4
    detail = detail_of(db, 'R1')
    assert detail['request']['version'] == 1
    assert detail['latest']['state'] == 'closed'
    assert detail['latest']['work_minutes'] == 180.0   # 09–10 + 11–12 + 13–14，午休 12–13 本就不计
    assert len(detail['latest']['segments']) == 3
    assert detail['latest']['event_digest']
    rows = db.query(ClockRelease).filter_by(request_no='R1').all()
    assert len(rows) == 1
    assert db.query(ClockSegment).filter_by(release_id=rows[0].id).count() == 3
    job = db.query(ClockJob).filter_by(request_no='R1').one()
    assert job.status == 'done'


def test_duplicate_retry_not_counted_again(db):
    from src.services.clock import receive_events, request_detail

    receive_events(db, User(), full_stream())
    version = request_detail(db, 'R1')['request']['version']
    again = receive_events(db, User(), full_stream())
    assert [r['status'] for r in again] == ['duplicate'] * 4
    detail = detail_of(db, 'R1')
    assert detail['request']['version'] == version  # 未触发新版本
    assert db.query(ClockEvent).filter_by(request_no='R1', status='accepted').count() == 4


def test_same_version_conflict_isolates_and_late_event_fills(db):
    from src.services.clock import receive_events, request_detail

    receive_events(db, User(), [ev('E1', 1, 'open', '2026-11-17T09:00:00+08:00', OPEN)])
    receive_events(db, User(), [ev('E2', 1, 'close', '2026-11-17T10:00:00+08:00')])
    assert detail_of(db, 'R1')['latest']['work_minutes'] == 60.0

    conflict = receive_events(db, User(), [ev('E2', 1, 'close', '2026-11-17T10:30:00+08:00')])
    assert conflict[0]['status'] == 'conflict'
    detail = detail_of(db, 'R1')
    assert detail['latest']['state'] == 'quarantined'
    assert '同版异值' in detail['latest']['reason']

    # 晚到的暂停事件补齐，并提升修订号更正冲突
    receive_events(db, User(), [
        ev('E2', 2, 'close', '2026-11-17T10:30:00+08:00'),
        ev('E3', 1, 'pause', '2026-11-17T09:30:00+08:00'),
    ])
    receive_events(db, User(), [ev('E4', 1, 'resume', '2026-11-17T09:45:00+08:00')])
    detail = detail_of(db, 'R1')
    assert detail['latest']['state'] == 'closed'
    assert detail['latest']['work_minutes'] == 75.0  # 09–09:30 + 09:45–10:30
    assert detail['request']['conflict_count'] == 1
    assert any(e['status'] == 'conflict' for e in detail['events'])


def test_illegal_chain_is_quarantined_with_reason(db):
    from src.services.clock import receive_events, request_detail

    receive_events(db, User(), [
        ev('E1', 1, 'open', '2026-11-17T09:00:00+08:00', OPEN),
        ev('E2', 1, 'resume', '2026-11-17T09:30:00+08:00'),
        ev('E3', 1, 'close', '2026-11-17T10:00:00+08:00'),
    ])
    detail = detail_of(db, 'R1')
    assert detail['request']['state'] == 'quarantined'
    assert '恢复前未处于暂停' in detail['request']['reason']
    assert detail['latest']['counted'] is False


def test_cutoff_freezes_report_and_late_events_do_not_change_it(db):
    from src.services.clock import receive_events, report_snapshot
    from src.config.database import Session

    receive_events(db, User(), [
        ev('E1', 1, 'open', '2026-11-17T09:00:00+08:00', OPEN, request='R1'),
        ev('E2', 1, 'close', '2026-11-17T10:00:00+08:00', None, request='R1'),
        ev('E1', 1, 'open', '2026-11-17T09:00:00+08:00', OPEN, request='R2'),
        ev('E2', 1, 'close', '2026-11-17T09:30:00+08:00', None, request='R2'),
    ])
    cutoff = time.time()
    snapshot = report_snapshot(db, '2026-11', cutoff, CONFIG['policy'], CONFIG['policy']['version'])
    assert snapshot['summary']['计入请求数'] == 2
    assert snapshot['summary']['有效业务分钟合计'] == 90.0
    frozen = snapshot['rows'][0]['有效业务分钟']

    time.sleep(0.01)
    receive_events(db, User(), [ev('E3', 1, 'pause', '2026-11-17T09:15:00+08:00', None, request='R1'),
                                ev('E4', 1, 'resume', '2026-11-17T09:45:00+08:00', None, request='R1')])
    replay = report_snapshot(db, '2026-11', cutoff, CONFIG['policy'], CONFIG['policy']['version'])
    assert replay['rows'][0]['有效业务分钟'] == frozen
    assert replay['summary']['有效业务分钟合计'] == 90.0
    # 截止点之后的新报告可见更正，但旧报告结果对象不变
    later = report_snapshot(db, '2026-11', time.time(), CONFIG['policy'], CONFIG['policy']['version'])
    assert later['summary']['有效业务分钟合计'] == 60.0
    assert snapshot['summary']['有效业务分钟合计'] == 90.0


def test_rebuild_only_touches_related_request(db):
    from src.services.clock import receive_events, run_job

    receive_events(db, User(), full_stream('R1'))
    receive_events(db, User(), full_stream('R2'))
    before = {row.request_no: (row.version, row.published_at) for row in db.query(ClockRequest).all()}
    receive_events(db, User(), [ev('E5', 1, 'revoke', '2026-11-17T15:00:00+08:00', None, request='R2')])
    after = {row.request_no: (row.version, row.published_at) for row in db.query(ClockRequest).all()}
    assert after['R1'] == before['R1']
    assert after['R2'][0] == before['R2'][0] + 1
    assert db.query(ClockRelease).filter_by(request_no='R1').count() == 1


def test_stale_computation_cannot_overwrite_newer_release(db, monkeypatch):
    """模拟：旧计算读取后、发布前又到达新事件，旧结果必须丢弃并与新状态一起重建。"""
    import src.services.clock as clock_service

    receive_events(db, User(), full_stream('R1'))
    clock_service._enqueue(db, 'R1', time.time())
    db.commit()

    real_build = clock_service.build_release
    state = {'first': True}

    def slow_build(events, policy, policy_version):
        result = real_build(events, policy, policy_version)
        if state['first']:
            state['first'] = False
            # 计算已经开始后才收到撤销事件，change_seq 前进
            receive_events(
                db,
                User(),
                [ev('E5', 1, 'revoke', '2026-11-17T15:00:00+08:00', None, request='R1')],
                process_inline=False,
            )
        return result

    monkeypatch.setattr(clock_service, 'build_release', slow_build)
    first = clock_service.run_job('R1')
    assert first is False  # 旧结果被丢弃，未发布
    assert state['first'] is False
    db.expire_all()
    request = db.query(ClockRequest).filter_by(request_no='R1').one()
    assert request.state == 'closed'
    assert request.version == 1
    job = db.query(ClockJob).filter_by(request_no='R1').one()
    assert job.status == 'pending' and job.dirty is True

    # 下一次调度按最新 change_seq 重建，撤销生效并产生新版本
    job.available_at = 0
    db.commit()
    assert clock_service.run_job('R1') is True
    db.expire_all()
    detail = clock_service.request_detail(db, 'R1')
    versions = [v['version'] for v in detail['versions']]
    assert versions == [2, 1]
    assert detail['request']['state'] == 'revoked'
    assert detail['request']['version'] == 2
    assert detail['versions'][0]['stale'] is False
    assert detail['versions'][1]['stale'] is True
    assert detail['versions'][1]['state'] == 'closed'


def test_report_queue_snapshots_cutoff(db):
    from src.services.report import submit
    from src.schemas.requests import RunInput

    receive_events(db, User(), [
        ev('E1', 1, 'open', '2026-11-17T09:00:00+08:00', OPEN, request='R1'),
        ev('E2', 1, 'close', '2026-11-17T10:00:00+08:00', None, request='R1'),
    ])
    body = RunInput(name='十一月时钟核算', policy_id=1, period='2026-11', kind='clock')
    run_obj = submit(db, User(), body)
    assert run_obj.kind == 'clock'
    assert run_obj.cutoff is not None
    assert run_obj.snapshot['kind'] == 'clock'


def test_interrupted_job_recovers_after_lease(db):
    """进程在 processing 租约内崩溃：租约过期后由调度重新领取并发布。"""
    import src.services.clock as clock_service

    receive_events(db, User(), full_stream('R1'), process_inline=False)
    job = db.query(ClockJob).filter_by(request_no='R1').one()
    assert job.status == 'pending'
    job.status = 'processing'
    job.lease_until = time.time() - 1
    job.attempts = 1
    db.commit()
    assert clock_service.recover_jobs() == 1
    db.expire_all()
    job = db.query(ClockJob).filter_by(request_no='R1').one()
    assert job.status == 'pending' and job.dirty is True
    assert clock_service.process_due_jobs() == 1
    db.expire_all()
    assert clock_service.request_detail(db, 'R1')['request']['version'] == 1


def test_processing_lease_blocks_second_worker(db):
    import src.services.clock as clock_service

    receive_events(db, User(), full_stream('R1'), process_inline=False)
    job = db.query(ClockJob).filter_by(request_no='R1').one()
    job.status = 'processing'
    job.lease_until = time.time() + 30
    db.commit()
    assert clock_service.run_job('R1') is False
    db.expire_all()
    assert clock_service.request_detail(db, 'R1')['request']['version'] == 0
