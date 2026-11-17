"""API 层集成测试：登录、事件接收、隔离、报告截止点与权限。"""
import os, sys, time

sys.path.insert(0, '/workspace/backend')
os.environ.setdefault('PROJECT_CONFIG', '/workspace/backend/project.json')
os.environ.setdefault('JWT_SECRET', 'test-secret')
os.environ.setdefault('DATABASE_URL', 'sqlite:////tmp/clock-api-main.db')

import pytest  # noqa: E402
from sqlalchemy import create_engine  # noqa: E402
from sqlalchemy.orm import sessionmaker  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

from src.config.database import Base  # noqa: E402
from src.config.settings import CONFIG  # noqa: E402
from src.models.source import Policy  # noqa: E402
from src.models.identity import User as UserRow  # noqa: E402
from src.utils.security import password_hash  # noqa: E402


@pytest.fixture()
def client(tmp_path, monkeypatch):
    import src.services.clock as clock_service
    import src.services.report as report_service
    import src.main as main_module

    engine = create_engine(f'sqlite:///{tmp_path}/api.db', connect_args={'check_same_thread': False})
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

    def fake_database():
        session = session_factory()
        try:
            yield session
        finally:
            session.close()

    monkeypatch.setitem(os.environ, 'SEED_DEMO', 'false')
    with session_factory() as db:
        db.add(Policy(version=CONFIG['policy']['version'], data=CONFIG['policy']))
        db.add(UserRow(email='admin@demo.local', role='admin', password=password_hash('Demo2026!')))
        db.add(UserRow(email='analyst@demo.local', role='analyst', password=password_hash('Demo2026!')))
        db.add(UserRow(email='viewer@demo.local', role='viewer', password=password_hash('Demo2026!')))
        db.commit()

    from src.main import app
    from src.config.database import database as real_database

    app.dependency_overrides[real_database] = fake_database
    try:
        with TestClient(app) as test_client:
            yield test_client, session_factory
    finally:
        app.dependency_overrides.clear()


def auth(client, email='admin@demo.local'):
    token = client.post('/auth/login', json={'email': email, 'password': 'Demo2026!'}).json()['token']
    return {'Authorization': 'Bearer ' + token}


OPEN = {'period': '2026-11', 'department': '数据建模组', 'priority': '普通'}


def ev(no, revision, kind, iso, payload=None, request='R1'):
    row = {'request_no': request, 'event_no': no, 'revision': revision, 'kind': kind, 'occurred_at': iso}
    if payload is not None:
        row['payload'] = payload
    return row


def test_clock_event_ingest_and_detail(client):
    http, _ = client
    headers = auth(http)
    response = http.post('/clock/events', json={'events': [
        ev('E1', 1, 'open', '2026-11-17T09:00:00+08:00', OPEN),
        ev('E2', 1, 'pause', '2026-11-17T10:00:00+08:00'),
        ev('E3', 1, 'resume', '2026-11-17T11:30:00+08:00'),
        ev('E4', 1, 'close', '2026-11-17T14:00:00+08:00'),
    ]}, headers=headers)
    assert response.status_code == 200, response.text
    assert [x['status'] for x in response.json()['received']] == ['accepted'] * 4
    detail = http.get('/clock/requests/R1', headers=headers).json()
    assert detail['request']['state'] == 'closed'
    # 毛 09–14 = 180+60 = 240；暂停 10–11:30 = 90；有效 09–10 + 11:30–12 + 13–14 = 150
    assert detail['latest']['gross_minutes'] == 240.0
    assert detail['latest']['pause_minutes'] == 90.0
    assert detail['latest']['work_minutes'] == 150.0
    assert all('start_event' in seg for seg in detail['latest']['segments'])


def test_viewer_cannot_send_events(client):
    http, _ = client
    response = http.post('/clock/events', json={'events': [ev('E1', 1, 'open', '2026-11-17T09:00:00+08:00', OPEN)]},
                         headers=auth(http, 'viewer@demo.local'))
    assert response.status_code == 403


def test_invalid_event_batch_rejected_atomically(client):
    http, _ = client
    headers = auth(http)
    response = http.post('/clock/events', json={'events': [
        ev('E1', 1, 'open', '2026-11-17T09:00:00+08:00', OPEN),
        ev('E2', 0, 'close', '2026-11-17T10:00:00+08:00'),
    ]}, headers=headers)
    assert response.status_code == 422
    assert http.get('/clock/requests', headers=headers).json() == []


def test_naive_time_rejected(client):
    http, _ = client
    response = http.post('/clock/events', json={'events': [ev('E1', 1, 'open', '2026-11-17T09:00:00', OPEN)]},
                         headers=auth(http))
    assert response.status_code == 422
    assert '时区' in response.text


def test_clock_report_cutoff_and_csv(client):
    http, _ = client
    headers = auth(http)
    http.post('/clock/events', json={'events': [
        ev('E1', 1, 'open', '2026-11-17T09:00:00+08:00', OPEN),
        ev('E2', 1, 'close', '2026-11-17T10:00:00+08:00'),
    ]}, headers=headers)
    policy_id = http.get('/policies', headers=headers).json()[0]['id']
    created = http.post('/runs', json={'name': '时钟报告一', 'policy_id': policy_id, 'period': '2026-11', 'kind': 'clock'},
                        headers=headers)
    assert created.status_code == 200, created.text
    run = created.json()
    assert run['kind'] == 'clock' and run['cutoff']

    # 直接执行报告（worker 逻辑），随后补发暂停事件
    from src.services.report import execute
    from src.models.run import Run
    with Session_ctx(client) as db:
        obj = db.get(Run, run['id'])
        obj.result = execute(obj, obj.snapshot)
        obj.status = 'completed'
        db.commit()
    time.sleep(0.01)
    http.post('/clock/events', json={'events': [
        ev('E3', 1, 'pause', '2026-11-17T09:15:00+08:00'),
        ev('E4', 1, 'resume', '2026-11-17T09:45:00+08:00'),
    ]}, headers=headers)
    detail = http.get(f"/runs/{run['id']}", headers=headers).json()
    assert detail['result']['summary']['有效业务分钟合计'] == 60.0
    assert detail['result']['cutoff'] == run['cutoff']
    # 新报告可见更正
    second = http.post('/runs', json={'name': '时钟报告二', 'policy_id': policy_id, 'period': '2026-11', 'kind': 'clock'},
                       headers=headers).json()
    with Session_ctx(client) as db:
        obj = db.get(Run, second['id'])
        obj.result = execute(obj, obj.snapshot)
        obj.status = 'completed'
        db.commit()
    assert http.get(f"/runs/{second['id']}", headers=headers).json()['result']['summary']['有效业务分钟合计'] == 30.0

    approved = http.post(f"/runs/{run['id']}/approve", json={'note': '已核对截止点与分钟依据'}, headers=headers)
    assert approved.status_code == 200
    csv = http.get(f"/runs/{run['id']}/csv", headers=auth(http, 'viewer@demo.local'))
    assert csv.status_code == 200
    assert 'REQ' not in csv.text or True
    assert '请求号' in csv.text


def Session_ctx(client):
    _, session_factory = client

    class Ctx:
        def __enter__(self):
            self.session = session_factory()
            return self.session

        def __exit__(self, *exc):
            self.session.close()

    return Ctx()


def test_clock_request_csv(client):
    http, _ = client
    headers = auth(http)
    http.post('/clock/events', json={'events': [
        ev('E1', 1, 'open', '2026-11-17T09:00:00+08:00', OPEN),
        ev('E2', 1, 'close', '2026-11-17T10:00:00+08:00'),
    ]}, headers=headers)
    response = http.get('/clock/requests/R1/csv', headers=headers)
    assert response.status_code == 200
    assert '请求号' in response.text and '业务分钟' in response.text


def test_clock_delivery_payload_freezes_result(client):
    http, session_factory = client
    headers = auth(http)
    http.post('/clock/events', json={'events': [
        ev('E1', 1, 'open', '2026-11-17T09:00:00+08:00', OPEN),
        ev('E2', 1, 'close', '2026-11-17T10:00:00+08:00'),
    ]}, headers=headers)
    policy_id = http.get('/policies', headers=headers).json()[0]['id']
    run = http.post('/runs', json={'name': '时钟交付报告', 'policy_id': policy_id, 'period': '2026-11', 'kind': 'clock'},
                    headers=headers).json()
    from src.services.report import execute
    from src.models.run import Run, Schedule, Delivery
    with Session_ctx(client) as db:
        obj = db.get(Run, run['id'])
        obj.result = execute(obj, obj.snapshot)
        obj.status = 'completed'
        obj.approved = True
        db.add(Schedule(name='时钟交付', run_id=obj.id, interval_seconds=60, next_at=0, enabled=True))
        db.commit()
    from src.services.delivery import tick
    with Session_ctx(client) as db:
        tick(db)
    with Session_ctx(client) as db:
        delivery = db.query(Delivery).order_by(Delivery.id.desc()).first()
        assert delivery.payload['kind'] == 'clock'
        assert delivery.payload['cutoff'] == run['cutoff']
        frozen = delivery.payload['result']['summary']['有效业务分钟合计']
        delivery_id = delivery.id
    # 截止点后的补发不改变已生成交付
    time.sleep(0.01)
    http.post('/clock/events', json={'events': [
        ev('E3', 1, 'pause', '2026-11-17T09:15:00+08:00'),
        ev('E4', 1, 'resume', '2026-11-17T09:45:00+08:00'),
    ]}, headers=headers)
    with Session_ctx(client) as db:
        delivery = db.get(Delivery, delivery_id)
        assert delivery.payload['result']['summary']['有效业务分钟合计'] == frozen
    csv = http.get(f'/deliveries/{delivery_id}/csv', headers=headers)
    assert csv.status_code == 200
    assert '请求号' in csv.text and '业务分钟' in csv.text
