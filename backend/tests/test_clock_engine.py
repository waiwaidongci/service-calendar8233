import os, sys, types, tempfile, importlib

# 在导入后端模块前注入最小的 src 包骨架，避免把 /workspace 当 PYTHONPATH 时的路径污染。
sys.path.insert(0, '/workspace/backend')
os.environ.setdefault('PROJECT_CONFIG', '/workspace/backend/project.json')
os.environ.setdefault('JWT_SECRET', 'test-secret')

from src.services.clock_engine import build_release, parse_when, fingerprint as fp  # noqa: E402
from src.config.settings import CONFIG  # noqa: E402

POLICY = CONFIG['policy']
OPEN = {'period': '2026-11', 'department': '数据建模组', 'priority': '普通'}


def event(no, revision, kind, iso, payload=None, status='accepted', received=None, request='R1'):
    ts = parse_when(iso)
    pl = payload or {}
    if isinstance(pl, dict) and payload is None:
        pl = {}
    return {
        'request_no': request,
        'event_no': no,
        'revision': revision,
        'kind': kind,
        'occurred_at': ts,
        'occurred_at_iso': iso,
        'received_at': received if received is not None else ts,
        'payload': pl,
        'status': status,
        'fingerprint': fp(kind, ts, pl),
        'reason': '',
    }


def basic_stream():
    return [
        event('E1', 1, 'open', '2026-11-13T16:30:00+08:00', OPEN),
        event('E2', 1, 'pause', '2026-11-13T17:00:00+08:00'),
        event('E3', 1, 'resume', '2026-11-16T09:00:00+08:00'),
        event('E4', 1, 'close', '2026-11-16T10:00:00+08:00'),
    ]


def test_weekend_holiday_pause_intersection():
    result = build_release(basic_stream(), POLICY, '工作时钟-v1')
    assert result['state'] == 'closed'
    assert result['gross_minutes'] == 90.0        # 周五 30 + 周一 60
    assert result['pause_minutes'] == 60.0        # 暂停跨越周末与假日 11-16，仅周一上午计 60
    assert result['work_minutes'] == 30.0
    assert [seg['minutes'] for seg in result['segments']] == [30.0, 60.0, 0.0]


def test_lunch_window_deducted():
    stream = [
        event('E1', 1, 'open', '2026-11-17T11:30:00+08:00', OPEN),
        event('E2', 1, 'pause', '2026-11-17T11:45:00+08:00'),
        event('E3', 1, 'resume', '2026-11-17T13:00:00+08:00'),
        event('E4', 1, 'close', '2026-11-17T13:30:00+08:00'),
    ]
    result = build_release(stream, POLICY, 'v1')
    assert result['gross_minutes'] == 60.0
    assert result['pause_minutes'] == 15.0        # 11:45–12:00 落在上午时窗
    assert result['work_minutes'] == 45.0


def test_late_event_changes_but_same_version_isolation():
    stream = basic_stream()
    result = build_release(stream, POLICY, 'v1')
    assert result['work_minutes'] == 30.0
    conflict = event('E2', 1, 'pause', '2026-11-13T18:00:00+08:00', status='conflict')
    quarantined = build_release(stream + [conflict], POLICY, 'v1')
    assert quarantined['state'] == 'quarantined'
    assert '同版异值' in quarantined['reason']
    # 提升修订号更正后恢复
    corrected = event('E2', 2, 'pause', '2026-11-13T18:00:00+08:00')
    fixed = build_release(stream + [conflict, corrected], POLICY, 'v1')
    assert fixed['state'] == 'closed'
    assert fixed['pause_minutes'] == 0.0


def test_duplicate_is_not_double_counted():
    stream = basic_stream() + basic_stream()
    for row in stream[4:]:
        row['status'] = 'duplicate'
    result = build_release(stream, POLICY, 'v1')
    assert result['work_minutes'] == 30.0


def test_illegal_chain_quarantined_with_reason():
    stream = [
        event('E1', 1, 'pause', '2026-11-17T09:00:00+08:00'),
        event('E2', 1, 'open', '2026-11-17T10:00:00+08:00', OPEN),
        event('E3', 1, 'open', '2026-11-17T10:30:00+08:00', OPEN),
        event('E4', 1, 'close', '2026-11-17T11:00:00+08:00'),
    ]
    result = build_release(stream, POLICY, 'v1')
    assert result['state'] == 'quarantined'
    assert '暂停前无进行中的请求' in result['reason']
    assert '重复开单' in result['reason']


def test_open_request_not_counted_and_revoked_not_counted():
    open_only = [event('E1', 1, 'open', '2026-11-17T09:00:00+08:00', OPEN)]
    result = build_release(open_only, POLICY, 'v1')
    assert result['state'] == 'open'
    assert result['counted'] is False
    revoked = open_only + [event('E2', 1, 'revoke', '2026-11-17T09:30:00+08:00')]
    result = build_release(revoked, POLICY, 'v1')
    assert result['state'] == 'revoked'
    assert result['counted'] is False


def test_overdue_threshold_is_strict():
    stream = [
        event('E1', 1, 'open', '2026-11-17T09:00:00+08:00', {'period': '2026-11', 'department': 'D', 'priority': '紧急'}),
        event('E2', 1, 'close', '2026-11-17T11:00:00+08:00'),  # 120 分钟，恰等于阈值
    ]
    result = build_release(stream, POLICY, 'v1')
    assert result['work_minutes'] == 120.0
    assert result['overdue'] is False


def test_boundary_minutes():
    stream = [
        event('E1', 1, 'open', '2026-11-17T09:00:00+08:00', OPEN),
        event('E2', 1, 'close', '2026-11-17T09:00:00+08:00'),
    ]
    result = build_release(stream, POLICY, 'v1')
    assert result['work_minutes'] == 0.0
