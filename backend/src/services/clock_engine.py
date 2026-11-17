"""业务时钟重放引擎。

纯函数：给定某请求的全部已接收事件与冻结口径，按发生时间重放状态机，
产出有效计时段、事件修订版次、业务分钟与隔离原因。不访问数据库，
便于并发发布与报告截止点重放共用同一套口径。
"""
from __future__ import annotations

import hashlib
import json
from datetime import datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo

from src.services.validation import validate_policy

KINDS = ('open', 'pause', 'resume', 'close', 'revoke')
KIND_LABELS = {
    'open': '开单',
    'pause': '暂停',
    'resume': '恢复',
    'close': '关闭',
    'revoke': '撤销',
}
STATE_LABELS = {
    'empty': '无事件',
    'open': '进行中',
    'paused': '暂停中',
    'closed': '已关闭',
    'revoked': '已撤销',
    'quarantined': '已隔离',
}
UTC = timezone.utc


def parse_when(value):
    """ISO 时间必须带时区；返回 UTC 秒。"""
    if not isinstance(value, str):
        raise ValueError('发生时间须为ISO格式')
    text = value.strip()
    if text.endswith('Z') or text.endswith('z'):
        text = text[:-1] + '+00:00'
    try:
        parsed = datetime.fromisoformat(text)
    except Exception as exc:
        raise ValueError('发生时间须为ISO格式') from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError('发生时间必须带时区')
    return parsed.timestamp()


def utc_iso(ts):
    return datetime.fromtimestamp(ts, tz=UTC).isoformat()


def local_iso(ts, zone_name):
    if ts is None:
        return None
    try:
        zone = ZoneInfo(zone_name)
    except Exception:
        zone = UTC
    return datetime.fromtimestamp(ts, tz=UTC).astimezone(zone).isoformat()


def normalize_payload(payload):
    """只保留参与计时与分组的业务字段，用于同版异值判定。"""
    if not isinstance(payload, dict):
        return {}
    out = {}
    for key in ('period', 'department', 'priority', 'policy_version'):
        value = payload.get(key)
        if value is None:
            continue
        if isinstance(value, str):
            value = value.strip()
            if not value:
                continue
        out[key] = value
    return out


def fingerprint(kind, occurred_at, payload):
    body = {
        'kind': kind,
        'occurred_at': utc_iso(occurred_at),
        'payload': normalize_payload(payload),
    }
    return hashlib.sha256(json.dumps(body, sort_keys=True, ensure_ascii=False).encode()).hexdigest()


def _work_seconds(start_ts, end_ts, policy):
    if start_ts is None or end_ts is None or end_ts <= start_ts:
        return 0.0
    zone = ZoneInfo(policy['timezone'])
    start = datetime.fromtimestamp(start_ts, tz=UTC).astimezone(zone)
    end = datetime.fromtimestamp(end_ts, tz=UTC).astimezone(zone)
    weekdays = set(policy['weekdays'])
    holidays = set(policy.get('holidays') or [])
    windows = policy['windows']
    total = 0.0
    day = start.date()
    while day <= end.date():
        if day.weekday() in weekdays and day.isoformat() not in holidays:
            midnight = datetime.combine(day, time(0), zone)
            for begin, finish in windows:
                lo = max(start, midnight + timedelta(minutes=begin))
                hi = min(end, midnight + timedelta(minutes=finish))
                if hi > lo:
                    total += (hi - lo).total_seconds()
        day += timedelta(days=1)
    return total


def _minutes(seconds):
    return round(seconds / 60.0, 6)


def latest_versions(events):
    """按事件号取最高修订；返回 (有效事件, 同版异值冲突列表, 重复行数)。

    events 为字典或带同名属性的对象；只应传入 accepted/conflict/duplicate。
    更高修订到达后，低修订的同版异值视为已被更正，不再隔离整个请求。
    """
    by_event = {}
    conflicts_by_event = {}
    duplicates = 0
    for item in events:
        row = item if isinstance(item, dict) else item.__dict__
        status = row.get('status', 'accepted')
        event_no = row['event_no']
        revision = row['revision']
        if status == 'duplicate':
            duplicates += 1
            continue
        if status == 'conflict':
            conflicts_by_event.setdefault(event_no, []).append(row)
            continue
        if status != 'accepted':
            continue
        current = by_event.get(event_no)
        if current is None or revision > current['revision']:
            by_event[event_no] = row
        elif revision == current['revision']:
            # 数据库唯一键理论上阻止；并发写入时在重放阶段兜底隔离。
            if row.get('fingerprint') != current.get('fingerprint'):
                conflicts_by_event.setdefault(event_no, []).append(row)
    conflicts = []
    for event_no, rows in conflicts_by_event.items():
        accepted = by_event.get(event_no)
        accepted_revision = accepted['revision'] if accepted else 0
        for row in rows:
            # 同版异值未被更高修订覆盖时隔离整条请求；更高修订到达即视为已更正。
            if row['revision'] >= accepted_revision and row not in conflicts:
                conflicts.append(row)
    return list(by_event.values()), conflicts, duplicates


def _sort_key(row):
    return (
        row['occurred_at'],
        row.get('received_at') or 0,
        row['event_no'],
        row['revision'],
    )


def _segment(kind, start, end, start_event, end_event, zone):
    return {
        'kind': kind,
        'start_at': start,
        'end_at': end,
        'start_iso': local_iso(start, zone),
        'end_iso': local_iso(end, zone),
        'start_event': start_event.get('event_no') if start_event else None,
        'start_revision': start_event.get('revision') if start_event else None,
        'end_event': end_event.get('event_no') if end_event else None,
        'end_revision': end_event.get('revision') if end_event else None,
    }


def build_release(events, policy, policy_version=None):
    """重放单个请求。

    events: 原始行（含 conflict/duplicate），仅 accepted 参与计算；
    policy: 冻结口径字典；policy_version: 口径名，用于展示与留痕。
    返回可直接写入发布版的普通字典。
    """
    try:
        validate_policy(policy)
    except ValueError as exc:
        return _quarantined(f'口径不合法：{exc}', policy_version)

    ordered_rows = []
    for item in events:
        row = item if isinstance(item, dict) else {
            column.name: getattr(item, column.name) for column in item.__table__.columns
        }
        ordered_rows.append(row)

    effective, conflicts, duplicates = latest_versions(ordered_rows)
    conflict_reasons = []
    for row in conflicts:
        conflict_reasons.append(
            f"同版异值：事件 {row['event_no']} 修订 {row['revision']} 存在不同取值"
        )
    # 去重键保证冲突行唯一；同一 (event_no,revision) 的多个 accepted 也视为冲突。
    if conflict_reasons:
        return _quarantined(
            '；'.join(dict.fromkeys(conflict_reasons)),
            policy_version,
            _revision_map(effective),
        )

    state = 'empty'
    errors = []
    open_at = close_at = revoked_at = None
    open_event = None
    active_start = None
    active_event = None
    pause_start = None
    pause_event = None
    raw_segments = []
    zone = policy['timezone']

    def fail(message):
        if len(errors) < 8:
            errors.append(message)

    for row in sorted(effective, key=_sort_key):
        kind = row['kind']
        at = row['occurred_at']
        label = KIND_LABELS.get(kind, kind)
        if state == 'revoked':
            fail(f"撤销后仍收到{label}事件：{row['event_no']}")
            continue
        if kind not in KINDS:
            fail(f"未知事件类型：{kind}")
            continue
        if kind == 'open':
            if state != 'empty':
                fail(f"重复开单：{row['event_no']}")
                continue
            state = 'open'
            open_at = at
            active_start = at
            active_event = row
            open_event = row
            continue
        if kind == 'pause':
            if state != 'open':
                fail(f"暂停前无进行中的请求：{row['event_no']}")
                continue
            raw_segments.append(_segment('active', active_start, at, active_event, row, zone))
            state = 'paused'
            pause_start = at
            pause_event = row
            continue
        if kind == 'resume':
            if state != 'paused':
                fail(f"恢复前未处于暂停：{row['event_no']}")
                continue
            raw_segments.append(_segment('pause', pause_start, at, pause_event, row, zone))
            state = 'open'
            active_start = at
            active_event = row
            continue
        if kind == 'close':
            if state == 'closed':
                fail(f"重复关闭：{row['event_no']}")
                continue
            if state == 'open':
                raw_segments.append(_segment('active', active_start, at, active_event, row, zone))
            elif state == 'paused':
                raw_segments.append(_segment('pause', pause_start, at, pause_event, row, zone))
            else:
                fail(f"关闭前无有效开单：{row['event_no']}")
                continue
            state = 'closed'
            close_at = at
            continue
        if kind == 'revoke':
            if state in ('open', 'paused'):
                if state == 'open':
                    raw_segments.append(_segment('active', active_start, at, active_event, row, zone))
                else:
                    raw_segments.append(_segment('pause', pause_start, at, pause_event, row, zone))
            elif state != 'closed':
                fail(f"撤销前无有效开单：{row['event_no']}")
                continue
            state = 'revoked'
            revoked_at = at
            continue

    if errors:
        return _quarantined('；'.join(errors), policy_version, _revision_map(effective))

    if state == 'empty' or open_event is None:
        return _quarantined('缺少开单事件', policy_version, _revision_map(effective))

    payload = normalize_payload(open_event.get('payload') or {})
    period = payload.get('period')
    department = payload.get('department')
    priority = payload.get('priority')
    if not period or not department or not priority:
        return _quarantined('开单缺少期间、小组或优先级', policy_version, _revision_map(effective))
    if priority not in policy.get('sla_minutes', {}):
        return _quarantined(f'优先级无SLA口径：{priority}', policy_version, _revision_map(effective))

    resolved_policy = payload.get('policy_version') or policy_version
    limit = policy['sla_minutes'][priority]
    summary = {
        'request_no': open_event.get('request_no'),
        'state': state,
        'state_label': STATE_LABELS[state],
        'period': period,
        'department': department,
        'priority': priority,
        'policy_version': resolved_policy,
        'sla_minutes': limit,
    }

    if state in ('open', 'paused'):
        summary.update({
            'counted': False,
            'gross_minutes': 0.0,
            'pause_minutes': 0.0,
            'work_minutes': 0.0,
            'overdue': False,
            'reason': '请求尚未关闭，暂不计入SLA',
        })
        return {
            'state': state,
            'reason': summary['reason'],
            'counted': False,
            'period': period,
            'department': department,
            'priority': priority,
            'opened_at': open_at,
            'closed_at': None,
            'revoked_at': None,
            'policy_version': resolved_policy,
            'gross_seconds': 0.0,
            'pause_seconds': 0.0,
            'effective_seconds': 0.0,
            'gross_minutes': 0.0,
            'pause_minutes': 0.0,
            'work_minutes': 0.0,
            'overdue': False,
            'summary': summary,
            'segments': _with_minutes(raw_segments, policy, counted=False),
            'revision_map': _revision_map(effective),
            'event_count': len(ordered_rows),
            'duplicate_count': duplicates,
        }

    if state == 'revoked':
        summary.update({
            'counted': False,
            'gross_minutes': 0.0,
            'pause_minutes': 0.0,
            'work_minutes': 0.0,
            'overdue': False,
            'reason': '请求已撤销，不计入SLA',
        })
        return {
            'state': state,
            'reason': summary['reason'],
            'counted': False,
            'period': period,
            'department': department,
            'priority': priority,
            'opened_at': open_at,
            'closed_at': close_at,
            'revoked_at': revoked_at,
            'policy_version': resolved_policy,
            'gross_seconds': 0.0,
            'pause_seconds': 0.0,
            'effective_seconds': 0.0,
            'gross_minutes': 0.0,
            'pause_minutes': 0.0,
            'work_minutes': 0.0,
            'overdue': False,
            'summary': summary,
            'segments': _with_minutes(raw_segments, policy, counted=False),
            'revision_map': _revision_map(effective),
            'event_count': len(ordered_rows),
            'duplicate_count': duplicates,
        }

    # 已关闭：毛业务分钟 = 开单至关闭 ∩ 工作日历；暂停分钟 = 暂停区间 ∩ 日历；
    # 有效分钟 = 各计费段 ∩ 日历，等价于毛值扣除暂停交集。
    gross_seconds = _work_seconds(open_at, close_at, policy)
    pause_seconds = sum(
        _work_seconds(seg['start_at'], seg['end_at'], policy)
        for seg in raw_segments if seg['kind'] == 'pause'
    )
    effective_seconds = sum(
        _work_seconds(seg['start_at'], seg['end_at'], policy)
        for seg in raw_segments if seg['kind'] == 'active'
    )
    work_minutes = _minutes(effective_seconds)
    overdue = work_minutes > limit
    summary.update({
        'counted': True,
        'gross_minutes': _minutes(gross_seconds),
        'pause_minutes': _minutes(pause_seconds),
        'work_minutes': work_minutes,
        'overdue': overdue,
        'reason': '',
    })
    return {
        'state': state,
        'reason': '',
        'counted': True,
        'period': period,
        'department': department,
        'priority': priority,
        'opened_at': open_at,
        'closed_at': close_at,
        'revoked_at': None,
        'policy_version': resolved_policy,
        'gross_seconds': gross_seconds,
        'pause_seconds': pause_seconds,
        'effective_seconds': effective_seconds,
        'gross_minutes': _minutes(gross_seconds),
        'pause_minutes': _minutes(pause_seconds),
        'work_minutes': work_minutes,
        'overdue': overdue,
        'summary': summary,
        'segments': _with_minutes(raw_segments, policy, counted=True),
        'revision_map': _revision_map(effective),
        'event_count': len(ordered_rows),
        'duplicate_count': duplicates,
    }


def _with_minutes(segments, policy, counted):
    out = []
    for index, seg in enumerate(segments, start=1):
        seconds = _work_seconds(seg['start_at'], seg['end_at'], policy) if counted else 0.0
        out.append({
            'seq': index,
            'kind': seg['kind'],
            'kind_label': '计费' if seg['kind'] == 'active' else '暂停',
            'start_at': seg['start_at'],
            'end_at': seg['end_at'],
            'start_iso': seg['start_iso'],
            'end_iso': seg['end_iso'],
            'seconds': round(seconds, 3),
            'minutes': _minutes(seconds),
            'start_event': seg['start_event'],
            'start_revision': seg['start_revision'],
            'end_event': seg['end_event'],
            'end_revision': seg['end_revision'],
        })
    return out


def _revision_map(events):
    return [
        {
            'event_no': row['event_no'],
            'revision': row['revision'],
            'kind': row['kind'],
            'kind_label': KIND_LABELS.get(row['kind'], row['kind']),
            'occurred_at': row['occurred_at'],
            'occurred_iso': row.get('occurred_at_iso'),
            'received_at': row.get('received_at'),
            'fingerprint': row.get('fingerprint'),
        }
        for row in sorted(events, key=_sort_key)
    ]


def _quarantined(reason, policy_version, revision_map=None):
    revision_map = revision_map or []
    return {
        'state': 'quarantined',
        'reason': reason,
        'counted': False,
        'period': None,
        'department': None,
        'priority': None,
        'opened_at': None,
        'closed_at': None,
        'revoked_at': None,
        'policy_version': policy_version,
        'gross_seconds': 0.0,
        'pause_seconds': 0.0,
        'effective_seconds': 0.0,
        'gross_minutes': 0.0,
        'pause_minutes': 0.0,
        'work_minutes': 0.0,
        'overdue': False,
        'summary': {'state': 'quarantined', 'state_label': STATE_LABELS['quarantined'], 'reason': reason, 'counted': False},
        'segments': [],
        'revision_map': revision_map,
        'event_count': len(revision_map),
        'duplicate_count': 0,
    }
