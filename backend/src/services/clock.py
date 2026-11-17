"""业务时钟事件接收、版本化重建、并发发布与报告冻结。"""
from __future__ import annotations

import hashlib
import json
import time

from fastapi import HTTPException
from sqlalchemy.exc import IntegrityError

from src.config.database import Session
from src.config.settings import CONFIG
from src.models.clock import (
    ClockEvent,
    ClockJob,
    ClockRelease,
    ClockRequest,
    ClockSegment,
)
from src.models.source import Policy
from src.services.audit import log
from src.services.clock_engine import (
    KIND_LABELS,
    KINDS,
    STATE_LABELS,
    build_release,
    local_iso,
    normalize_payload,
    parse_when,
    utc_iso,
)
from src.services.validation import validate_policy

JOB_LEASE_SECONDS = 60
MAX_BATCH = 500
MAX_REASON = 2000


def _trim(value, name, limit=80):
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f'{name}不能为空')
    value = value.strip()
    if len(value) > limit:
        raise ValueError(f'{name}长度不能超过{limit}')
    return value


def validate_event(item):
    if not isinstance(item, dict):
        raise ValueError('事件必须为对象')
    allowed = {'request_no', 'event_no', 'revision', 'kind', 'occurred_at', 'payload'}
    extra = set(item) - allowed
    if extra:
        raise ValueError('未知事件字段：' + '、'.join(sorted(extra)))
    request_no = _trim(item.get('request_no'), '请求号')
    event_no = _trim(item.get('event_no'), '事件号')
    revision = item.get('revision')
    if isinstance(revision, bool) or not isinstance(revision, int) or not 1 <= revision <= 1_000_000:
        raise ValueError('修订号须为不小于1的整数')
    kind = item.get('kind')
    if kind not in KINDS:
        raise ValueError('事件类型须为 open/pause/resume/close/revoke')
    occurred_at = parse_when(item.get('occurred_at'))
    payload = item.get('payload') or {}
    if not isinstance(payload, dict):
        raise ValueError('事件业务字段须为对象')
    payload = normalize_payload(payload)
    return {
        'request_no': request_no,
        'event_no': event_no,
        'revision': revision,
        'kind': kind,
        'occurred_at': occurred_at,
        'occurred_at_iso': utc_iso(occurred_at),
        'payload': payload,
    }


def _fingerprint(item):
    body = {
        'kind': item['kind'],
        'occurred_at': utc_iso(item['occurred_at']),
        'payload': normalize_payload(item['payload']),
    }
    return hashlib.sha256(json.dumps(body, sort_keys=True, ensure_ascii=False).encode()).hexdigest()


def _get_or_create_request(db, request_no, now):
    query = db.query(ClockRequest).filter_by(request_no=request_no)
    request = query.with_for_update().one_or_none()
    if request:
        return request
    try:
        with db.begin_nested():
            request = ClockRequest(
                request_no=request_no,
                state='empty',
                version=0,
                change_seq=0,
                event_count=0,
                conflict_count=0,
                created=now,
                updated=now,
            )
            db.add(request)
            db.flush()
        return request
    except IntegrityError:
        # 并发首写：另一个事务已创建请求，重试读取其已提交行。
        for _ in range(5):
            request = query.with_for_update().one_or_none()
            if request:
                return request
            time.sleep(0.02)
        raise


def _enqueue(db, request_no, now):
    job = db.query(ClockJob).filter_by(request_no=request_no).with_for_update().one_or_none()
    if not job:
        job = ClockJob(
            request_no=request_no,
            status='pending',
            dirty=True,
            attempts=0,
            available_at=now,
            lease_until=0,
            requested_at=now,
            updated=now,
        )
        try:
            with db.begin_nested():
                db.add(job)
                db.flush()
        except IntegrityError:
            job = db.query(ClockJob).filter_by(request_no=request_no).with_for_update().one()
            job.dirty = True
            job.available_at = now
            job.updated = now
            if job.status != 'processing':
                job.status = 'pending'
        return job
    job.dirty = True
    job.available_at = now
    job.updated = now
    if job.status != 'processing':
        job.status = 'pending'
    return job


def receive_events(db, user, items, process_inline=True):
    """接收一批事件。整批校验失败则不写入；单条同版异值隔离但其余照常接收。"""
    if hasattr(items, 'model_dump'):
        items = [x.model_dump() for x in getattr(items, 'events', [])]
    elif isinstance(items, dict):
        items = [items]
    if not isinstance(items, list) or not items:
        raise ValueError('请至少提交一条事件')
    if len(items) > MAX_BATCH:
        raise ValueError(f'单批事件不能超过{MAX_BATCH}条')
    validated = [validate_event(item) for item in items]
    now = time.time()
    results = []
    touched = []
    seen = set()
    for item in validated:
        fingerprint = _fingerprint(item)
        item['fingerprint'] = fingerprint
        request = _get_or_create_request(db, item['request_no'], now)
        received_at = now
        existing = (
            db.query(ClockEvent)
            .filter_by(
                request_no=item['request_no'],
                event_no=item['event_no'],
                revision=item['revision'],
            )
            .with_for_update()
            .all()
        )
        status = 'accepted'
        reason = ''
        if existing and any(row.fingerprint == fingerprint for row in existing):
            status = 'duplicate'
            reason = '重复投递：同请求号、事件号、修订号且内容一致，不重复计入'
            results.append(
                {
                    'request_no': item['request_no'],
                    'event_no': item['event_no'],
                    'revision': item['revision'],
                    'kind': item['kind'],
                    'status': status,
                    'reason': reason,
                }
            )
            continue
        if existing:
            status = 'conflict'
            reason = (
                f"同版异值：事件 {item['event_no']} 修订 {item['revision']} "
                '已存在不同内容，已隔离；请提升修订号更正'
            )
            request.conflict_count = (request.conflict_count or 0) + 1
        row = ClockEvent(
            request_no=item['request_no'],
            event_no=item['event_no'],
            revision=item['revision'],
            kind=item['kind'],
            occurred_at=item['occurred_at'],
            occurred_at_iso=item['occurred_at_iso'],
            received_at=received_at,
            received_iso=utc_iso(received_at),
            payload=item['payload'],
            fingerprint=fingerprint,
            status=status,
            reason=reason[:MAX_REASON],
            created=now,
        )
        request.last_received = max(request.last_received or 0, received_at)
        request.change_seq = (request.change_seq or 0) + 1
        request.updated = now
        try:
            with db.begin_nested():
                db.add(row)
                db.flush()
        except IntegrityError:
            # 并发投递同一版本：唯一键兜底，按已存在内容判定重复或冲突。
            rows = db.query(ClockEvent).filter_by(
                request_no=item['request_no'],
                event_no=item['event_no'],
                revision=item['revision'],
            ).all()
            if any(x.fingerprint == fingerprint for x in rows):
                status = 'duplicate'
                reason = '重复投递：同请求号、事件号、修订号且内容一致，不重复计入'
                request.change_seq = (request.change_seq or 0) - 1
            else:
                status = 'conflict'
                reason = (
                    f"同版异值：事件 {item['event_no']} 修订 {item['revision']} "
                    '已被并发写入不同内容，已隔离；请提升修订号更正'
                )
                request.conflict_count = (request.conflict_count or 0) + 1
            results.append(
                {
                    'request_no': item['request_no'],
                    'event_no': item['event_no'],
                    'revision': item['revision'],
                    'kind': item['kind'],
                    'status': status,
                    'reason': reason,
                }
            )
            if status == 'duplicate':
                continue
            row = ClockEvent(
                request_no=item['request_no'],
                event_no=item['event_no'],
                revision=item['revision'],
                kind=item['kind'],
                occurred_at=item['occurred_at'],
                occurred_at_iso=item['occurred_at_iso'],
                received_at=received_at,
                received_iso=utc_iso(received_at),
                payload=item['payload'],
                fingerprint=fingerprint,
                status=status,
                reason=reason[:MAX_REASON],
                created=now,
            )
            with db.begin_nested():
                db.add(row)
                db.flush()
        key = (item['request_no'], item['event_no'], item['revision'], fingerprint)
        if key not in seen:
            seen.add(key)
            touched.append(item['request_no'])
        results.append(
            {
                'request_no': item['request_no'],
                'event_no': item['event_no'],
                'revision': item['revision'],
                'kind': item['kind'],
                'status': status,
                'reason': reason,
            }
        )
        log(
            db,
            user.email if hasattr(user, 'email') else str(user),
            'clock.event.receive',
            f"{item['request_no']}/{item['event_no']}@{item['revision']} {item['kind']} {status}",
        )
    for request_no in dict.fromkeys(touched):
        _enqueue(db, request_no, now)
    db.commit()
    if process_inline:
        for request_no in dict.fromkeys(touched):
            try:
                run_job(request_no)
            except Exception as exc:  # 保留待处理任务，worker 可继续恢复
                _mark_retry(request_no, str(exc))
        db.expire_all()
    for result in results:
        request = db.query(ClockRequest).filter_by(request_no=result['request_no']).one_or_none()
        if request:
            result['request_version'] = request.version
            result['request_state'] = request.state
    return results


def _mark_retry(request_no, error):
    with Session() as db:
        job = db.query(ClockJob).filter_by(request_no=request_no).one_or_none()
        if job:
            job.status = 'pending'
            job.dirty = True
            job.lease_until = 0
            job.available_at = time.time() + 1
            job.last_error = str(error)[:MAX_REASON]
            job.updated = time.time()
            db.commit()


def _choose_policy(db, request, events):
    """请求首次发布即冻结口径名；后续更正沿用同一版本，避免跨版本漂移。"""
    candidates = []
    if request.policy_version:
        candidates.append(request.policy_version)
    for row in events:
        if row.status == 'accepted' and row.kind == 'open':
            value = (row.payload or {}).get('policy_version')
            if value:
                candidates.append(value)
    for version in dict.fromkeys(candidates):
        policy = db.query(Policy).filter_by(version=version).first()
        if policy:
            return policy.data, policy.version
    latest = db.query(Policy).order_by(Policy.id.desc()).first()
    if latest:
        return latest.data, latest.version
    return CONFIG['policy'], CONFIG['policy']['version']


def run_job(request_no, now=None):
    """声明并执行一次重建；计算与发布分离，发布时以 change_seq/version 双 CAS 防旧覆盖。"""
    now = now or time.time()
    with Session() as db:
        job = db.query(ClockJob).filter_by(request_no=request_no).with_for_update().one_or_none()
        if not job:
            return False
        if job.status == 'processing' and (job.lease_until or 0) > now:
            return False
        if job.status not in ('pending', 'processing'):
            return False
        if job.status == 'pending' and (job.available_at or 0) > now:
            return False
        request = db.query(ClockRequest).filter_by(request_no=request_no).with_for_update().one_or_none()
        if not request:
            job.status = 'done'
            job.dirty = False
            db.commit()
            return False
        expected_seq = request.change_seq
        expected_version = request.version
        my_lease = now + JOB_LEASE_SECONDS
        job.status = 'processing'
        job.lease_until = my_lease
        job.attempts = (job.attempts or 0) + 1
        job.updated = now
        db.commit()

    with Session() as db:
        request = db.query(ClockRequest).filter_by(request_no=request_no).one()
        events = (
            db.query(ClockEvent)
            .filter_by(request_no=request_no)
            .order_by(ClockEvent.occurred_at, ClockEvent.received_at, ClockEvent.event_no, ClockEvent.revision)
            .all()
        )
        policy, policy_version = _choose_policy(db, request, events)
        try:
            release = build_release(events, policy, policy_version)
        except Exception as exc:  # 口径异常等不得阻断队列
            release = _failed_release(str(exc), policy_version)

    with Session() as db:
        job = db.query(ClockJob).filter_by(request_no=request_no).with_for_update().one()
        request = db.query(ClockRequest).filter_by(request_no=request_no).with_for_update().one()
        if job.status != 'processing' or job.lease_until != my_lease:
            # 租约已被回收或另一进程接管：放弃本份结果，由当前持有者发布。
            db.rollback()
            return False
        if request.change_seq != expected_seq or request.version != expected_version:
            # 计算期间已有新事件或已被另一发布推进：丢弃这份旧结果并稍后重算。
            job.status = 'pending'
            job.dirty = True
            job.lease_until = 0
            job.available_at = now + 0.2
            job.updated = now
            db.commit()
            return False

        version = request.version + 1
        event_rows = (
            db.query(ClockEvent)
            .filter_by(request_no=request_no)
            .all()
        )
        accepted = [row for row in event_rows if row.status == 'accepted']
        digest = hashlib.sha256(
            json.dumps(
                sorted(
                    f'{row.event_no}@{row.revision}:{row.fingerprint}'
                    for row in accepted
                )
            ).encode()
        ).hexdigest()
        published_at = time.time()
        record = ClockRelease(
            request_id=request.id,
            request_no=request.request_no,
            version=version,
            state=release['state'],
            reason=(release.get('reason') or '')[:MAX_REASON],
            event_digest=digest,
            event_count=len(release.get('revision_map') or []),
            revision_map=release.get('revision_map') or [],
            intervals=release.get('segments') or [],
            gross_seconds=release.get('gross_seconds') or 0,
            pause_seconds=release.get('pause_seconds') or 0,
            effective_seconds=release.get('effective_seconds') or 0,
            gross_minutes=release.get('gross_minutes') or 0,
            pause_minutes=release.get('pause_minutes') or 0,
            work_minutes=release.get('work_minutes') or 0,
            policy_version=release.get('policy_version'),
            summary=release.get('summary') or {},
            published_at=published_at,
            counted=bool(release.get('counted')),
            stale=False,
        )
        db.add(record)
        db.flush()
        for segment in release.get('segments') or []:
            db.add(
                ClockSegment(
                    release_id=record.id,
                    seq=segment['seq'],
                    kind=segment['kind'],
                    start_at=segment['start_at'],
                    end_at=segment['end_at'],
                    seconds=segment.get('seconds') or 0,
                )
            )
        db.query(ClockRelease).filter(
            ClockRelease.request_id == request.id,
            ClockRelease.version < version,
        ).update({'stale': True}, synchronize_session=False)

        request.state = release['state']
        request.reason = (release.get('reason') or '')[:MAX_REASON]
        request.period = release.get('period') or request.period
        request.department = release.get('department') or request.department
        request.priority = release.get('priority') or request.priority
        request.opened_at = release.get('opened_at')
        request.closed_at = release.get('closed_at')
        request.revoked_at = release.get('revoked_at')
        request.version = version
        request.minutes = release.get('work_minutes') or 0
        request.overdue = bool(release.get('overdue'))
        request.policy_version = release.get('policy_version') or request.policy_version
        request.published_at = published_at
        request.updated = published_at
        request.event_count = len(release.get('revision_map') or [])
        request.conflict_count = sum(1 for row in event_rows if row.status == 'conflict')

        job.status = 'done'
        job.dirty = False
        job.lease_until = 0
        job.last_error = ''
        job.updated = published_at
        db.commit()
    return True


def _failed_release(reason, policy_version):
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
        'summary': {'state': 'quarantined', 'state_label': '已隔离', 'reason': reason, 'counted': False},
        'segments': [],
        'revision_map': [],
        'event_count': 0,
        'duplicate_count': 0,
    }


def recover_jobs(now=None):
    """进程重启或中断恢复：回收过期租约，重新排队。"""
    now = now or time.time()
    with Session() as db:
        stale = (
            db.query(ClockJob)
            .filter(ClockJob.status == 'processing', ClockJob.lease_until < now)
            .with_for_update()
            .all()
        )
        for job in stale:
            job.status = 'pending'
            job.dirty = True
            job.available_at = now
            job.updated = now
        if stale:
            db.commit()
        return len(stale)


def process_due_jobs(limit=20, now=None):
    now = now or time.time()
    recover_jobs(now)
    with Session() as db:
        rows = (
            db.query(ClockJob.request_no)
            .filter(ClockJob.status == 'pending', ClockJob.available_at <= now)
            .order_by(ClockJob.available_at, ClockJob.id)
            .limit(limit)
            .all()
        )
    done = 0
    for (request_no,) in rows:
        if run_job(request_no, now):
            done += 1
    return done


def request_detail(db, request_no):
    request = db.query(ClockRequest).filter_by(request_no=request_no).one_or_none()
    if not request:
        raise HTTPException(404, '请求不存在')
    releases = (
        db.query(ClockRelease)
        .filter_by(request_id=request.id)
        .order_by(ClockRelease.version.desc())
        .all()
    )
    versions = []
    for release in releases:
        versions.append(
            {
                'version': release.version,
                'state': release.state,
                'reason': release.reason,
                'counted': release.counted,
                'stale': release.stale,
                'published_at': release.published_at,
                'event_digest': release.event_digest,
                'event_count': release.event_count,
                'gross_minutes': release.gross_minutes,
                'pause_minutes': release.pause_minutes,
                'work_minutes': release.work_minutes,
                'summary': release.summary,
                'revision_map': release.revision_map,
                'segments': release.intervals,
            }
        )
    events = (
        db.query(ClockEvent)
        .filter_by(request_no=request_no)
        .order_by(ClockEvent.received_at, ClockEvent.event_no, ClockEvent.revision)
        .all()
    )
    return {
        'request': {
            'request_no': request.request_no,
            'state': request.state,
            'state_label': STATE_LABELS.get(request.state, request.state),
            'version': request.version,
            'period': request.period,
            'department': request.department,
            'priority': request.priority,
            'minutes': request.minutes,
            'overdue': request.overdue,
            'policy_version': request.policy_version,
            'reason': request.reason,
            'event_count': request.event_count,
            'conflict_count': request.conflict_count,
            'change_seq': request.change_seq,
            'last_received': request.last_received,
            'published_at': request.published_at,
        },
        'latest': versions[0] if versions else None,
        'versions': versions,
        'events': [
            {
                'event_no': row.event_no,
                'revision': row.revision,
                'kind': row.kind,
                'kind_label': KIND_LABELS.get(row.kind, row.kind),
                'occurred_at': row.occurred_at,
                'occurred_iso': row.occurred_at_iso,
                'received_at': row.received_at,
                'received_iso': row.received_iso,
                'payload': row.payload,
                'status': row.status,
                'reason': row.reason,
                'fingerprint': row.fingerprint,
            }
            for row in events
        ],
    }


def list_requests(db, limit=200):
    rows = db.query(ClockRequest).order_by(ClockRequest.updated.desc()).limit(limit).all()
    return [
        {
            'request_no': row.request_no,
            'state': row.state,
            'state_label': STATE_LABELS.get(row.state, row.state),
            'period': row.period,
            'department': row.department,
            'priority': row.priority,
            'minutes': row.minutes,
            'overdue': row.overdue,
            'policy_version': row.policy_version,
            'version': row.version,
            'event_count': row.event_count,
            'conflict_count': row.conflict_count,
            'reason': row.reason,
            'last_received': row.last_received,
            'published_at': row.published_at,
        }
        for row in rows
    ]


def list_jobs(db, limit=200):
    rows = db.query(ClockJob).order_by(ClockJob.updated.desc()).limit(limit).all()
    return [
        {
            'request_no': row.request_no,
            'status': row.status,
            'dirty': row.dirty,
            'attempts': row.attempts,
            'available_at': row.available_at,
            'lease_until': row.lease_until,
            'last_error': row.last_error,
        }
        for row in rows
    ]


def _event_period(events):
    for row in events:
        if row.status == 'accepted' and row.kind == 'open':
            value = (row.payload or {}).get('period')
            if value:
                return value
    for row in events:
        value = (row.payload or {}).get('period') if row.status == 'accepted' else None
        if value:
            return value
    return None


def report_snapshot(db, period, cutoff, policy, policy_version=None):
    """按接收截止点重放，生成可长期留存的分钟依据。"""
    validate_policy(policy)
    policy_version = policy_version or policy.get('version')
    events = (
        db.query(ClockEvent)
        .filter(ClockEvent.received_at <= cutoff)
        .order_by(ClockEvent.request_no, ClockEvent.occurred_at, ClockEvent.received_at, ClockEvent.event_no)
        .all()
    )
    grouped = {}
    for row in events:
        grouped.setdefault(row.request_no, []).append(row)

    aggregates = {}
    trace = []
    quarantined = []
    open_rows = []
    revoked = []
    for request_no in sorted(grouped):
        request_events = grouped[request_no]
        event_period = _event_period(request_events)
        # 无法判定期间的残缺请求仍纳入报表（进入隔离区），避免静默丢失。
        if period != '全部' and event_period is not None and event_period != period:
            continue
        release = build_release(request_events, policy, policy_version)
        state = release['state']
        row_period = release.get('period') or event_period
        if state == 'quarantined':
            quarantined.append(
                {
                    '请求号': request_no,
                    '期间': event_period or '—',
                    '状态': STATE_LABELS['quarantined'],
                    '原因': release['reason'],
                    '事件版本': '；'.join(
                        f"{item['event_no']}@{item['revision']}" for item in (release.get('revision_map') or [])
                    ),
                }
            )
            continue
        if release.get('counted'):
            key = (row_period, release['department'], release['priority'])
            bucket = aggregates.setdefault(
                key,
                {'count': 0, 'gross': 0.0, 'pause': 0.0, 'minutes': 0.0, 'overdue': 0},
            )
            bucket['count'] += 1
            bucket['gross'] += release['gross_minutes']
            bucket['pause'] += release['pause_minutes']
            bucket['minutes'] += release['work_minutes']
            bucket['overdue'] += int(release['overdue'])
        elif state in ('open', 'paused'):
            open_rows.append(
                {
                    '请求号': request_no,
                    '期间': row_period or '—',
                    '状态': STATE_LABELS[state],
                    '原因': release['reason'],
                }
            )
        elif state == 'revoked':
            revoked.append(
                {
                    '请求号': request_no,
                    '期间': row_period or '—',
                    '状态': STATE_LABELS['revoked'],
                    '原因': release['reason'],
                }
            )
        segments = [
            {
                '段号': seg['seq'],
                '段类型': seg['kind_label'],
                '开始': seg['start_iso'],
                '结束': seg['end_iso'],
                '业务分钟': seg['minutes'],
                '起点事件': f"{seg['start_event']}@{seg['start_revision']}",
                '终点事件': f"{seg['end_event']}@{seg['end_revision']}",
            }
            for seg in release.get('segments') or []
            if seg['kind'] == 'active'
        ]
        trace.append(
            {
                '请求号': request_no,
                '状态': STATE_LABELS.get(state, state),
                '期间': row_period or '—',
                '小组': release.get('department') or '—',
                '优先级': release.get('priority') or '—',
                '开单': local_iso(release.get('opened_at'), policy['timezone']) or '—',
                '关闭': local_iso(release.get('closed_at'), policy['timezone']) or '—',
                '毛业务分钟': release.get('gross_minutes') or 0,
                '暂停扣除分钟': release.get('pause_minutes') or 0,
                '有效业务分钟': release.get('work_minutes') or 0,
                'SLA分钟': release.get('summary', {}).get('sla_minutes'),
                '是否超时': '是' if release.get('overdue') else '否',
                '事件版本': '；'.join(
                    f"{item['event_no']}@{item['revision']}" for item in (release.get('revision_map') or [])
                ),
                '计时段': segments,
            }
        )

    rows = []
    for (row_period, department, priority), bucket in sorted(aggregates.items()):
        rows.append(
            {
                '月份': row_period,
                '小组': department,
                '优先级': priority,
                '计入请求数': bucket['count'],
                '毛业务分钟': round(bucket['gross'], 6),
                '暂停扣除分钟': round(bucket['pause'], 6),
                '有效业务分钟': round(bucket['minutes'], 6),
                '平均有效分钟': round(bucket['minutes'] / bucket['count'], 6),
                '超时数': bucket['overdue'],
                '超时率': bucket['overdue'] / bucket['count'],
            }
        )
    summary = {
        '接收截止点': utc_iso(cutoff),
        '时区': policy['timezone'],
        '口径版本': policy_version,
        '请求总数': len(trace) + len(quarantined),
        '计入请求数': sum(bucket['count'] for bucket in aggregates.values()),
        '毛业务分钟合计': round(sum(bucket['gross'] for bucket in aggregates.values()), 6),
        '暂停扣除分钟合计': round(sum(bucket['pause'] for bucket in aggregates.values()), 6),
        '有效业务分钟合计': round(sum(bucket['minutes'] for bucket in aggregates.values()), 6),
        '超时数': sum(bucket['overdue'] for bucket in aggregates.values()),
        '隔离数': len(quarantined),
        '未闭合数': len(open_rows),
        '撤销数': len(revoked),
    }
    return {
        'cutoff': cutoff,
        'cutoff_iso': local_iso(cutoff, policy['timezone']),
        'policy_version': policy_version,
        'summary': summary,
        'rows': rows,
        'trace': trace,
        'quarantined': quarantined,
        'open': open_rows,
        'revoked': revoked,
        'method': (
            '事件按发生时间重放；暂停区间与工作日历求交后从毛时长扣除，'
            '仅已关闭且未撤销请求计入SLA。报告冻结接收截止点，截止后的补发不改变本报告。'
        ),
    }


def clock_csv(clock):
    from src.services.export import csv_rows

    rows = clock.get('trace') or []
    return csv_rows(rows, '分钟依据')
