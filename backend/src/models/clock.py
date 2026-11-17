"""业务时钟：事件请求、事件、版本化计时发布与重建任务。

设计要点：
- clock_events 只追加不覆盖；同 (request_no,event_no,revision) 的不同内容各存一行，
  以 fingerprint 区分，status 标记 accepted / conflict / duplicate。
- clock_releases 是"完整同版"发布：事件修订集合、计时明细与汇总贡献在同一次提交中写入。
- clock_jobs 是数据库内的重建任务/中断恢复队列，按请求号去重。
"""
from sqlalchemy import (
    Boolean,
    Column,
    Float,
    Index,
    Integer,
    JSON,
    String,
    Text,
    UniqueConstraint,
)
from src.config.database import Base


class ClockRequest(Base):
    __tablename__ = 'clock_requests'
    id = Column(Integer, primary_key=True)
    request_no = Column(String(80), unique=True, nullable=False, index=True)
    state = Column(String(20), nullable=False, default='empty')  # empty/open/paused/closed/revoked/quarantined
    period = Column(String(40))
    department = Column(String(120))
    priority = Column(String(40))
    opened_at = Column(Float)
    closed_at = Column(Float)
    revoked_at = Column(Float)
    version = Column(Integer, nullable=False, default=0)
    minutes = Column(Float, nullable=False, default=0)
    overdue = Column(Boolean, nullable=False, default=False)
    policy_version = Column(String(120))
    reason = Column(Text, default='')
    event_count = Column(Integer, nullable=False, default=0)
    conflict_count = Column(Integer, nullable=False, default=0)
    last_received = Column(Float)
    published_at = Column(Float)
    change_seq = Column(Integer, nullable=False, default=0)
    created = Column(Float)
    updated = Column(Float)


class ClockEvent(Base):
    __tablename__ = 'clock_events'
    id = Column(Integer, primary_key=True)
    request_no = Column(String(80), nullable=False, index=True)
    event_no = Column(String(80), nullable=False)
    revision = Column(Integer, nullable=False, default=1)
    kind = Column(String(20), nullable=False)  # open/pause/resume/close/revoke
    occurred_at = Column(Float, nullable=False)
    occurred_at_iso = Column(String(40), nullable=False)
    received_at = Column(Float, nullable=False)
    received_iso = Column(String(40), nullable=False)
    payload = Column(JSON, nullable=False, default=dict)
    fingerprint = Column(String(64), nullable=False)
    status = Column(String(20), nullable=False, default='accepted')  # accepted/conflict/duplicate
    reason = Column(Text, default='')
    created = Column(Float)
    __table_args__ = (
        UniqueConstraint('request_no', 'event_no', 'revision', 'fingerprint', name='uq_clock_event_version'),
        Index('ix_clock_event_rebuild', 'request_no', 'status', 'occurred_at'),
        Index('ix_clock_event_received', 'received_at'),
    )


class ClockRelease(Base):
    __tablename__ = 'clock_releases'
    id = Column(Integer, primary_key=True)
    request_id = Column(Integer, nullable=False, index=True)
    request_no = Column(String(80), nullable=False, index=True)
    version = Column(Integer, nullable=False)
    state = Column(String(20), nullable=False)
    reason = Column(Text, default='')
    event_digest = Column(String(64), nullable=False)
    event_count = Column(Integer, nullable=False, default=0)
    revision_map = Column(JSON, nullable=False, default=list)
    intervals = Column(JSON, nullable=False, default=list)
    gross_seconds = Column(Float, nullable=False, default=0)
    pause_seconds = Column(Float, nullable=False, default=0)
    effective_seconds = Column(Float, nullable=False, default=0)
    gross_minutes = Column(Float, nullable=False, default=0)
    pause_minutes = Column(Float, nullable=False, default=0)
    work_minutes = Column(Float, nullable=False, default=0)
    policy_version = Column(String(120))
    summary = Column(JSON, nullable=False, default=dict)
    published_at = Column(Float, nullable=False)
    counted = Column(Boolean, nullable=False, default=False)
    stale = Column(Boolean, nullable=False, default=False)
    __table_args__ = (
        UniqueConstraint('request_id', 'version', name='uq_clock_release_version'),
        Index('ix_clock_release_latest', 'request_id', 'version'),
    )


class ClockSegment(Base):
    __tablename__ = 'clock_segments'
    id = Column(Integer, primary_key=True)
    release_id = Column(Integer, nullable=False, index=True)
    seq = Column(Integer, nullable=False)
    kind = Column(String(20), nullable=False)  # active/pause
    start_at = Column(Float, nullable=False)
    end_at = Column(Float)
    seconds = Column(Float)


class ClockJob(Base):
    __tablename__ = 'clock_jobs'
    id = Column(Integer, primary_key=True)
    request_no = Column(String(80), unique=True, nullable=False, index=True)
    status = Column(String(20), nullable=False, default='pending')  # pending/processing/done
    dirty = Column(Boolean, nullable=False, default=False)
    attempts = Column(Integer, nullable=False, default=0)
    available_at = Column(Float, nullable=False, default=0)
    lease_until = Column(Float, nullable=False, default=0)
    requested_at = Column(Float)
    updated = Column(Float)
    last_error = Column(Text, default='')
