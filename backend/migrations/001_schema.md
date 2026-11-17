# 初始结构

SQLAlchemy启动创建以下业务表：service_periods, service_cases, business_calendars, sla_runs, delivery_schedules, deliveries、users、audits。Source专属JSON字段由对应validation严格校验；Policy为不可覆盖的版本记录；Run保存source/policy冻结快照；Period仅用于已有期间选择器，不计入核心复杂度。已有数据库升级需先备份，后续字段演进应以独立迁移执行。

## 事件业务时钟（增量）

`Base.metadata.create_all` 会为已有库补建新表（不修改既有表结构）：`clock_requests`（请求当前态与版本号）、`clock_events`（只追加事件与隔离状态）、`clock_releases`（不可变完整发布版）、`clock_segments`（计时段明细）、`clock_jobs`（重建任务与租约）。`sla_runs` 新增 `kind`/`cutoff`/`period` 三列用于区分底稿核算与事件时钟报告；既有 PostgreSQL 库需执行等价 `ALTER TABLE sla_runs ADD COLUMN ...` 后再切换 worker。
