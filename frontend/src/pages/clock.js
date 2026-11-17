import {heading} from '../components/common/header.js';
import {table} from '../components/common/table.js';
import {esc,time,fmt} from '../utils/format.js';
import {CLOCK_KINDS,CLOCK_STATES,EVENT_STATUS} from '../constants/enums.js';
import {store} from '../stores/business.js';

const kindOptions=CLOCK_KINDS.map(([v,l])=>`<option value="${v}">${l}</option>`).join('');
const stateTag=(state,label)=>`<span class="status ${esc(state)}">${esc(label||CLOCK_STATES[state]||state)}</span>`;

function eventForm(c){
 const d=store.clockDraft||{};
 return `<details open><summary>补发、更正或新增事件</summary>
 <form id="clock-event-form">
  <div class="form-grid">
   <label>请求号<input name="request_no" value="${esc(d.request_no)}" required></label>
   <label>事件号<input name="event_no" value="${esc(d.event_no)}" required></label>
   <label>修订号（更正时提升）<input name="revision" type="number" min="1" value="${esc(d.revision)}" required></label>
   <label>事件类型<select name="kind">${CLOCK_KINDS.map(([v,l])=>`<option value="${v}" ${d.kind===v?'selected':''}>${l}</option>`).join('')}</select></label>
   <label>发生时间（ISO+时区）<input name="occurred_at" value="${esc(d.occurred_at)}" required></label>
   <label>结算月份<input name="period" value="${esc(d.period)}"></label>
   <label>分析小组<input name="department" value="${esc(d.department)}"></label>
   <label>优先级<input name="priority" value="${esc(d.priority)}"></label>
   <label>口径版本（留空用最新）<input name="policy_version" value="${esc(d.policy_version)}"></label>
  </div>
  <p class="hint">开单事件携带期间/小组/优先级；暂停、恢复、关闭、撤销只需请求号、事件号与发生时间。晚到事件按发生时间补齐，不必按到达顺序。</p>
  <button>提交事件</button>
 </form>
 <details><summary>批量粘贴事件（JSON 数组）</summary>
  <form id="clock-batch-form">
   <label>事件数组<textarea name="events" rows="6" placeholder='[{"request_no":"R1","event_no":"E1","revision":1,"kind":"open","occurred_at":"2026-11-17T09:00:00+08:00","payload":{"period":"2026-11","department":"数据建模组","priority":"普通"}}]'></textarea></label>
   <button>批量接收</button>
  </form>
 </details></details>`;
}

function detailSection(detail){
 if(!detail) return '<p class="empty">选择左侧请求查看版本、计时段与分钟依据。</p>';
 const r=detail.request;
 const versions=detail.versions||[];
 const selected=detail.latest;
 return `<section class="report">
  <div class="report-top"><h2>${esc(r.request_no)} ${stateTag(r.state,r.state_label)}</h2><span class="version-badge">当前版本 v${r.version}</span></div>
  <p>${r.period?esc(r.period)+' · ':''}${r.department?esc(r.department)+' · ':''}${r.priority?esc(r.priority)+' · ':''}口径 ${esc(r.policy_version||'—')} · 有效 ${fmt(r.minutes)} 分钟 ${r.overdue?'<b class="error">已超时</b>':''}</p>
  ${r.reason?`<p class="warning">${esc(r.reason)}</p>`:''}
  <p class="checksum">事件 ${r.event_count} 条 · 冲突 ${r.conflict_count} 条 · 最近接收 ${r.last_received?esc(time(r.last_received)):'—'}</p>
  <div class="clock-actions">
   <button type="button" class="compose-clock" data-no="${esc(r.request_no)}">补发/更正此请求</button>
   <button type="button" class="rebuild-clock" data-no="${esc(r.request_no)}">立即重建</button>
   <button type="button" class="download-clock" data-no="${esc(r.request_no)}">导出分钟依据 CSV</button>
  </div>
  ${selected?`<div class="metrics">
   <div><small>毛业务分钟</small><strong>${fmt(selected.gross_minutes)}</strong></div>
   <div><small>暂停扣除（与日历求交）</small><strong>${fmt(selected.pause_minutes)}</strong></div>
   <div><small>有效业务分钟</small><strong>${fmt(selected.work_minutes)}</strong></div>
   <div><small>SLA分钟</small><strong>${fmt(selected.summary?.sla_minutes)}</strong></div>
   <div><small>是否超时</small><strong>${selected.summary?.overdue?'是':'否'}</strong></div>
   <div><small>计入汇总</small><strong>${selected.counted?'是':'否'}</strong></div></div>
  <h3>有效计时段</h3>${table((selected.segments||[]).map(s=>({段号:s.seq,类型:s.kind_label,开始:s.start_iso,结束:s.end_iso,业务分钟:s.minutes,起点:`${s.start_event}@${s.start_revision}`,终点:`${s.end_event}@${s.end_revision}`})))}`
  :'<p class="empty">尚无发布版本。</p>'}
  <h3>事件修订（按接收顺序）</h3>${table((detail.events||[]).map(e=>({请求号:r.request_no,事件号:e.event_no,修订:e.revision,类型:e.kind_label,发生时间:e.occurred_iso,接收时间:e.received_iso,状态:EVENT_STATUS[e.status]||e.status,说明:e.reason})),['事件号','修订','类型','发生时间','接收时间','状态','说明'])}
  <h3>版本历史</h3>${table(versions.map(v=>({版本:'v'+v.version,状态:CLOCK_STATES[v.state]||v.state,计入:v.counted?'是':'否',有效分钟:v.work_minutes,暂停扣除:v.pause_minutes,发布于:time(v.published_at),旧版:v.stale?'已失效':'当前',事件摘要:(v.event_digest||'').slice(0,12),原因:v.reason||'—'})))}
 </section>`;
}

export function clock(c,d){
 const list=d['clock/requests']||[];
 const detail=store.clock;
 return heading('EVENT CLOCK / 04','事件业务时钟','按请求号、事件号、修订号接收开单/暂停/恢复/关闭/撤销；晚到补齐，同版异值隔离，重复投递不重复计入。')+
  eventForm(c)+
  `<div class="clock-toolbar"><button type="button" id="run-clock-jobs">处理积压重建任务</button><span class="hint">进程中断后由数据库任务队列恢复；同一请求的旧计算不会覆盖新版本。</span></div>`+
  `<div class="run-layout"><aside class="run-list">${list.length?list.map(r=>`<button class="select-clock ${store.clockSelected===r.request_no?'active':''}" data-no="${esc(r.request_no)}">${esc(r.request_no)}<span class="status ${esc(r.state)}">${esc(r.state_label)}</span><small>${fmt(r.minutes)} 分钟 · v${r.version}</small></button>`).join(''):'<p class="empty">暂无请求。</p>'}</aside><div>${detailSection(detail)}</div></div>`;
}
