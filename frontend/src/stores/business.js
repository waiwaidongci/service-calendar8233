import {loadAll} from '../api/business.js';
export const store={session:JSON.parse(localStorage.getItem('session')||'null'),data:{sources:[],periods:[],policies:[],runs:[],schedules:[],deliveries:[],audits:[],'clock/requests':[]},selected:null,clock:null,clockSelected:null,clockDraft:{request_no:'REQ-2026-1106',event_no:'E1',revision:1,kind:'open',occurred_at:'2026-11-17T09:00:00+08:00',period:'2026-11',department:'数据建模组',priority:'普通',policy_version:''}};
export async function reload(){store.data=await loadAll()}
export function setSession(value){store.session=value;if(value)localStorage.setItem('session',JSON.stringify(value));else localStorage.removeItem('session')}
