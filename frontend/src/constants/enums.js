export const ROLES=['admin','analyst','viewer'];
export const STATUSES={queued:'排队中',running:'计算中',completed:'已完成',failed:'失败'};
export const CLOCK_KINDS=[['open','开单'],['pause','暂停'],['resume','恢复'],['close','关闭'],['revoke','撤销']];
export const CLOCK_STATES={empty:'无事件',open:'进行中',paused:'暂停中',closed:'已关闭',revoked:'已撤销',quarantined:'已隔离'};
export const EVENT_STATUS={accepted:'已接收',duplicate:'重复忽略',conflict:'同版异值'};
