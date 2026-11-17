import csv,io,json
from fastapi import HTTPException

def _write(rows,empty):
 if not rows: raise HTTPException(409,empty)
 stream=io.StringIO();keys=list(dict.fromkeys(k for row in rows for k in row));w=csv.DictWriter(stream,fieldnames=keys);w.writeheader()
 for row in rows: w.writerow({k:(json.dumps(v,ensure_ascii=False) if isinstance(v,(dict,list)) else v) for k,v in row.items()})
 return '﻿'+stream.getvalue()

def csv_text(result):
 return _write(result.get('rows',[]),'尚无结果')

def csv_rows(rows,empty='尚无结果'):
 return _write(rows,empty)
