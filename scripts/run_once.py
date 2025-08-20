from __future__ import annotations
import os, time, requests
from typing import Any, Dict, List, Optional, Tuple
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE_URL = "https://api.coverage.cms.gov/v1"
_HELLO_MSG = "Hello MCIM API Users!"

def _build_session() -> requests.Session:
    s = requests.Session()
    retries = Retry(
        total=5, backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retries, pool_connections=20, pool_maxsize=40)
    s.mount("https://", adapter); s.mount("http://", adapter)
    s.headers.update({"Accept": "application/json"})
    return s

_session = _build_session()
_TOKEN: Optional[str] = None
_TOKEN_EXP: float = 0.0

def _full_url(path: str) -> str:
    if path.startswith("http"): return path
    if not path.startswith("/"): path = "/" + path
    return BASE_URL + path

def ensure_license_acceptance(timeout: Optional[float] = None) -> None:
    global _TOKEN, _TOKEN_EXP
    url = _full_url("/metadata/license-agreement")
    print(f"[DEBUG] GET {url} -> ...")
    r = _session.get(url, timeout=timeout)
    try: j = r.json()
    except Exception: j = None
    if r.status_code != 200:
        raise RuntimeError(f"license-agreement {r.status_code}: {getattr(j,'text',j)}")
    print(f"[DEBUG] GET {url} -> {r.status_code}; keys={list((j or {}).keys())}")
    data = (j or {}).get("data", [])
    if isinstance(data, list) and data and "Token" in data[0]:
        _TOKEN = data[0]["Token"]
        _TOKEN_EXP = time.time() + 55*60
        print("[note] CMS license agreement accepted; session token cached.")
    else:
        _TOKEN = None; _TOKEN_EXP = 0.0
        print("[note] CMS license agreement acknowledged (no token provided).")

def _maybe_attach_token(headers: Dict[str, str]) -> Dict[str,str]:
    global _TOKEN, _TOKEN_EXP
    if _TOKEN and time.time() >= _TOKEN_EXP:
        try: ensure_license_acceptance(timeout=float(os.getenv("COVERAGE_TIMEOUT") or 30))
        except Exception as e: print(f"[warn] token refresh failed; proceeding without token: {e}")
    if _TOKEN:
        h=dict(headers); h["Authorization"]=f"Bearer {_TOKEN}"; return h
    return headers

def _get_json(method:str,path:str,params:Optional[Dict[str,Any]]=None,timeout:Optional[float]=None)->Dict[str,Any]:
    url=_full_url(path); headers=_maybe_attach_token({})
    r=_session.request(method=method.upper(),url=url,params=params or {},headers=headers,timeout=timeout)
    if r.status_code in (401,403) and _TOKEN is not None:
        ensure_license_acceptance(timeout=timeout)
        headers=_maybe_attach_token({})
        r=_session.request(method=method.upper(),url=url,params=params or {},headers=headers,timeout=timeout)
    try:j=r.json()
    except Exception:j={"text":r.text}
    if r.status_code>=400:
        msg=(j.get("message") if isinstance(j,dict) else None) or r.text
        raise RuntimeError(f"{url} {r.status_code}: {msg}")
    return j if isinstance(j,dict) else {}

# -------- reports ----------
def fetch_local_reports(timeout:Optional[float]=None)->Tuple[List[dict],List[dict]]:
    lcds=_get_json("GET","/reports/local-coverage-final-lcds",timeout=timeout).get("data",[])
    arts=_get_json("GET","/reports/local-coverage-articles",timeout=timeout).get("data",[])
    return (lcds if isinstance(lcds,list) else []),(arts if isinstance(arts,list) else [])

# -------- Article harvesting ----------
ARTICLE_ENDPOINTS=[
  "/data/article/code-table","/data/article/icd10-covered","/data/article/icd10-noncovered",
  "/data/article/hcpc-code","/data/article/hcpc-modifier","/data/article/revenue-code","/data/article/bill-codes"
]
def harvest_article_endpoints(article_row:dict,timeout:Optional[float]):
    aid=int(article_row.get("article_id") or article_row.get("document_id") or 0)
    ver=article_row.get("document_version") or article_row.get("ver")
    disp=article_row.get("article_display_id") or article_row.get("document_display_id")
    print(f"[DEBUG] [Article] {disp or aid}")
    results={}
    if not aid: return {},{"article_id":None,"article_display_id":disp,"document_version":ver}
    params={"articleid":aid}; 
    if ver: params["ver"]=ver
    for ep in ARTICLE_ENDPOINTS:
        try: data=_get_json("GET",ep,params=params,timeout=timeout).get("data",[])
        except Exception as e: print(f"[DEBUG]   -> {ep} error: {e}"); data=[]
        results[ep]=data
    return results,{"article_id":aid,"article_display_id":disp,"document_version":ver}

# -------- LCD harvesting ----------
_LCD_EP_SUPPORTED:Dict[str,Optional[bool]]={}
LCD_ENDPOINTS=[
  "/data/lcd/code-table","/data/lcd/icd10-covered","/data/lcd/icd10-noncovered",
  "/data/lcd/hcpc-code","/data/lcd/hcpc-modifier","/data/lcd/revenue-code","/data/lcd/bill-codes"
]
def harvest_lcd_endpoints(lcd_row:dict,timeout:Optional[float]):
    lid=int(lcd_row.get("lcd_id") or lcd_row.get("document_id") or 0)
    ver=lcd_row.get("document_version") or lcd_row.get("ver")
    disp=lcd_row.get("lcd_display_id") or lcd_row.get("document_display_id")
    print(f"[DEBUG] [LCD] {disp or lid}")
    if not lid: return {},{"lcd_id":None,"lcd_display_id":disp,"document_version":ver}
    params={"lcdid":lid}; 
    if ver: params["ver"]=ver
    results={}
    for ep in LCD_ENDPOINTS:
        if _LCD_EP_SUPPORTED.get(ep) is False: continue
        try:data=_get_json("GET",ep,params=params,timeout=timeout).get("data",[])
        except Exception as e:
            if _HELLO_MSG in str(e): _LCD_EP_SUPPORTED[ep]=False; print(f"[note] disabling {ep}"); data=[]
            else: print(f"[DEBUG]   -> {ep} error: {e}"); data=[]
        results[ep]=data
    return results,{"lcd_id":lid,"lcd_display_id":disp,"document_version":ver}
