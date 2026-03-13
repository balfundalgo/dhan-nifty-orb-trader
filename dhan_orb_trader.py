import os
import sys
import time
import json
import signal
import struct
import threading
from datetime import datetime, timezone, timedelta
from collections import deque
from typing import Optional, Dict, Any, List, Tuple
import csv
import traceback

import requests
import websocket


def _load_dotenv_fallback(path: str = '.env') -> None:
    try:
        with open(path, 'r', encoding='utf-8') as f:
            for raw in f:
                line = raw.strip()
                if not line or line.startswith('#') or '=' not in line:
                    continue
                k, v = line.split('=', 1)
                k = k.strip()
                v = v.strip().strip('"').strip("'")
                if k and k not in os.environ:
                    os.environ[k] = v
    except Exception:
        return


_load_dotenv_fallback()

DHAN_CLIENT_ID = os.getenv('DHAN_CLIENT_ID', '').strip()
DHAN_ACCESS_TOKEN = os.getenv('DHAN_ACCESS_TOKEN', '').strip()
if not DHAN_CLIENT_ID or not DHAN_ACCESS_TOKEN:
    raise SystemExit('Missing DHAN_CLIENT_ID / DHAN_ACCESS_TOKEN in .env')

WS_URL = (
    f"wss://api-feed.dhan.co?version=2"
    f"&token={DHAN_ACCESS_TOKEN}&clientId={DHAN_CLIENT_ID}&authType=2"
)

# ---------------- Config ----------------
# TRADE_MODE options:
#   Single : NIFTY | BANKNIFTY | SENSEX | DIXON | KAYNES | BAJAJ_AUTO | MARUTI |
#            EICHERMOT | HEROMOTOCO | BSE | MCX | ADANIENT | LTIM | PERSISTENT |
#            OFSS | INDIGO | TVSMOTOR | ULTRACEMCO | BRITANNIA | APOLLOHOSP | RELIANCE
#   Groups : ALL_INDEX  (NIFTY + BANKNIFTY + SENSEX)
#            ALL_STOCKS (all 18 stocks)
#            ALL        (ALL_INDEX + ALL_STOCKS)
TRADE_MODE = os.getenv('TRADE_MODE', 'NIFTY').strip().upper()
ST_ATR_LEN = 10
ST_FACTOR = 3.0
BOOTSTRAP_HISTORY = True
BOOTSTRAP_LOOKBACK_DAYS = 10
BOOTSTRAP_CANDLES_1M = 3000
BOOTSTRAP_DEBUG = True
REFRESH_EVERY_SEC = 1.0
SHOW_3M_HISTORY = 8
SHOW_TRADE_LOG = 8
OPTION_REFRESH_SEC = 3.5
OPTION_CHAIN_URL = 'https://api.dhan.co/v2/optionchain'
OPTION_EXPIRY_URL = 'https://api.dhan.co/v2/optionchain/expirylist'
INSTRUMENT_MASTER_URL = 'https://images.dhan.co/api-data/api-scrip-master-detailed.csv'

REQ_SUB_TICKER = 15
RESP_TICKER = 2
RESP_PREV_CLOSE = 6
RESP_DISCONNECT = 50

INSTRUMENTS = {
    # ── Indices ──────────────────────────────────────────────────────────────
    'NIFTY': {
        'key': 'NIFTY', 'name': 'NIFTY 50',
        'exchange': 'IDX_I', 'security_id': '13',
        'instrument_type': 'INDEX',
        'display_prec': 2, 'strike_step': 50, 'option_prefix': 'NIFTY',
        'fno_exchange': 'NSE_FNO',
        'default_lot_size': int(os.getenv('NIFTY_LOT_SIZE', '75')),
        'lots': int(os.getenv('NIFTY_LOTS', '1')),
    },
    'BANKNIFTY': {
        'key': 'BANKNIFTY', 'name': 'BANKNIFTY',
        'exchange': 'IDX_I', 'security_id': '25',
        'instrument_type': 'INDEX',
        'display_prec': 2, 'strike_step': 100, 'option_prefix': 'BANKNIFTY',
        'fno_exchange': 'NSE_FNO',
        'default_lot_size': int(os.getenv('BANKNIFTY_LOT_SIZE', '35')),
        'lots': int(os.getenv('BANKNIFTY_LOTS', '1')),
    },
    'SENSEX': {
        'key': 'SENSEX', 'name': 'SENSEX',
        'exchange': 'IDX_I', 'security_id': '51',
        'instrument_type': 'INDEX',
        'display_prec': 2, 'strike_step': 100, 'option_prefix': 'SENSEX',
        'fno_exchange': 'BSE_FNO',
        'default_lot_size': int(os.getenv('SENSEX_LOT_SIZE', '20')),
        'lots': int(os.getenv('SENSEX_LOTS', '1')),
    },

    # ── Stocks ───────────────────────────────────────────────────────────────
    # security_id: best-effort NSE token — auto-corrected from scrip master at startup.
    # IDs marked '0' are newer listings; runtime resolver MUST succeed for them.
    # strike_step: approximate; ATM resolver picks nearest chain strike regardless.
    'DIXON': {
        'key': 'DIXON', 'name': 'DIXON TECH', 'nse_symbol': 'DIXON',
        'exchange': 'NSE_EQ', 'security_id': '21690',
        'instrument_type': 'EQUITY',
        'display_prec': 2, 'strike_step': 500, 'option_prefix': 'DIXON',
        'fno_exchange': 'NSE_FNO',
        'default_lot_size': int(os.getenv('DIXON_LOT_SIZE', '50')),
        'lots': int(os.getenv('DIXON_LOTS', '1')),
    },
    'KAYNES': {
        'key': 'KAYNES', 'name': 'KAYNES TECH', 'nse_symbol': 'KAYNES',
        'exchange': 'NSE_EQ', 'security_id': '12092',
        'instrument_type': 'EQUITY',
        'display_prec': 2, 'strike_step': 100, 'option_prefix': 'KAYNES',
        'fno_exchange': 'NSE_FNO',
        'default_lot_size': int(os.getenv('KAYNES_LOT_SIZE', '100')),
        'lots': int(os.getenv('KAYNES_LOTS', '1')),
    },
    'BAJAJ_AUTO': {
        'key': 'BAJAJ_AUTO', 'name': 'BAJAJ AUTO', 'nse_symbol': 'BAJAJ-AUTO',
        'exchange': 'NSE_EQ', 'security_id': '16669',
        'instrument_type': 'EQUITY',
        'display_prec': 2, 'strike_step': 250, 'option_prefix': 'BAJAJ-AUTO',
        'fno_exchange': 'NSE_FNO',
        'default_lot_size': int(os.getenv('BAJAJ_AUTO_LOT_SIZE', '75')),
        'lots': int(os.getenv('BAJAJ_AUTO_LOTS', '1')),
    },
    'MARUTI': {
        'key': 'MARUTI', 'name': 'MARUTI SUZUKI', 'nse_symbol': 'MARUTI',
        'exchange': 'NSE_EQ', 'security_id': '10999',
        'instrument_type': 'EQUITY',
        'display_prec': 2, 'strike_step': 500, 'option_prefix': 'MARUTI',
        'fno_exchange': 'NSE_FNO',
        'default_lot_size': int(os.getenv('MARUTI_LOT_SIZE', '50')),
        'lots': int(os.getenv('MARUTI_LOTS', '1')),
    },
    'EICHERMOT': {
        'key': 'EICHERMOT', 'name': 'EICHER MOTORS', 'nse_symbol': 'EICHERMOT',
        'exchange': 'NSE_EQ', 'security_id': '910',
        'instrument_type': 'EQUITY',
        'display_prec': 2, 'strike_step': 100, 'option_prefix': 'EICHERMOT',
        'fno_exchange': 'NSE_FNO',
        'default_lot_size': int(os.getenv('EICHERMOT_LOT_SIZE', '100')),
        'lots': int(os.getenv('EICHERMOT_LOTS', '1')),
    },
    'HEROMOTOCO': {
        'key': 'HEROMOTOCO', 'name': 'HERO MOTOCORP', 'nse_symbol': 'HEROMOTOCO',
        'exchange': 'NSE_EQ', 'security_id': '1348',
        'instrument_type': 'EQUITY',
        'display_prec': 2, 'strike_step': 100, 'option_prefix': 'HEROMOTOCO',
        'fno_exchange': 'NSE_FNO',
        'default_lot_size': int(os.getenv('HEROMOTOCO_LOT_SIZE', '150')),
        'lots': int(os.getenv('HEROMOTOCO_LOTS', '1')),
    },
    'BSE': {
        'key': 'BSE', 'name': 'BSE LTD', 'nse_symbol': 'BSE',
        'exchange': 'NSE_EQ', 'security_id': '19585',
        'instrument_type': 'EQUITY',
        'display_prec': 2, 'strike_step': 200, 'option_prefix': 'BSE',
        'fno_exchange': 'NSE_FNO',
        'default_lot_size': int(os.getenv('BSE_LOT_SIZE', '375')),
        'lots': int(os.getenv('BSE_LOTS', '1')),
    },
    'MCX': {
        'key': 'MCX', 'name': 'MCX INDIA', 'nse_symbol': 'MCX',
        'exchange': 'NSE_EQ', 'security_id': '31181',
        'instrument_type': 'EQUITY',
        'display_prec': 2, 'strike_step': 200, 'option_prefix': 'MCX',
        'fno_exchange': 'NSE_FNO',
        'default_lot_size': int(os.getenv('MCX_LOT_SIZE', '625')),
        'lots': int(os.getenv('MCX_LOTS', '1')),
    },
    'ADANIENT': {
        'key': 'ADANIENT', 'name': 'ADANI ENT', 'nse_symbol': 'ADANIENT',
        'exchange': 'NSE_EQ', 'security_id': '25',
        'instrument_type': 'EQUITY',
        'display_prec': 2, 'strike_step': 50, 'option_prefix': 'ADANIENT',
        'fno_exchange': 'NSE_FNO',
        'default_lot_size': int(os.getenv('ADANIENT_LOT_SIZE', '275')),
        'lots': int(os.getenv('ADANIENT_LOTS', '1')),
    },
    'LTIM': {
        'key': 'LTIM', 'name': 'LTIMindtree', 'nse_symbol': 'LTM',
        'exchange': 'NSE_EQ', 'security_id': '17818',
        'instrument_type': 'EQUITY',
        'display_prec': 2, 'strike_step': 100, 'option_prefix': 'LTIM',
        'fno_exchange': 'NSE_FNO',
        'default_lot_size': int(os.getenv('LTIM_LOT_SIZE', '75')),
        'lots': int(os.getenv('LTIM_LOTS', '1')),
    },
    'PERSISTENT': {
        'key': 'PERSISTENT', 'name': 'PERSISTENT SYS', 'nse_symbol': 'PERSISTENT',
        'exchange': 'NSE_EQ', 'security_id': '18365',
        'instrument_type': 'EQUITY',
        'display_prec': 2, 'strike_step': 100, 'option_prefix': 'PERSISTENT',
        'fno_exchange': 'NSE_FNO',
        'default_lot_size': int(os.getenv('PERSISTENT_LOT_SIZE', '100')),
        'lots': int(os.getenv('PERSISTENT_LOTS', '1')),
    },
    'OFSS': {
        'key': 'OFSS', 'name': 'ORACLE FIN SERV', 'nse_symbol': 'OFSS',
        'exchange': 'NSE_EQ', 'security_id': '10738',
        'instrument_type': 'EQUITY',
        'display_prec': 2, 'strike_step': 200, 'option_prefix': 'OFSS',
        'fno_exchange': 'NSE_FNO',
        'default_lot_size': int(os.getenv('OFSS_LOT_SIZE', '75')),
        'lots': int(os.getenv('OFSS_LOTS', '1')),
    },
    'INDIGO': {
        'key': 'INDIGO', 'name': 'INDIGO (INTERGLOBE)', 'nse_symbol': 'INDIGO',
        'exchange': 'NSE_EQ', 'security_id': '11195',
        'instrument_type': 'EQUITY',
        'display_prec': 2, 'strike_step': 100, 'option_prefix': 'INDIGO',
        'fno_exchange': 'NSE_FNO',
        'default_lot_size': int(os.getenv('INDIGO_LOT_SIZE', '150')),
        'lots': int(os.getenv('INDIGO_LOTS', '1')),
    },
    'TVSMOTOR': {
        'key': 'TVSMOTOR', 'name': 'TVS MOTOR', 'nse_symbol': 'TVSMOTOR',
        'exchange': 'NSE_EQ', 'security_id': '8479',
        'instrument_type': 'EQUITY',
        'display_prec': 2, 'strike_step': 50, 'option_prefix': 'TVSMOTOR',
        'fno_exchange': 'NSE_FNO',
        'default_lot_size': int(os.getenv('TVSMOTOR_LOT_SIZE', '175')),
        'lots': int(os.getenv('TVSMOTOR_LOTS', '1')),
    },
    'ULTRACEMCO': {
        'key': 'ULTRACEMCO', 'name': 'ULTRATECH CEMENT', 'nse_symbol': 'ULTRACEMCO',
        'exchange': 'NSE_EQ', 'security_id': '11532',
        'instrument_type': 'EQUITY',
        'display_prec': 2, 'strike_step': 500, 'option_prefix': 'ULTRACEMCO',
        'fno_exchange': 'NSE_FNO',
        'default_lot_size': int(os.getenv('ULTRACEMCO_LOT_SIZE', '50')),
        'lots': int(os.getenv('ULTRACEMCO_LOTS', '1')),
    },
    'BRITANNIA': {
        'key': 'BRITANNIA', 'name': 'BRITANNIA', 'nse_symbol': 'BRITANNIA',
        'exchange': 'NSE_EQ', 'security_id': '547',
        'instrument_type': 'EQUITY',
        'display_prec': 2, 'strike_step': 100, 'option_prefix': 'BRITANNIA',
        'fno_exchange': 'NSE_FNO',
        'default_lot_size': int(os.getenv('BRITANNIA_LOT_SIZE', '125')),
        'lots': int(os.getenv('BRITANNIA_LOTS', '1')),
    },
    'APOLLOHOSP': {
        'key': 'APOLLOHOSP', 'name': 'APOLLO HOSPITAL', 'nse_symbol': 'APOLLOHOSP',
        'exchange': 'NSE_EQ', 'security_id': '157',
        'instrument_type': 'EQUITY',
        'display_prec': 2, 'strike_step': 100, 'option_prefix': 'APOLLOHOSP',
        'fno_exchange': 'NSE_FNO',
        'default_lot_size': int(os.getenv('APOLLOHOSP_LOT_SIZE', '125')),
        'lots': int(os.getenv('APOLLOHOSP_LOTS', '1')),
    },
    'RELIANCE': {
        'key': 'RELIANCE', 'name': 'RELIANCE', 'nse_symbol': 'RELIANCE',
        'exchange': 'NSE_EQ', 'security_id': '2885',
        'instrument_type': 'EQUITY',
        'display_prec': 2, 'strike_step': 20, 'option_prefix': 'RELIANCE',
        'fno_exchange': 'NSE_FNO',
        'default_lot_size': int(os.getenv('RELIANCE_LOT_SIZE', '500')),
        'lots': int(os.getenv('RELIANCE_LOTS', '1')),
    },
}

# Instrument groupings for TRADE_MODE
TRADE_MODE_GROUPS: Dict[str, List[str]] = {
    'ALL_INDEX':  ['NIFTY', 'BANKNIFTY', 'SENSEX'],
    'ALL_STOCKS': ['DIXON', 'KAYNES', 'BAJAJ_AUTO', 'MARUTI', 'EICHERMOT',
                   'HEROMOTOCO', 'BSE', 'MCX', 'ADANIENT', 'LTIM',
                   'PERSISTENT', 'OFSS', 'INDIGO', 'TVSMOTOR', 'ULTRACEMCO',
                   'BRITANNIA', 'APOLLOHOSP', 'RELIANCE'],
}
TRADE_MODE_GROUPS['ALL'] = TRADE_MODE_GROUPS['ALL_INDEX'] + TRADE_MODE_GROUPS['ALL_STOCKS']


# Dhan WebSocket exchange segment byte → exchange string mapping
_EXCH_SEG_MAP: Dict[int, str] = {
    0: 'IDX_I',    # Indices
    1: 'NSE_EQ',   # NSE Cash Equity
    2: 'NSE_FNO',  # NSE F&O
    3: 'BSE_EQ',   # BSE Cash Equity
    4: 'BSE_FNO',  # BSE F&O
    5: 'NSE_CUR',  # NSE Currency
    7: 'MCX_COM',  # MCX Commodity
}

def _engine_key(security_id: str, exchange: str) -> str:
    """Composite key to avoid collision where same security_id exists in two segments (e.g. ADANIENT and BANKNIFTY both = 25)."""
    return f"{exchange}:{security_id}"

# ANSI
RESET = "\033[0m"
BOLD = "\033[1m"
GREEN = "\033[92m"
RED = "\033[91m"
YELLOW = "\033[93m"
WHITE = "\033[97m"
CYAN = "\033[96m"
DIM = "\033[2m"


# ---------------- time helpers ----------------
def _normalize_dhan_epoch(ts: int) -> int:
    ts = int(ts)
    now_ts = int(time.time())
    diff = ts - now_ts
    if int(4.5 * 3600) <= diff <= int(6.5 * 3600):
        ts -= 19800
    return ts


def epoch_to_local_dt(ts: int) -> datetime:
    ts = _normalize_dhan_epoch(int(ts))
    return datetime.fromtimestamp(ts, tz=timezone.utc).astimezone()


def epoch_to_local_str(ts: Optional[int], with_seconds: bool = True) -> str:
    if not ts:
        return '-'
    dt = epoch_to_local_dt(int(ts))
    return dt.strftime('%H:%M:%S' if with_seconds else '%H:%M')


def minute_bucket_epoch(epoch_sec: int) -> int:
    epoch_sec = _normalize_dhan_epoch(int(epoch_sec))
    return epoch_sec - (epoch_sec % 60)


def bucket_3m_epoch(epoch_sec: int) -> int:
    epoch_sec = _normalize_dhan_epoch(int(epoch_sec))
    return epoch_sec - (epoch_sec % 180)


def is_market_time(dt: datetime) -> bool:
    hm = dt.hour * 60 + dt.minute
    return 9 * 60 + 15 <= hm <= 15 * 60 + 30


def now_local() -> datetime:
    return datetime.now().astimezone()


def current_market_phase() -> str:
    dt = now_local()
    hm = dt.hour * 60 + dt.minute
    if hm < 9 * 60 + 15:
        return 'PREOPEN'
    if hm < 9 * 60 + 24:
        return 'ORB_WAIT'
    if hm < 10 * 60:
        return 'PRE10'
    if hm <= 15 * 60 + 30:
        return 'POST10'
    return 'POSTMARKET'


def _api_headers() -> Dict[str, str]:
    return {
        'Content-Type': 'application/json',
        'access-token': DHAN_ACCESS_TOKEN,
        'client-id': DHAN_CLIENT_ID,
    }


_lot_size_cache: Dict[str, int] = {}
_lot_size_cache_lock = threading.Lock()
_master_loaded = False


def resolve_lot_size_from_master(security_id: str, fallback: int) -> int:
    """Best-effort lot-size lookup.

    MUST be non-throwing and MUST always return an int.
    If master isn't loaded yet (or security id not found), return fallback.
    """
    sec = str(security_id)
    with _lot_size_cache_lock:
        v = _lot_size_cache.get(sec)
        if isinstance(v, int) and v > 0:
            return v
    return int(fallback)

def preload_instrument_master() -> None:
    """Preload Dhan instrument master once to improve lot-size accuracy and reduce delays.

    Safe: never raises. Populates _lot_size_cache asynchronously.
    """
    global _master_loaded
    try:
        r = requests.get(INSTRUMENT_MASTER_URL, timeout=25)
        if r.status_code != 200:
            return
        lines = r.text.splitlines()
        reader = csv.DictReader(lines)
        local: Dict[str, int] = {}
        for row in reader:
            sid = str(row.get('SECURITY_ID') or '').strip()
            if not sid:
                continue
            lot_raw = (
                row.get('LOT_SIZE')
                or row.get('SEM_LOT_UNITS')
                or row.get('LOT_UNITS')
                or row.get('LOT')
                or row.get('LotSize')
                or row.get('LOT_SIZE/CONTRACT')
                or ''
            )
            try:
                v = int(float(str(lot_raw).strip()))
                if v <= 0 or v > 50000:
                    continue
                local[sid] = v
            except Exception:
                continue
        with _lot_size_cache_lock:
            _lot_size_cache.update(local)
            _master_loaded = True
    except Exception:
        return

def resolve_stock_security_ids_from_master() -> None:
    """Look up correct security_id for each stock instrument from Dhan scrip master.

    Updates INSTRUMENTS in-place. Safe: never raises. Called once at bootstrap.
    Stocks with nse_symbol defined are matched against NSE_EQ rows in the master CSV.
    Instruments whose ID is '0' MUST be resolved here or they will fail to subscribe.
    """
    try:
        r = requests.get(INSTRUMENT_MASTER_URL, timeout=25)
        if r.status_code != 200:
            print(f'[SECID] scrip master fetch failed: {r.status_code}')
            return
        lines = r.text.splitlines()
        reader = csv.DictReader(lines)
        # Build symbol→secid map for NSE equity rows (EXCH_ID=NSE, SEGMENT=E, INSTRUMENT=EQUITY)
        # Symbol is in UNDERLYING_SYMBOL column (short trading code e.g. RELIANCE, DIXON)
        sym_map: Dict[str, str] = {}
        for row in reader:
            if (row.get('EXCH_ID','').strip().upper() != 'NSE'
                    or row.get('SEGMENT','').strip().upper() != 'E'
                    or row.get('INSTRUMENT','').strip().upper() != 'EQUITY'):
                continue
            sym = str(row.get('UNDERLYING_SYMBOL') or '').strip().upper()
            sid = str(row.get('SECURITY_ID') or '').strip()
            if sym and sid and sid != '0':
                sym_map[sym] = sid
        resolved = 0
        for key, inst in INSTRUMENTS.items():
            if inst.get('instrument_type') != 'EQUITY':
                continue
            nse_sym = str(inst.get('nse_symbol', '')).strip().upper()
            if not nse_sym:
                continue
            sid = sym_map.get(nse_sym)
            if sid:
                inst['security_id'] = sid
                resolved += 1
            else:
                print(f'[SECID] WARNING: could not resolve security_id for {key} ({nse_sym}) — using fallback {inst["security_id"]}')
        print(f'[SECID] Resolved {resolved} stock security IDs from scrip master.')
    except Exception as e:
        print(f'[SECID] Error resolving stock security IDs: {e}')


def safe_int(val: Any, default: int = 0) -> int:
    """Convert val to int safely; returns default for None/blank/invalid."""
    try:
        if val is None:
            return int(default)
        if isinstance(val, bool):
            return int(val)
        s = str(val).strip()
        if s == '' or s.lower() == 'none':
            return int(default)
        return int(float(s))
    except Exception:
        return int(default)


def fetch_option_expiry(underlying_scrip: str, underlying_seg: str) -> Optional[str]:
    payload = {'UnderlyingScrip': safe_int(underlying_scrip, 0), 'UnderlyingSeg': str(underlying_seg)}
    try:
        r = requests.post(OPTION_EXPIRY_URL, headers=_api_headers(), json=payload, timeout=15)
        if r.status_code != 200:
            return None
        data = r.json()
    except Exception:
        return None

    candidates = []
    raw = data.get('data', data)
    if isinstance(raw, dict):
        for key in ('expiries', 'expiry', 'list', 'data'):
            val = raw.get(key)
            if isinstance(val, list):
                candidates.extend(str(x) for x in val if x)
    elif isinstance(raw, list):
        candidates.extend(str(x) for x in raw if x)
    for c in candidates:
        if c and c.lower() != 'none':
            return c
    return None


def fetch_option_chain(underlying_scrip: str, underlying_seg: str, expiry: str) -> Optional[Dict[str, Any]]:
    payload = {'UnderlyingScrip': safe_int(underlying_scrip, 0), 'UnderlyingSeg': str(underlying_seg), 'Expiry': str(expiry)}
    try:
        r = requests.post(OPTION_CHAIN_URL, headers=_api_headers(), json=payload, timeout=20)
        if r.status_code != 200:
            # Return minimal diagnostics for caller (rate-limit handling etc.)
            body_snip = None
            try:
                body_snip = r.text[:300]
            except Exception:
                body_snip = None
            return {'_status_code': r.status_code, '_body': body_snip, '_payload': payload}
        data = r.json()
    except Exception:
        return None
    raw = data.get('data', data)
    if not isinstance(raw, dict):
        return None
    oc = raw.get('oc') or {}
    if not isinstance(oc, dict):
        return None
    out = {'last_price': raw.get('last_price'), 'oc': oc, 'status': raw.get('status')}
    return out


# ---------------- data fetch ----------------
def fetch_intraday_1m_history(security_id: str, exchange_segment: str, lookback_days: int, limit: int, instrument_type: str = 'INDEX') -> List[Dict[str, Any]]:
    url = 'https://api.dhan.co/v2/charts/intraday'
    now = now_local()
    from_dt = now - timedelta(days=max(1, int(lookback_days)))
    headers = {
        'Content-Type': 'application/json',
        'access-token': DHAN_ACCESS_TOKEN,
        'client-id': DHAN_CLIENT_ID,
    }
    payload = {
        'securityId': str(security_id),
        'exchangeSegment': str(exchange_segment),
        'instrument': str(instrument_type),   # 'INDEX' for indices, 'EQUITY' for stocks
        'expiryCode': 0,
        'oi': False,
        'interval': '1',
        'fromDate': from_dt.strftime('%Y-%m-%d %H:%M:%S'),
        'toDate': now.strftime('%Y-%m-%d %H:%M:%S'),
    }
    try:
        r = requests.post(url, headers=headers, json=payload, timeout=20)
        print(f"[HIST] {exchange_segment} {security_id} status={r.status_code}")
        if r.status_code != 200:
            if BOOTSTRAP_DEBUG:
                print('[HIST] body:', (r.text or '')[:800])
            return []
        data = r.json()
    except Exception as e:
        if BOOTSTRAP_DEBUG:
            print(f'[HIST] error: {e}')
        return []

    ts = data.get('timestamp') or data.get('timestamps') or data.get('t') or []
    o = data.get('open') or data.get('o') or []
    h = data.get('high') or data.get('h') or []
    l = data.get('low') or data.get('l') or []
    c = data.get('close') or data.get('c') or []
    v = data.get('volume') or data.get('v') or []
    n = min(len(ts), len(o), len(h), len(l), len(c))
    out = []
    for i in range(n):
        out.append({
            'time': int(ts[i]),
            'open': float(o[i]),
            'high': float(h[i]),
            'low': float(l[i]),
            'close': float(c[i]),
            'volume': float(v[i]) if i < len(v) and v[i] is not None else 0.0,
        })
    if limit and len(out) > int(limit):
        out = out[-int(limit):]
    return out


# ---------------- supertrend ----------------
class SupertrendState:
    def __init__(self, atr_len: int, factor: float):
        self.atr_len = int(atr_len)
        self.factor = float(factor)
        self.prev_close: Optional[float] = None
        self.tr_q = deque(maxlen=max(1, int(atr_len)))
        self.atr: Optional[float] = None
        self.fub: Optional[float] = None
        self.flb: Optional[float] = None
        self.value: Optional[float] = None
        self.dir: Optional[int] = None
        self.last_signal: Optional[str] = None

    def reset(self) -> None:
        self.__init__(self.atr_len, self.factor)

    def update(self, o: float, h: float, l: float, c: float) -> None:
        if self.prev_close is None:
            tr = float(h) - float(l)
        else:
            pc = float(self.prev_close)
            tr = max(float(h) - float(l), abs(float(h) - pc), abs(float(l) - pc))

        self.tr_q.append(float(tr))
        n = self.atr_len
        if self.atr is None:
            if len(self.tr_q) < n:
                self.prev_close = float(c)
                self.last_signal = None
                return
            self.atr = sum(self.tr_q) / float(n)
        else:
            alpha = 1.0 / float(n)
            self.atr = alpha * float(tr) + (1.0 - alpha) * float(self.atr)

        atr = float(self.atr)
        hl2 = (float(h) + float(l)) / 2.0
        basic_upper = hl2 + self.factor * atr
        basic_lower = hl2 - self.factor * atr

        prev_upper = basic_upper if self.fub is None else float(self.fub)
        prev_lower = basic_lower if self.flb is None else float(self.flb)
        prev_close = float(c) if self.prev_close is None else float(self.prev_close)

        upper = basic_upper if (basic_upper < prev_upper or prev_close > prev_upper) else prev_upper
        lower = basic_lower if (basic_lower > prev_lower or prev_close < prev_lower) else prev_lower

        signal = None
        if self.dir is None:
            direction = 1
            st_val = lower
        else:
            if int(self.dir) == 1:
                if float(c) < lower:
                    direction = -1
                    st_val = upper
                    signal = 'DOWN'
                else:
                    direction = 1
                    st_val = lower
            else:
                if float(c) > upper:
                    direction = 1
                    st_val = lower
                    signal = 'UP'
                else:
                    direction = -1
                    st_val = upper

        self.fub = float(upper)
        self.flb = float(lower)
        self.value = float(st_val)
        self.dir = int(direction)
        self.last_signal = signal
        self.prev_close = float(c)


# ---------------- instrument engine ----------------
class IndexPaperEngine:
    def __init__(self, instrument: Dict[str, Any]):
        self.instrument = instrument
        self.lock = threading.Lock()
        self.prev_close: Optional[float] = None
        self.last_ltp: Optional[float] = None
        self.last_ltt_epoch: Optional[int] = None
        self.last_tick_seen_epoch: Optional[int] = None

        self.current_1m: Optional[Dict[str, Any]] = None
        self.completed_1m = deque(maxlen=5000)
        self.current_3m: Optional[Dict[str, Any]] = None
        self.completed_3m = deque(maxlen=500)

        self.st = SupertrendState(ST_ATR_LEN, ST_FACTOR)

        self.current_session_date: Optional[str] = None
        self.orb_high: Optional[float] = None
        self.orb_low: Optional[float] = None
        self.orb_ready: bool = False
        self.orb_bars_count: int = 0

        self.position: Optional[str] = None  # CE / PE
        self.entry_price: Optional[float] = None  # index reference at entry
        self.entry_time: Optional[int] = None
        self.entry_strike: Optional[int] = None

        # Option-paper-trading state
        self.entry_option_price: Optional[float] = None
        self.current_option_price: Optional[float] = None
        self.entry_option_security_id: Optional[int] = None
        self.entry_lot_size: Optional[int] = None
        self.entry_expiry: Optional[str] = None
        self.last_option_tick_wc: float = 0.0   # wall-clock time of last WS option tick (time.time())
        self._pending_option_sub_req: Optional[Dict[str, str]] = None  # consumed by App.process_option_subscriptions
        self.option_chain_expiry: Optional[str] = None
        self.option_chain_last_fetch: float = 0.0
        self.option_chain_cooldown_until: float = 0.0  # set when 429 rate-limit happens
        self.option_chain_min_interval: float = 3.2  # seconds (Dhan recommends >=3s)
        self.option_chain_underlying: Optional[float] = None
        self.option_chain_map: Dict[Tuple[int, str], Dict[str, Any]] = {}
        self.entry_lots: int = safe_int(self.instrument.get('lots'), 1)

        self.realized_pnl: float = 0.0  # option premium points
        self.realized_pnl_rupees: float = 0.0
        self.trade_log = deque(maxlen=40)
        self.last_strategy_note: str = 'waiting for market to open'

    def _today_key(self) -> str:
        return now_local().strftime('%Y-%m-%d')

    def _atm_strike(self, price: float) -> int:
        """Return nearest ATM strike for the underlying price.

        Robust against missing/None strike_step; falls back to sensible defaults.
        """
        step_raw = self.instrument.get('strike_step')
        step = None
        try:
            if step_raw is not None and str(step_raw).strip() != '':
                step = int(float(step_raw))
        except Exception:
            step = None

        if not step or step <= 0:
            # Fallbacks by instrument type/name
            name = str(self.instrument.get('name', '')).upper()
            if 'BANK' in name:
                step = 100
            elif 'SENSEX' in name:
                step = 100
            else:
                step = 50  # NIFTY default
        return int(round(float(price) / float(step)) * step)

    def update_prev_close(self, prev_close: float):
        with self.lock:
            self.prev_close = float(prev_close)

    def seed_from_history(self, candles_1m: List[Dict[str, Any]]) -> None:
        if not candles_1m:
            return
        with self.lock:
            self.current_1m = None
            self.completed_1m.clear()
            self.current_3m = None
            self.completed_3m.clear()
            self.st.reset()
            self.current_session_date = None
            self.orb_high = None
            self.orb_low = None
            self.orb_ready = False
            self.orb_bars_count = 0
            self.position = None
            self.entry_price = None
            self.entry_time = None
            self.entry_strike = None
            self.entry_option_price = None
            self.current_option_price = None
            self.entry_option_security_id = None
            self.entry_lot_size = None
            self.entry_expiry = None
            self.option_chain_expiry = None
            self.option_chain_last_fetch = 0.0
            self.option_chain_cooldown_until: float = 0.0  # set when 429 rate-limit happens
            self.option_chain_min_interval: float = 3.2  # seconds (Dhan recommends >=3s)
            self.option_chain_underlying = None
            self.option_chain_map.clear()
            self.realized_pnl = 0.0
            self.realized_pnl_rupees = 0.0
            self.trade_log.clear()
            self.last_strategy_note = 'waiting for market to open'
            for cd in candles_1m:
                self._ingest_completed_1m_locked(
                    int(cd['time']), float(cd['open']), float(cd['high']), float(cd['low']), float(cd['close']), historical=True
                )
            # Always reset both live-forming bars after bootstrap.
            # current_3m must be wiped so live counting starts clean from the
            # next full 3m bucket — never carry a history-fed partial candle
            # into live tick counting (it would fire 1 candle too early).
            self.current_1m = None
            self.current_3m = None
            # if history belonged to a prior day, also reset ORB state
            if self.current_session_date != self._today_key():
                self.current_session_date = None
                self.orb_high = None
                self.orb_low = None
                self.orb_ready = False
                self.orb_bars_count = 0
                self.last_strategy_note = 'waiting for market to open'

    def on_tick(self, ltp: float, ltt_epoch: int):
        ltp = float(ltp)
        ltt_epoch = _normalize_dhan_epoch(int(ltt_epoch))
        bucket = minute_bucket_epoch(ltt_epoch)
        with self.lock:
            self.last_ltp = ltp
            self.last_ltt_epoch = ltt_epoch
            self.last_tick_seen_epoch = int(time.time())
            if self.current_1m is None:
                self.current_1m = {'bucket': bucket, 'open': ltp, 'high': ltp, 'low': ltp, 'close': ltp, 'tick_count': 1}
                return
            cur_bucket = int(self.current_1m['bucket'])
            if bucket == cur_bucket:
                self.current_1m['high'] = max(float(self.current_1m['high']), ltp)
                self.current_1m['low'] = min(float(self.current_1m['low']), ltp)
                self.current_1m['close'] = ltp
                self.current_1m['tick_count'] = int(self.current_1m.get('tick_count', 0)) + 1
                return
            if bucket > cur_bucket:
                co = float(self.current_1m['open'])
                ch = float(self.current_1m['high'])
                cl = float(self.current_1m['low'])
                cc = float(self.current_1m['close'])
                self._ingest_completed_1m_locked(cur_bucket, co, ch, cl, cc, historical=False)
                self.current_1m = {'bucket': bucket, 'open': ltp, 'high': ltp, 'low': ltp, 'close': ltp, 'tick_count': 1}

    def on_option_tick(self, option_sec: str, premium: float, ltt_epoch: int):
        premium = float(premium)
        with self.lock:
            if self.entry_option_security_id and str(self.entry_option_security_id) == str(option_sec):
                self.current_option_price = premium
                self.last_option_tick_wc = time.time()  # wall-clock, safe to compare with time.time() in ui_loop

    def pop_option_subscribe_request(self) -> Optional[Dict[str,str]]:
        with self.lock:
            req = getattr(self, '_pending_option_sub_req', None)
            self._pending_option_sub_req = None
            return req

    def _ingest_completed_1m_locked(self, bucket: int, o: float, h: float, l: float, c: float, historical: bool):
        self.completed_1m.append({'bucket': int(bucket), 'open': o, 'high': h, 'low': l, 'close': c})
        b3 = bucket_3m_epoch(bucket)
        if self.current_3m is None:
            # If this 1m is not the first minute of its 3m bucket, we started mid-candle.
            # Skip it entirely and wait for the next clean full 3m candle.
            minute_offset = (int(bucket) - int(b3)) // 60
            if minute_offset > 0:
                return
            self.current_3m = {'bucket': b3, 'open': o, 'high': h, 'low': l, 'close': c, 'parts': 1}
            return
        if b3 == int(self.current_3m['bucket']):
            self.current_3m['high'] = max(float(self.current_3m['high']), h)
            self.current_3m['low'] = min(float(self.current_3m['low']), l)
            self.current_3m['close'] = c
            parts = int(self.current_3m.get('parts', 0)) + 1
            self.current_3m['parts'] = min(parts, 3)
            # Finalize as soon as all 3 parts are in
            if parts >= 3:
                prev = dict(self.current_3m)
                self._finalize_3m_locked(prev, historical=historical)
                self.current_3m = None
            return
        prev = dict(self.current_3m)
        if int(prev.get('parts', 0)) >= 1:
            self._finalize_3m_locked(prev, historical=historical)
        self.current_3m = {'bucket': b3, 'open': o, 'high': h, 'low': l, 'close': c, 'parts': 1}

    def _reset_daily_orb_locked(self, day_key: str):
        self.current_session_date = day_key
        self.orb_high = None
        self.orb_low = None
        self.orb_ready = False
        self.orb_bars_count = 0
        self.last_strategy_note = 'waiting for ORB levels'

    def _finalize_3m_locked(self, bar: Dict[str, Any], historical: bool):
        bucket = int(bar['bucket'])
        dt = epoch_to_local_dt(bucket)
        o = float(bar['open']); h = float(bar['high']); l = float(bar['low']); c = float(bar['close'])

        if not is_market_time(dt):
            self.completed_3m.append({'bucket': bucket, 'open': o, 'high': h, 'low': l, 'close': c})
            self.st.update(o, h, l, c)
            return

        day_key = dt.strftime('%Y-%m-%d')
        if self.current_session_date != day_key:
            self._reset_daily_orb_locked(day_key)

        self.st.update(o, h, l, c)
        self.completed_3m.append({'bucket': bucket, 'open': o, 'high': h, 'low': l, 'close': c})

        hm = dt.hour * 60 + dt.minute
        # New ORB bars: 09:18 and 09:21 start candles only
        if hm in (9 * 60 + 18, 9 * 60 + 21):
            self.orb_high = h if self.orb_high is None else max(float(self.orb_high), h)
            self.orb_low = l if self.orb_low is None else min(float(self.orb_low), l)
            self.orb_bars_count += 1
            if self.orb_bars_count >= 2:
                self.orb_ready = True
                self.last_strategy_note = f'ORB ready H={self.orb_high:.2f} L={self.orb_low:.2f}'
            else:
                self.last_strategy_note = 'waiting for ORB levels'
            return

        if historical:
            return

        # Strategy is evaluated on the *close time* of this 3m candle.
        # `bucket` is the candle start; add 180s so logs/notes align with the real close (e.g., 14:03 candle closes at 14:06).
        close_bucket = int(bucket + 180)
        self._run_strategy_on_closed_3m_locked(close_bucket, o, h, l, c)


    def _refresh_option_chain_locked(self, force: bool = False) -> bool:
        now_ts = time.time()

        # If we are cooling down due to rate limit, don't hammer the API.
        if now_ts < float(self.option_chain_cooldown_until):
            return bool(self.option_chain_map)

        # Hard minimum interval between optionchain calls.
        if self.option_chain_map and (now_ts - float(self.option_chain_last_fetch) < self.option_chain_min_interval):
            return True

        # Softer refresh interval (normal polling)
        if (not force) and self.option_chain_map and (now_ts - float(self.option_chain_last_fetch) < OPTION_REFRESH_SEC):
            return True

        expiry = self.option_chain_expiry
        if force or not expiry:
            expiry = fetch_option_expiry(self.instrument['security_id'], self.instrument['exchange'])
            if not expiry:
                return False
            self.option_chain_expiry = expiry

        chain = fetch_option_chain(self.instrument['security_id'], self.instrument['exchange'], self.option_chain_expiry)
        if isinstance(chain, dict) and chain.get('_status_code'):
            sc = int(chain.get('_status_code'))
            if sc == 429:
                self.option_chain_cooldown_until = time.time() + 8.0
            return False
        if not chain and force:
            fresh = fetch_option_expiry(self.instrument['security_id'], self.instrument['exchange'])
            if fresh:
                self.option_chain_expiry = fresh
                chain = fetch_option_chain(self.instrument['security_id'], self.instrument['exchange'], self.option_chain_expiry)
        if not chain:
            return False

        oc = chain.get('oc') or {}
        parsed: Dict[Tuple[int, str], Dict[str, Any]] = {}
        for strike_key, node in oc.items():
            try:
                strike = int(round(float(strike_key)))
            except Exception:
                continue
            if not isinstance(node, dict):
                continue
            for side in ('CE', 'PE'):
                leg = node.get(side) or node.get(side.lower()) or {}
                if not isinstance(leg, dict):
                    continue
                premium = leg.get('last_price', leg.get('lastPrice', leg.get('ltp')))
                secid = leg.get('security_id', leg.get('securityId', leg.get('sid')))
                if premium is None or secid in (None, ''):
                    continue
                try:
                    prem_val = float(premium)
                    sid_str = str(secid)
                except Exception:
                    continue
                lot_size = resolve_lot_size_from_master(sid_str, safe_int(self.instrument.get('default_lot_size'), 1))
                parsed[(strike, side)] = {
                    'strike': strike,
                    'side': side,
                    'premium': prem_val,
                    'security_id': sid_str,
                    'lot_size': int(lot_size),
                    'expiry': self.option_chain_expiry,
                }

        if not parsed:
            return False

        self.option_chain_map = parsed
        self.option_chain_last_fetch = now_ts
        try:
            self.option_chain_underlying = float(chain.get('last_price')) if chain.get('last_price') is not None else self.last_ltp
        except Exception:
            self.option_chain_underlying = self.last_ltp
        self.option_chain_last_fetch = now_ts
        self.option_chain_cooldown_until: float = 0.0  # set when 429 rate-limit happens
        self.option_chain_min_interval: float = 3.2  # seconds (Dhan recommends >=3s)
        return True

    def _resolve_option_snapshot_locked(self, side: str, price: float, force_refresh: bool = False) -> Optional[Dict[str, Any]]:
        if side not in ('CE', 'PE'):
            return None
        if not self._refresh_option_chain_locked(force=False):
            return None
        desired = self._atm_strike(price)
        candidates = []
        for (strike, s), snap in self.option_chain_map.items():
            if s != side:
                continue
            candidates.append((abs(int(strike) - int(desired)), int(strike), snap))
        if not candidates:
            return None
        candidates.sort(key=lambda x: (x[0], x[1]))
        return dict(candidates[0][2])

    def poll_option_prices(self):
        with self.lock:
            ok = self._refresh_option_chain_locked(force=False)
            if not ok:
                return
            if self.position is None or self.entry_strike is None:
                return
            snap = self.option_chain_map.get((int(self.entry_strike), str(self.position)))
            if snap is None:
                snap = self._resolve_option_snapshot_locked(str(self.position), self.last_ltp or self.entry_price or 0.0, force_refresh=False)
            if snap:
                self.current_option_price = float(snap['premium'])

    def _log_trade_locked(self, action: str, side: str, strike: Optional[int], price: float, bucket: int, note: str):
        stamp = epoch_to_local_str(bucket, with_seconds=False)
        opt_txt = f"{self.instrument['option_prefix']} ATM {side} {strike}" if strike else f"{self.instrument['option_prefix']} ATM {side}"
        prem_txt = '-'
        if action == 'ENTER' and self.entry_option_price is not None:
            prem_txt = f'prem {self.entry_option_price:,.2f}'
        elif action == 'EXIT' and self.current_option_price is not None:
            prem_txt = f'prem {self.current_option_price:,.2f}'
        lot_txt = '-'
        if self.entry_lot_size:
            lot_txt = f'qty {int(self.entry_lot_size) * int(self.entry_lots)}'
        self.trade_log.appendleft(f'{stamp} | {action:<5} | {opt_txt:<26} | idx {price:,.2f} | {prem_txt:<14} | {lot_txt:<8} | {note}')
        self.last_strategy_note = note

    def _exit_locked(self, price: float, bucket: int, reason: str):
        if self.position is None or self.entry_price is None:
            return
        exit_prem = self.current_option_price
        if exit_prem is None:
            snap = self._resolve_option_snapshot_locked(self.position, price, force_refresh=True)
            if snap:
                exit_prem = float(snap['premium'])
                self.current_option_price = exit_prem
        if exit_prem is None:
            exit_prem = self.entry_option_price or 0.0
        entry_prem = self.entry_option_price or 0.0
        prem_pnl = float(exit_prem) - float(entry_prem)
        qty = int((self.entry_lot_size or 1) * max(1, int(self.entry_lots)))
        rupees_pnl = prem_pnl * qty
        side = self.position
        strike = self.entry_strike
        self.realized_pnl += prem_pnl
        self.realized_pnl_rupees += rupees_pnl
        self._log_trade_locked('EXIT', side, strike, price, bucket, f'{reason} | pnl={prem_pnl:.2f} prem | ₹{rupees_pnl:.2f}')
        self.position = None
        self.entry_price = None
        self.entry_time = None
        self.entry_strike = None
        self.entry_option_price = None
        self.current_option_price = None
        self.entry_option_security_id = None
        self.entry_lot_size = None
        self.entry_expiry = None

    def _enter_locked(self, side: str, price: float, bucket: int, reason: str):
        if self.position == side:
            self.last_strategy_note = f'hold {side} | {reason}'
            return
        if self.position is not None:
            self._exit_locked(price, bucket, f'reverse to {side}')
        snap = self._resolve_option_snapshot_locked(side, price, force_refresh=False)
        if not snap:
            self.last_strategy_note = f'option chain unavailable for {side} entry'
            return
        self.position = side
        self.entry_price = float(price)
        self.entry_time = int(bucket)
        self.entry_strike = int(snap['strike'])
        self.entry_option_price = float(snap['premium'])
        self.current_option_price = float(snap['premium'])
        self.entry_option_security_id = str(snap['security_id'])
        self.entry_lot_size = int(snap['lot_size'])
        self.entry_expiry = snap.get('expiry')
        self.last_option_tick_wc = 0.0  # reset so fallback polling activates until first WS tick arrives
        # Queue a WS subscription for this option contract — picked up by App.process_option_subscriptions()
        self._pending_option_sub_req = {
            'security_id': str(snap['security_id']),
            'exchange': self.instrument['fno_exchange'],
        }
        self._log_trade_locked('ENTER', side, self.entry_strike, price, bucket, f"{reason} | {self.instrument['option_prefix']} {side} @ {self.entry_option_price:.2f}")

    def _run_strategy_on_closed_3m_locked(self, bucket: int, o: float, h: float, l: float, c: float):
        dt = epoch_to_local_dt(bucket)
        hm = dt.hour * 60 + dt.minute
        st_val = self.st.value
        st_dir = self.st.dir

        # exits first
        if self.position == 'CE':
            if self.orb_ready and self.orb_low is not None and c <= float(self.orb_low):
                self._exit_locked(c, bucket, 'ORB low SL')
            elif st_val is not None and c < float(st_val):
                self._exit_locked(c, bucket, 'close below ST')
        elif self.position == 'PE':
            if self.orb_ready and self.orb_high is not None and c >= float(self.orb_high):
                self._exit_locked(c, bucket, 'ORB high SL')
            elif st_val is not None and c > float(st_val):
                self._exit_locked(c, bucket, 'close above ST')

        # Before 09:15 no trading; 09:15-09:24 wait for ORB
        if hm < 9 * 60 + 24:
            self.last_strategy_note = 'waiting for ORB levels'
            return

        if hm < 10 * 60:
            if not self.orb_ready:
                self.last_strategy_note = 'waiting for ORB levels'
                return
            if self.orb_high is not None and l > float(self.orb_high):
                self._enter_locked('CE', c, bucket, 'pre-10 ORB up breakout')
                return
            if self.orb_low is not None and h < float(self.orb_low):
                self._enter_locked('PE', c, bucket, 'pre-10 ORB down breakout')
                return
            self.last_strategy_note = 'pre-10 no breakout'
            return

        # 10:00 onward => Supertrend only
        if st_dir is None:
            self.last_strategy_note = 'post-10 waiting ST warmup'
            return
        if int(st_dir) > 0:
            self._enter_locked('CE', c, bucket, 'post-10 ST UP')
        else:
            self._enter_locked('PE', c, bucket, 'post-10 ST DOWN')

    def _effective_orb_state_locked(self):
        if self.current_session_date != self._today_key():
            return None, None, False
        return self.orb_high, self.orb_low, self.orb_ready


    def _next_eval_time_locked(self, phase: str) -> Optional[str]:
        """Return next 3-minute strategy evaluation time (HH:MM) in local time."""
        try:
            if phase == 'POSTMARKET':
                return None
            # Before market open: next evaluation is market open time
            if phase == 'PREOPEN':
                dt = now_local().replace(hour=9, minute=15, second=0, microsecond=0)
                return dt.strftime('%H:%M')
            # If we have a forming 3m candle, next evaluation is at its end
            if self.current_3m and 'bucket' in self.current_3m:
                start_dt = epoch_to_local_dt(int(self.current_3m['bucket']))
                nxt = (start_dt + timedelta(minutes=3)).replace(second=0, microsecond=0)
                return nxt.strftime('%H:%M')
            # Fallback: round current time up to next 3-minute boundary
            base = now_local()
            minute = (base.minute // 3 + 1) * 3
            hour = base.hour
            if minute >= 60:
                minute = 0
                hour = (hour + 1) % 24
            nxt = base.replace(hour=hour, minute=minute, second=0, microsecond=0)
            return nxt.strftime('%H:%M')
        except Exception:
            return None

    def snapshot(self) -> Dict[str, Any]:
        with self.lock:
            unreal = 0.0
            unreal_rupees = 0.0
            qty = int((self.entry_lot_size or 1) * max(1, int(self.entry_lots)))
            if self.position and self.entry_option_price is not None and self.current_option_price is not None:
                unreal = float(self.current_option_price) - float(self.entry_option_price)
                unreal_rupees = unreal * qty
            orb_h, orb_l, orb_ready = self._effective_orb_state_locked()
            note = self.last_strategy_note
            phase = current_market_phase()
            next_eval = self._next_eval_time_locked(phase)
            if phase == 'PREOPEN':
                note = 'waiting for market to open'
            elif phase == 'ORB_WAIT' and not orb_ready:
                note = 'waiting for ORB levels'
            elif phase == 'POSTMARKET':
                note = 'market closed'
            return {
                'instrument': self.instrument,
                'prev_close': self.prev_close,
                'last_ltp': self.last_ltp,
                'last_ltt_epoch': self.last_ltt_epoch,
                'last_tick_seen_epoch': self.last_tick_seen_epoch,
                'current_3m': dict(self.current_3m) if self.current_3m else None,
                'history_3m': list(self.completed_3m)[-SHOW_3M_HISTORY:],
                'st_value': self.st.value,
                'st_dir': self.st.dir,
                'st_signal': self.st.last_signal,
                'orb_high': orb_h,
                'orb_low': orb_l,
                'orb_ready': orb_ready,
                'position': self.position,
                'entry_price': self.entry_price,
                'entry_time': self.entry_time,
                'entry_strike': self.entry_strike,
                'entry_option_price': self.entry_option_price,
                'current_option_price': self.current_option_price,
                'entry_option_security_id': self.entry_option_security_id,
                'entry_lot_size': self.entry_lot_size,
                'entry_lots': self.entry_lots,
                'entry_expiry': self.entry_expiry,
                'realized_pnl': self.realized_pnl,
                'realized_pnl_rupees': self.realized_pnl_rupees,
                'unrealized_pnl': unreal,
                'unrealized_pnl_rupees': unreal_rupees,
                'trade_log': list(self.trade_log),
                'next_eval': next_eval,
                'note': note,
            }


# ---------------- ws parsing ----------------
def parse_header_8(msg: bytes) -> Optional[Dict[str, Any]]:
    if len(msg) < 8:
        return None
    return {
        'resp_code': msg[0],
        'msg_len': struct.unpack_from('<H', msg, 1)[0],
        'exch_seg_num': msg[3],
        'security_id': str(struct.unpack_from('<I', msg, 4)[0]),
        'payload': msg[8:],
    }


def parse_ticker(payload: bytes) -> Optional[Dict[str, Any]]:
    if len(payload) < 8:
        return None
    return {
        'ltp': float(struct.unpack_from('<f', payload, 0)[0]),
        'ltt_epoch': int(struct.unpack_from('<I', payload, 4)[0]),
    }


def parse_prev_close(payload: bytes) -> Optional[Dict[str, Any]]:
    if len(payload) < 8:
        return None
    return {'prev_close': float(struct.unpack_from('<f', payload, 0)[0])}


# ---------------- app ----------------
class App:
    def __init__(self):
        self.selected = self._resolve_selected_instruments()
        self.engines = {_engine_key(inst['security_id'], inst['exchange']): IndexPaperEngine(inst) for inst in self.selected}
        self.stats_lock = threading.Lock()
        self.packet_counts = {'ticker': 0, 'prev_close': 0, 'other': 0, 'disconnect': 0}
        self.last_ws_error: Optional[str] = None
        self.last_ws_connect_time: Optional[int] = None
        self.stop_evt = threading.Event()
        self.ws: Optional[websocket.WebSocketApp] = None
        self.ws_thread: Optional[threading.Thread] = None
        self.ui_thread: Optional[threading.Thread] = None
        self.option_routes: Dict[str, str] = {}  # option_sec_id -> underlying engine sec_id
        self.subscribed_secids: set[str] = set(_engine_key(str(inst['security_id']), inst['exchange']) for inst in self.selected)

    def _resolve_selected_instruments(self) -> List[Dict[str, Any]]:
        if TRADE_MODE in TRADE_MODE_GROUPS:
            keys = TRADE_MODE_GROUPS[TRADE_MODE]
        elif TRADE_MODE in INSTRUMENTS:
            keys = [TRADE_MODE]
        else:
            valid = list(INSTRUMENTS.keys()) + list(TRADE_MODE_GROUPS.keys())
            raise SystemExit(f'TRADE_MODE={TRADE_MODE!r} is invalid. Valid options: {valid}')
        selected = [INSTRUMENTS[k] for k in keys]
        # Warn if any stock has security_id still '0' (resolver failed)
        for inst in selected:
            if inst.get('instrument_type') == 'EQUITY' and str(inst.get('security_id', '0')) == '0':
                print(f"[WARN] {inst['key']}: security_id is 0 — scrip master lookup failed. This instrument will not receive ticks.")
        return selected

    def on_open(self, ws):
        with self.stats_lock:
            self.last_ws_connect_time = int(time.time())
            self.last_ws_error = None
            self.last_ws_error = None
        sub = {
            'RequestCode': REQ_SUB_TICKER,
            'InstrumentCount': len(self.selected),
            'InstrumentList': [
                {'ExchangeSegment': inst['exchange'], 'SecurityId': str(inst['security_id'])}
                for inst in self.selected
            ],
        }
        ws.send(json.dumps(sub))

    def _send_subscribe(self, instruments: List[Dict[str,str]]):
        """Subscribe additional instruments (does not unsubscribe)."""
        if not instruments or self.ws is None:
            return
        msg = {
            'RequestCode': REQ_SUB_TICKER,
            'InstrumentCount': len(instruments),
            'InstrumentList': instruments,
        }
        try:
            self.ws.send(json.dumps(msg))
        except Exception as e:
            with self.stats_lock:
                self.last_ws_error = f'WS subscribe error: {e}'


    def on_message(self, ws, message):
        if not isinstance(message, (bytes, bytearray)):
            return
        head = parse_header_8(message)
        if not head:
            return
        resp_code = int(head['resp_code'])
        sec = str(head['security_id'])
        exch_str = _EXCH_SEG_MAP.get(int(head['exch_seg_num']), '')
        payload = head['payload']
        # Try composite key first (handles collisions like ADANIENT vs BANKNIFTY both = 25)
        engine = self.engines.get(_engine_key(sec, exch_str)) if exch_str else None
        if engine is None:
            engine = self.engines.get(_engine_key(sec, 'IDX_I'))  # fallback for indices
        if engine is None:
            # Option tick routing
            eng_sec = self.option_routes.get(sec)
            if eng_sec is None:
                return
            engine = self.engines.get(eng_sec)
            if engine is None:
                return
            is_option_tick = True
        else:
            is_option_tick = False

        if resp_code == RESP_TICKER:
            t = parse_ticker(payload)
            if t:
                with self.stats_lock:
                    self.packet_counts['ticker'] += 1
                if is_option_tick:
                    engine.on_option_tick(sec, t['ltp'], t['ltt_epoch'])
                else:
                    engine.on_tick(t['ltp'], t['ltt_epoch'])
            return
        if resp_code == RESP_PREV_CLOSE:
            p = parse_prev_close(payload)
            if p:
                with self.stats_lock:
                    self.packet_counts['prev_close'] += 1
                engine.update_prev_close(p['prev_close'])
            return
        if resp_code == RESP_DISCONNECT:
            with self.stats_lock:
                self.packet_counts['disconnect'] += 1
            return
        with self.stats_lock:
            self.packet_counts['other'] += 1

    def on_error(self, ws, error):
        with self.stats_lock:
            self.last_ws_error = str(error)

    def on_close(self, ws, close_status_code, close_msg):
        return

    def bootstrap(self):
        # Single download: resolve stock security IDs AND preload lot-size cache
        # preload_instrument_master must run first so lot sizes are ready before
        # option chain parsing begins (resolve_stock_security_ids shares the download).
        preload_instrument_master()
        resolve_stock_security_ids_from_master()
        # Rebuild engine map now that security IDs may have changed
        self.engines = {_engine_key(inst['security_id'], inst['exchange']): IndexPaperEngine(inst) for inst in self.selected}
        self.subscribed_secids = set(_engine_key(str(inst['security_id']), inst['exchange']) for inst in self.selected)
        if not BOOTSTRAP_HISTORY:
            return
        print('Bootstrapping with recent 1m candles...')
        for inst in self.selected:
            hist = fetch_intraday_1m_history(
                security_id=inst['security_id'],
                exchange_segment=inst['exchange'],
                lookback_days=BOOTSTRAP_LOOKBACK_DAYS,
                limit=BOOTSTRAP_CANDLES_1M,
                instrument_type=inst.get('instrument_type', 'INDEX'),
            )
            if hist:
                self.engines[_engine_key(inst['security_id'], inst['exchange'])].seed_from_history(hist)
                print(f"  {inst['name']}: bootstrapped {len(hist)} x 1m candles.")
            else:
                print(f"  {inst['name']}: history unavailable. Continuing with live warmup.")

    def run_ws_loop(self):
        websocket.enableTrace(False)
        while not self.stop_evt.is_set():
            try:
                self.ws = websocket.WebSocketApp(
                    WS_URL,
                    on_open=self.on_open,
                    on_message=self.on_message,
                    on_error=self.on_error,
                    on_close=self.on_close,
                )
                self.ws.run_forever(ping_interval=20, ping_timeout=10)
            except Exception as e:
                with self.stats_lock:
                    self.last_ws_error = f'WS exception: {e}'
            finally:
                if not self.stop_evt.is_set():
                    time.sleep(2)

    def start(self):
        self.bootstrap()
        self.ws_thread = threading.Thread(target=self.run_ws_loop, daemon=True)
        self.ui_thread = threading.Thread(target=self.ui_loop, daemon=True)
        self.ws_thread.start()
        self.ui_thread.start()
        while not self.stop_evt.is_set():
            time.sleep(0.2)

    def stop(self):
        self.stop_evt.set()
        try:
            if self.ws:
                self.ws.close()
        except Exception:
            pass

    def process_option_subscriptions(self):
        """Subscribe to option security IDs requested by engines."""
        pending: List[Dict[str,str]] = []
        for inst in self.selected:
            eng = self.engines[_engine_key(inst['security_id'], inst['exchange'])]
            req = eng.pop_option_subscribe_request()
            if not req:
                continue
            opt_sec = str(req['security_id'])
            ex = req['exchange']
            if _engine_key(opt_sec, ex) in self.subscribed_secids:
                continue
            pending.append({'ExchangeSegment': ex, 'SecurityId': opt_sec})
            self.subscribed_secids.add(_engine_key(opt_sec, ex))
            self.option_routes[opt_sec] = _engine_key(inst['security_id'], inst['exchange'])
        if pending:
            self._send_subscribe(pending)



# ══════════════════════════════════════════════════════════════════════════════
#  GUI — CustomTkinter Dashboard
# ══════════════════════════════════════════════════════════════════════════════
import tkinter as tk
import tkinter.messagebox as mb
from pathlib import Path

try:
    import customtkinter as ctk
except ImportError:
    import tkinter as _tk
    _tk.Tk().withdraw()
    mb.showerror("Missing Package",
        "customtkinter is not installed.\n\nRun:  pip install customtkinter")
    raise SystemExit(1)

# ── resolve .env path relative to exe ────────────────────────────────────────
if getattr(sys, 'frozen', False):
    BASE_DIR = Path(sys.executable).parent
else:
    BASE_DIR = Path(__file__).parent
ENV_PATH = BASE_DIR / '.env'

def _save_env(**kwargs):
    existing: dict = {}
    try:
        for raw in ENV_PATH.read_text(encoding='utf-8').splitlines():
            line = raw.strip()
            if not line or line.startswith('#') or '=' not in line:
                continue
            k, v = line.split('=', 1)
            existing[k.strip()] = v.strip()
    except Exception:
        pass
    existing.update(kwargs)
    ENV_PATH.write_text('\n'.join(f"{k}={v}" for k, v in existing.items()) + '\n',
                        encoding='utf-8')
    for k, v in kwargs.items():
        os.environ[k] = v

def _creds_ok() -> bool:
    return bool(os.getenv('DHAN_CLIENT_ID','').strip() and
                os.getenv('DHAN_ACCESS_TOKEN','').strip())

# ── colours ───────────────────────────────────────────────────────────────────
C_BG      = "#0d1117"
C_PANEL   = "#161b22"
C_BORDER  = "#30363d"
C_ACCENT  = "#58a6ff"
C_GREEN   = "#3fb950"
C_RED     = "#f85149"
C_YELLOW  = "#d29922"
C_WHITE   = "#e6edf3"
C_MUTED   = "#8b949e"
C_DARK    = "#1c2128"

FONT_MONO = "Courier New"

def _fmt(v, prec=2):
    return f"{v:,.{prec}f}" if v is not None else "—"

def _pnl_col(v):
    if not v: return C_WHITE
    return C_GREEN if v > 0 else C_RED


# ══════════════════════════════════════════════════════════════════════════════
#  CREDENTIALS DIALOG
# ══════════════════════════════════════════════════════════════════════════════
class CredentialsDialog(ctk.CTkToplevel):
    def __init__(self, parent, on_saved):
        super().__init__(parent)
        self.on_saved = on_saved
        self.title("Dhan Credentials")
        self.geometry("500x400")
        self.resizable(False, False)
        self.configure(fg_color=C_BG)
        self.grab_set()
        self.lift()

        ctk.CTkLabel(self, text="Dhan API Credentials",
            font=ctk.CTkFont(FONT_MONO, 17, "bold"),
            text_color=C_ACCENT).pack(pady=(22,4))
        ctk.CTkLabel(self,
            text="Get these from  web.dhan.co → Profile → API Access",
            font=ctk.CTkFont(FONT_MONO, 11), text_color=C_MUTED).pack(pady=(0,14))

        frm = ctk.CTkFrame(self, fg_color=C_PANEL, corner_radius=10)
        frm.pack(fill="x", padx=28)

        def _row(label, env_key, show=""):
            r = ctk.CTkFrame(frm, fg_color="transparent")
            r.pack(fill="x", padx=14, pady=5)
            ctk.CTkLabel(r, text=label, width=150, anchor="w",
                font=ctk.CTkFont(FONT_MONO, 12), text_color=C_MUTED).pack(side="left")
            e = ctk.CTkEntry(r, show=show, width=270,
                font=ctk.CTkFont(FONT_MONO, 12),
                fg_color=C_DARK, border_color=C_BORDER, text_color=C_WHITE)
            val = os.getenv(env_key, '')
            if val and val != '__PLACEHOLDER__':
                e.insert(0, val)
            e.pack(side="left")
            return e

        self.e_cid   = _row("Client ID",     "DHAN_CLIENT_ID")
        self.e_tok   = _row("Access Token",  "DHAN_ACCESS_TOKEN", "•")
        self.e_pin   = _row("PIN (4-digit)", "DHAN_PIN",          "•")
        self.e_totp  = _row("TOTP Secret",   "DHAN_TOTP_SECRET",  "•")

        self.lbl_err = ctk.CTkLabel(self, text="",
            font=ctk.CTkFont(FONT_MONO, 11), text_color=C_RED)
        self.lbl_err.pack(pady=(10,0))

        ctk.CTkButton(self, text="Save & Continue",
            font=ctk.CTkFont(FONT_MONO, 13, "bold"),
            fg_color=C_ACCENT, text_color=C_BG, hover_color="#79c0ff",
            height=38, command=self._save).pack(pady=14)

    def _save(self):
        cid  = self.e_cid.get().strip()
        tok  = self.e_tok.get().strip()
        pin  = self.e_pin.get().strip()
        totp = self.e_totp.get().strip()
        if not cid:
            self.lbl_err.configure(text="Client ID is required."); return
        if not tok and not (pin and totp):
            self.lbl_err.configure(
                text="Need Access Token  OR  both PIN + TOTP Secret."); return
        _save_env(DHAN_CLIENT_ID=cid, DHAN_ACCESS_TOKEN=tok,
                  DHAN_PIN=pin, DHAN_TOTP_SECRET=totp)
        # reload env into process
        _load_dotenv_fallback(str(ENV_PATH))
        self.destroy()
        self.on_saved()


# ══════════════════════════════════════════════════════════════════════════════
#  INSTRUMENT CARD  (one per selected instrument)
# ══════════════════════════════════════════════════════════════════════════════
class InstrumentCard(ctk.CTkFrame):
    def __init__(self, parent, inst: dict):
        super().__init__(parent, fg_color=C_PANEL, corner_radius=10)
        self.inst = inst
        self._build()

    def _lbl(self, parent, text="—", font_size=12, bold=False, color=C_WHITE, anchor="w", **pack):
        l = ctk.CTkLabel(parent, text=text, anchor=anchor,
            font=ctk.CTkFont(FONT_MONO, font_size, "bold" if bold else "normal"),
            text_color=color)
        l.pack(**pack)
        return l

    def _build(self):
        # ── header ──────────────────────────────────────────────────────────
        hdr = ctk.CTkFrame(self, fg_color=C_DARK, corner_radius=8)
        hdr.pack(fill="x", padx=10, pady=(10,4))

        self.lbl_name = ctk.CTkLabel(hdr,
            text=self.inst['name'],
            font=ctk.CTkFont(FONT_MONO, 14, "bold"), text_color=C_ACCENT)
        self.lbl_name.pack(side="left", padx=12, pady=7)

        self.lbl_ltp = ctk.CTkLabel(hdr, text="LTP: —",
            font=ctk.CTkFont(FONT_MONO, 14, "bold"), text_color=C_WHITE)
        self.lbl_ltp.pack(side="left", padx=10)

        self.lbl_note = ctk.CTkLabel(hdr, text="",
            font=ctk.CTkFont(FONT_MONO, 11), text_color=C_MUTED)
        self.lbl_note.pack(side="right", padx=12)

        self.lbl_next = ctk.CTkLabel(hdr, text="",
            font=ctk.CTkFont(FONT_MONO, 11), text_color=C_YELLOW)
        self.lbl_next.pack(side="right", padx=4)

        # ── metrics strip ────────────────────────────────────────────────────
        mrow = ctk.CTkFrame(self, fg_color="transparent")
        mrow.pack(fill="x", padx=10, pady=2)

        def _tile(parent, label):
            f = ctk.CTkFrame(parent, fg_color=C_DARK, corner_radius=6)
            f.pack(side="left", expand=True, fill="x", padx=2)
            ctk.CTkLabel(f, text=label,
                font=ctk.CTkFont(FONT_MONO, 9), text_color=C_MUTED).pack(pady=(4,0))
            v = ctk.CTkLabel(f, text="—",
                font=ctk.CTkFont(FONT_MONO, 12, "bold"), text_color=C_WHITE)
            v.pack(pady=(0,4))
            return v

        self.t_prev   = _tile(mrow, "PREV CLOSE")
        self.t_orbh   = _tile(mrow, "ORB HIGH")
        self.t_orbl   = _tile(mrow, "ORB LOW")
        self.t_st     = _tile(mrow, "SUPERTREND")
        self.t_trend  = _tile(mrow, "TREND")
        self.t_form   = _tile(mrow, "FORMING 3m")

        # ── trade row ────────────────────────────────────────────────────────
        trow = ctk.CTkFrame(self, fg_color=C_DARK, corner_radius=8)
        trow.pack(fill="x", padx=10, pady=3)

        ctk.CTkLabel(trow, text="POSITION",
            font=ctk.CTkFont(FONT_MONO, 9), text_color=C_MUTED).pack(
            side="left", padx=(10,4), pady=6)

        self.t_pos     = ctk.CTkLabel(trow, text="No position",
            font=ctk.CTkFont(FONT_MONO, 12, "bold"), text_color=C_MUTED)
        self.t_pos.pack(side="left", padx=4)

        self.t_entry_p = ctk.CTkLabel(trow, text="",
            font=ctk.CTkFont(FONT_MONO, 11), text_color=C_MUTED)
        self.t_entry_p.pack(side="left", padx=6)

        self.t_curr_p  = ctk.CTkLabel(trow, text="",
            font=ctk.CTkFont(FONT_MONO, 11), text_color=C_WHITE)
        self.t_curr_p.pack(side="left", padx=6)

        self.t_qty     = ctk.CTkLabel(trow, text="",
            font=ctk.CTkFont(FONT_MONO, 11), text_color=C_MUTED)
        self.t_qty.pack(side="left", padx=6)

        self.t_unreal  = ctk.CTkLabel(trow, text="",
            font=ctk.CTkFont(FONT_MONO, 12, "bold"), text_color=C_WHITE)
        self.t_unreal.pack(side="right", padx=10)

        self.t_real    = ctk.CTkLabel(trow, text="Realized: ₹0.00",
            font=ctk.CTkFont(FONT_MONO, 11), text_color=C_MUTED)
        self.t_real.pack(side="right", padx=10)

        # ── candle table ─────────────────────────────────────────────────────
        ctk.CTkLabel(self, text="Last closed 3m candles",
            font=ctk.CTkFont(FONT_MONO, 10), text_color=C_MUTED,
            anchor="w").pack(fill="x", padx=14, pady=(6,0))

        ctbl = ctk.CTkFrame(self, fg_color=C_DARK, corner_radius=6)
        ctbl.pack(fill="x", padx=10, pady=(2,0))

        hdr2 = ctk.CTkFrame(ctbl, fg_color="transparent")
        hdr2.pack(fill="x", padx=6, pady=(3,0))
        for col, w in [("TIME",60),("OPEN",90),("HIGH",90),("LOW",90),("CLOSE",90)]:
            ctk.CTkLabel(hdr2, text=col, width=w, anchor="e",
                font=ctk.CTkFont(FONT_MONO, 9), text_color=C_MUTED).pack(side="left")

        self.c_rows = []
        for _ in range(SHOW_3M_HISTORY):
            r = ctk.CTkFrame(ctbl, fg_color="transparent")
            r.pack(fill="x", padx=6)
            cells = []
            for w in [60,90,90,90,90]:
                lbl = ctk.CTkLabel(r, text="", width=w, anchor="e",
                    font=ctk.CTkFont(FONT_MONO, 11), text_color=C_WHITE)
                lbl.pack(side="left")
                cells.append(lbl)
            self.c_rows.append(cells)

        # ── trade log ────────────────────────────────────────────────────────
        ctk.CTkLabel(self, text="Trade log",
            font=ctk.CTkFont(FONT_MONO, 10), text_color=C_MUTED,
            anchor="w").pack(fill="x", padx=14, pady=(6,0))

        self.log_box = ctk.CTkTextbox(self, height=120,
            fg_color=C_DARK, border_color=C_BORDER,
            font=ctk.CTkFont(FONT_MONO, 11), text_color=C_WHITE,
            state="disabled")
        self.log_box.pack(fill="x", padx=10, pady=(2,10))

    # ── live update ──────────────────────────────────────────────────────────
    def refresh(self, snap: dict):
        ltp  = snap['last_ltp']
        prev = snap['prev_close']

        # LTP
        if ltp is not None and prev is not None:
            chg = ltp - prev
            pct = chg / prev * 100
            col = C_GREEN if chg >= 0 else C_RED
            sgn = "+" if chg >= 0 else ""
            self.lbl_ltp.configure(
                text=f"LTP: {ltp:,.2f}  {sgn}{chg:.2f} ({sgn}{pct:.2f}%)",
                text_color=col)
        elif ltp is not None:
            self.lbl_ltp.configure(text=f"LTP: {ltp:,.2f}", text_color=C_WHITE)

        self.t_prev.configure(text=_fmt(prev))
        self.lbl_note.configure(text=(snap.get('note') or '')[:50])
        nxt = snap.get('next_eval')
        self.lbl_next.configure(text=f"Next eval: {nxt}" if nxt else "")

        # ORB
        orb_rdy = snap['orb_ready']
        self.t_orbh.configure(text=_fmt(snap['orb_high']),
            text_color=C_GREEN if orb_rdy else C_MUTED)
        self.t_orbl.configure(text=_fmt(snap['orb_low']),
            text_color=C_RED if orb_rdy else C_MUTED)

        # Supertrend
        st_dir = snap['st_dir']
        if st_dir is not None:
            trend_txt = "▲ UP" if int(st_dir) > 0 else "▼ DOWN"
            trend_col = C_GREEN if int(st_dir) > 0 else C_RED
        else:
            trend_txt, trend_col = "—", C_MUTED
        self.t_st.configure(text=_fmt(snap['st_value']))
        self.t_trend.configure(text=trend_txt, text_color=trend_col)

        # Forming 3m
        c3 = snap['current_3m']
        if c3:
            self.t_form.configure(
                text=f"{epoch_to_local_str(c3['bucket'],False)} {c3.get('parts',0)}/3",
                text_color=C_YELLOW)
        else:
            self.t_form.configure(text="—", text_color=C_MUTED)

        # Position
        pos = snap['position']
        inst = snap['instrument']
        if pos:
            strike = snap['entry_strike']
            sym = f"{inst['option_prefix']} ATM {pos} {strike}"
            self.t_pos.configure(text=sym,
                text_color=C_GREEN if pos == 'CE' else C_RED)
            ep = snap.get('entry_option_price')
            cp = snap.get('current_option_price')
            self.t_entry_p.configure(text=f"Entry: {_fmt(ep)}", text_color=C_MUTED)
            self.t_curr_p.configure(text=f"Last: {_fmt(cp)}", text_color=C_WHITE)
            qty = (snap.get('entry_lot_size') or 1) * max(1, snap.get('entry_lots', 1))
            self.t_qty.configure(text=f"Qty: {qty}", text_color=C_MUTED)
            ur = snap.get('unrealized_pnl_rupees', 0.0) or 0.0
            sgn = "+" if ur >= 0 else ""
            self.t_unreal.configure(
                text=f"Unreal: {sgn}₹{ur:,.2f}", text_color=_pnl_col(ur))
        else:
            self.t_pos.configure(text="No position", text_color=C_MUTED)
            self.t_entry_p.configure(text="")
            self.t_curr_p.configure(text="")
            self.t_qty.configure(text="")
            self.t_unreal.configure(text="")

        rr = snap.get('realized_pnl_rupees', 0.0) or 0.0
        sgn = "+" if rr >= 0 else ""
        self.t_real.configure(
            text=f"Realized: {sgn}₹{rr:,.2f}", text_color=_pnl_col(rr))

        # Candle history
        hist = snap['history_3m']
        for i, cells in enumerate(self.c_rows):
            idx = len(hist) - SHOW_3M_HISTORY + i
            if 0 <= idx < len(hist):
                cd = hist[idx]
                t = epoch_to_local_str(cd['bucket'], False)
                o,h,l,c = cd['open'],cd['high'],cd['low'],cd['close']
                ccol = C_GREEN if c >= o else C_RED
                for cell, val in zip(cells,
                        [t, f"{o:.2f}", f"{h:.2f}", f"{l:.2f}", f"{c:.2f}"]):
                    cell.configure(text=val,
                        text_color=ccol if cell is cells[4] else C_WHITE)
            else:
                for cell in cells:
                    cell.configure(text="")

        # Trade log
        logs = snap['trade_log'][:SHOW_TRADE_LOG]
        self.log_box.configure(state="normal")
        self.log_box.delete("1.0", "end")
        if logs:
            for row in reversed(logs):
                self.log_box.insert("end", row + "\n")
        else:
            self.log_box.insert("end", "No trades yet.")
        self.log_box.configure(state="disabled")


# ══════════════════════════════════════════════════════════════════════════════
#  MAIN WINDOW
# ══════════════════════════════════════════════════════════════════════════════
class MainWindow(ctk.CTk):
    def __init__(self):
        super().__init__()
        self.title("Dhan ORB + Supertrend — Paper Trader  |  Balfund Trading")
        self.geometry("1280x820")
        self.minsize(960, 640)
        self.configure(fg_color=C_BG)
        ctk.set_appearance_mode("dark")
        ctk.set_default_color_theme("blue")

        self._app: 'App | None' = None
        self._running = False
        self._cards: dict = {}

        self._build_topbar()
        self._build_body()
        self._build_statusbar()
        self.protocol("WM_DELETE_WINDOW", self._on_close)
        self._tick()

    # ── top bar ──────────────────────────────────────────────────────────────
    def _build_topbar(self):
        bar = ctk.CTkFrame(self, fg_color=C_PANEL, corner_radius=0, height=54)
        bar.pack(fill="x")
        bar.pack_propagate(False)

        ctk.CTkLabel(bar, text="◈  DHAN ORB PAPER TRADER",
            font=ctk.CTkFont(FONT_MONO, 15, "bold"),
            text_color=C_ACCENT).pack(side="left", padx=16)

        self.lbl_clock = ctk.CTkLabel(bar, text="",
            font=ctk.CTkFont(FONT_MONO, 12), text_color=C_MUTED)
        self.lbl_clock.pack(side="left", padx=10)

        # right side
        ctk.CTkButton(bar, text="⚙  Credentials", width=130,
            font=ctk.CTkFont(FONT_MONO, 12),
            fg_color=C_BORDER, hover_color=C_DARK, text_color=C_WHITE,
            command=self._open_creds).pack(side="right", padx=8, pady=9)

        self.btn_start = ctk.CTkButton(bar, text="▶  START", width=130,
            font=ctk.CTkFont(FONT_MONO, 13, "bold"),
            fg_color=C_GREEN, text_color=C_BG, hover_color="#56d364",
            command=self._toggle).pack(side="right", padx=4, pady=9)

        # store reference after pack
        self.btn_start = None
        # rebuild properly
        for w in bar.winfo_children():
            w.pack_forget()

        ctk.CTkLabel(bar, text="◈  DHAN ORB PAPER TRADER",
            font=ctk.CTkFont(FONT_MONO, 15, "bold"),
            text_color=C_ACCENT).pack(side="left", padx=16)
        self.lbl_clock = ctk.CTkLabel(bar, text="",
            font=ctk.CTkFont(FONT_MONO, 12), text_color=C_MUTED)
        self.lbl_clock.pack(side="left", padx=10)

        ctk.CTkButton(bar, text="⚙  Credentials", width=130,
            font=ctk.CTkFont(FONT_MONO, 12),
            fg_color=C_BORDER, hover_color=C_DARK, text_color=C_WHITE,
            command=self._open_creds).pack(side="right", padx=8, pady=9)

        self.btn_start = ctk.CTkButton(bar, text="▶  START", width=130,
            font=ctk.CTkFont(FONT_MONO, 13, "bold"),
            fg_color=C_GREEN, text_color=C_BG, hover_color="#56d364",
            command=self._toggle)
        self.btn_start.pack(side="right", padx=4, pady=9)

        all_modes = list(INSTRUMENTS.keys()) + list(TRADE_MODE_GROUPS.keys())
        self.mode_var = ctk.StringVar(value=os.getenv('TRADE_MODE','NIFTY'))
        ctk.CTkOptionMenu(bar, values=all_modes, variable=self.mode_var,
            width=170, font=ctk.CTkFont(FONT_MONO, 12),
            fg_color=C_DARK, button_color=C_BORDER,
            dropdown_fg_color=C_PANEL, text_color=C_WHITE).pack(
            side="right", padx=4, pady=9)

        ctk.CTkLabel(bar, text="MODE:", font=ctk.CTkFont(FONT_MONO, 11),
            text_color=C_MUTED).pack(side="right", padx=(8,0))

    # ── body ─────────────────────────────────────────────────────────────────
    def _build_body(self):
        self.scroll = ctk.CTkScrollableFrame(self, fg_color=C_BG, corner_radius=0)
        self.scroll.pack(fill="both", expand=True, padx=6, pady=4)

    # ── status bar ───────────────────────────────────────────────────────────
    def _build_statusbar(self):
        bar = ctk.CTkFrame(self, fg_color=C_PANEL, corner_radius=0, height=28)
        bar.pack(fill="x", side="bottom")
        bar.pack_propagate(False)
        self.lbl_ws    = ctk.CTkLabel(bar, text="● WS: offline",
            font=ctk.CTkFont(FONT_MONO, 11), text_color=C_RED)
        self.lbl_ws.pack(side="left", padx=12, pady=4)
        self.lbl_pkts  = ctk.CTkLabel(bar, text="Packets T/P/O/D: 0/0/0/0",
            font=ctk.CTkFont(FONT_MONO, 11), text_color=C_MUTED)
        self.lbl_pkts.pack(side="left", padx=12)
        self.lbl_wserr = ctk.CTkLabel(bar, text="",
            font=ctk.CTkFont(FONT_MONO, 11), text_color=C_RED)
        self.lbl_wserr.pack(side="left", padx=8)
        self.lbl_phase = ctk.CTkLabel(bar, text="",
            font=ctk.CTkFont(FONT_MONO, 11), text_color=C_YELLOW)
        self.lbl_phase.pack(side="right", padx=12)

    # ── cards ────────────────────────────────────────────────────────────────
    def _build_cards(self, selected):
        for w in self.scroll.winfo_children():
            w.destroy()
        self._cards.clear()
        for inst in selected:
            card = InstrumentCard(self.scroll, inst)
            card.pack(fill="x", padx=4, pady=4)
            self._cards[_engine_key(inst['security_id'], inst['exchange'])] = card

    # ── credentials ──────────────────────────────────────────────────────────
    def _open_creds(self):
        CredentialsDialog(self, lambda: None)

    # ── start / stop ─────────────────────────────────────────────────────────
    def _toggle(self):
        if self._running:
            self._stop()
        else:
            self._start()

    def _start(self):
        if not _creds_ok():
            CredentialsDialog(self, lambda: None)
            mb.showinfo("Credentials needed",
                "Please save your credentials then click START again.")
            return

        mode = self.mode_var.get().strip().upper()
        os.environ['TRADE_MODE'] = mode
        _save_env(TRADE_MODE=mode)

        self.btn_start.configure(text="⏳  Starting…", state="disabled",
            fg_color=C_YELLOW, text_color=C_BG)
        self.lbl_wserr.configure(text="")
        self.update()

        def _init_thread():
            try:
                # reload credentials fresh
                _load_dotenv_fallback(str(ENV_PATH))
                # rebuild global WS_URL with fresh token
                global DHAN_CLIENT_ID, DHAN_ACCESS_TOKEN, WS_URL
                DHAN_CLIENT_ID    = os.getenv('DHAN_CLIENT_ID','').strip()
                DHAN_ACCESS_TOKEN = os.getenv('DHAN_ACCESS_TOKEN','').strip()
                WS_URL = (f"wss://api-feed.dhan.co?version=2"
                          f"&token={DHAN_ACCESS_TOKEN}"
                          f"&clientId={DHAN_CLIENT_ID}&authType=2")

                app = App()
                app.bootstrap()
                self._app = app
                self._running = True
                self.after(0, lambda: self._build_cards(app.selected))
                self.after(0, lambda: self.btn_start.configure(
                    text="◼  STOP", state="normal",
                    fg_color=C_RED, text_color=C_WHITE))
                # start WS in background daemon thread
                t = threading.Thread(target=app.run_ws_loop, daemon=True)
                t.start()
                app.ws_thread = t
            except Exception as e:
                self._running = False
                err_msg = str(e)
                self.after(0, lambda: [
                    self.btn_start.configure(text="▶  START", state="normal",
                        fg_color=C_GREEN, text_color=C_BG),
                    self.lbl_wserr.configure(text=f"Error: {err_msg[:80]}")])

        threading.Thread(target=_init_thread, daemon=True).start()

    def _stop(self):
        if self._app:
            self._app.stop()
            self._app = None
        self._running = False
        self.btn_start.configure(text="▶  START",
            fg_color=C_GREEN, text_color=C_BG, state="normal")
        self.lbl_ws.configure(text="● WS: offline", text_color=C_RED)

    # ── tick (1 second refresh) ───────────────────────────────────────────────
    def _tick(self):
        try:
            self.lbl_clock.configure(
                text=now_local().strftime("%Y-%m-%d  %H:%M:%S"))

            if self._app and self._running:
                with self._app.stats_lock:
                    pc  = dict(self._app.packet_counts)
                    err = self._app.last_ws_error
                    ct  = self._app.last_ws_connect_time

                # WS status
                if ct:
                    self.lbl_ws.configure(
                        text=f"● WS: online  {int(time.time()-ct)}s",
                        text_color=C_GREEN)
                else:
                    self.lbl_ws.configure(
                        text="● WS: connecting…", text_color=C_YELLOW)

                self.lbl_pkts.configure(
                    text=f"Packets T/P/O/D: {pc['ticker']}/{pc['prev_close']}/{pc['other']}/{pc['disconnect']}")
                self.lbl_wserr.configure(
                    text=f"WS: {err}" if err else "")

                phase_map = {
                    'PREOPEN':    'Pre-open',
                    'ORB_WAIT':   'ORB window (09:18–09:24)',
                    'PRE10':      'Pre-10 ORB breakout mode',
                    'POST10':     'Post-10 Supertrend mode',
                    'POSTMARKET': 'Market closed',
                }
                self.lbl_phase.configure(
                    text=phase_map.get(current_market_phase(), ''))

                # option subscriptions + fallback poll
                self._app.process_option_subscriptions()
                now_ts = time.time()
                for inst in self._app.selected:
                    eng = self._app.engines.get(
                        _engine_key(inst['security_id'], inst['exchange']))
                    if eng is None: continue
                    with eng.lock:
                        wc  = eng.last_option_tick_wc
                        has = eng.position is not None
                    if has and (now_ts - wc) > 5.0:
                        eng.poll_option_prices()

                # refresh cards
                for key, card in self._cards.items():
                    eng = self._app.engines.get(key)
                    if eng:
                        card.refresh(eng.snapshot())
            else:
                self.lbl_ws.configure(text="● WS: offline", text_color=C_RED)

        except Exception:
            pass
        self.after(1000, self._tick)

    def _on_close(self):
        self._stop()
        self.destroy()


# ══════════════════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════
def main():
    # suppress module-level crash if creds missing — GUI handles it
    os.environ.setdefault('DHAN_CLIENT_ID',    '__PLACEHOLDER__')
    os.environ.setdefault('DHAN_ACCESS_TOKEN', '__PLACEHOLDER__')

    # load .env
    _load_dotenv_fallback(str(ENV_PATH))

    # first-launch message
    if not ENV_PATH.exists():
        root = tk.Tk(); root.withdraw()
        mb.showinfo("Welcome",
            "No .env file found.\n\n"
            "Please enter your Dhan credentials on the next screen.\n\n"
            "You can get them from:\n"
            "web.dhan.co → Profile → API Access")
        root.destroy()

    win = MainWindow()

    # auto-open credentials if missing
    if not _creds_ok():
        win.after(400, win._open_creds)

    win.mainloop()


if __name__ == '__main__':
    main()
