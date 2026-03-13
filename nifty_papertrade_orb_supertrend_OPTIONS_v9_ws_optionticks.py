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

    def ui_loop(self):
        while not self.stop_evt.is_set():
            try:
                self.process_option_subscriptions()
                # Fallback: if no WS option tick has arrived in the last 5s,
                # poll the option chain API to keep current_option_price fresh.
                # Once WS ticks flow, on_option_tick() takes over and this becomes a no-op.
                now_ts = time.time()
                for inst in self.selected:
                    eng = self.engines[_engine_key(inst['security_id'], inst['exchange'])]
                    with eng.lock:
                        last_tick_wc = eng.last_option_tick_wc
                        has_position = eng.position is not None
                    if has_position:
                        # Compare wall-clock times — safe, no IST/UTC mismatch
                        ws_tick_age = now_ts - last_tick_wc
                        if ws_tick_age > 5.0:
                            eng.poll_option_prices()
                self.print_screen()
            except Exception as e:
                sys.stdout.write(f'\nUI error: {e}\n')
                sys.stdout.flush()
            time.sleep(REFRESH_EVERY_SEC)


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

    def print_screen(self):
        with self.stats_lock:
            pcounts = dict(self.packet_counts)
            err = self.last_ws_error
            conn_time = self.last_ws_connect_time
        uptime = '-' if not conn_time else f"{int(time.time() - conn_time)}s"
        phase = current_market_phase()
        phase_note = {
            'PREOPEN': 'waiting for market to open',
            'ORB_WAIT': 'waiting for ORB levels (09:18-09:24)',
            'PRE10': 'pre-10 ORB breakout mode',
            'POST10': 'post-10 Supertrend-only mode',
            'POSTMARKET': 'market closed',
        }[phase]

        sys.stdout.write('\033[2J\033[H')
        sys.stdout.flush()
        title = ' / '.join(inst['name'] for inst in self.selected)
        print(f"{BOLD}Index Paper Trading | 3m ORB + Supertrend | {title}{RESET}  {DIM}{now_local().strftime('%Y-%m-%d %H:%M:%S')}{RESET}  WS:{uptime}")
        print('═' * 132)
        print(f"  Mode: {CYAN}{TRADE_MODE}{RESET}   Session: {YELLOW}{phase_note}{RESET}")
        print(f"  Packets T/P/O/D: {pcounts['ticker']}/{pcounts['prev_close']}/{pcounts['other']}/{pcounts['disconnect']}")
        if err:
            print(f"  {RED}WS Error:{RESET} {err}")

        for inst in self.selected:
            snap = self.engines[_engine_key(inst['security_id'], inst['exchange'])].snapshot()
            print('\n' + '─' * 132)
            print(f"{BOLD}  {inst['name']}{RESET} [{inst['exchange']}] secId={inst['security_id']}")
            print(f"  Last LTP: {snap['last_ltp'] if snap['last_ltp'] is not None else '-'}   PrevClose: {snap['prev_close'] if snap['prev_close'] is not None else '-'}")

            c3 = snap['current_3m']
            if c3:
                print(f"  Forming 3m: {epoch_to_local_str(c3['bucket'], with_seconds=False)}  O:{c3['open']:.2f} H:{c3['high']:.2f} L:{c3['low']:.2f} C:{c3['close']:.2f} parts:{c3.get('parts',0)}")
            else:
                print('  Forming 3m: -')

            nxt = snap.get('next_eval') or '-'
            print(f"  Next evaluation (3m close): {nxt}")
            st_dir = snap['st_dir']
            trend = '-' if st_dir is None else ('UP' if int(st_dir) > 0 else 'DOWN')
            trend_col = WHITE if st_dir is None else (GREEN if int(st_dir) > 0 else RED)
            stv = '-' if snap['st_value'] is None else f"{snap['st_value']:.2f}"
            sig = snap['st_signal'] or '-'
            orb_h = '-' if snap['orb_high'] is None else f"{snap['orb_high']:.2f}"
            orb_l = '-' if snap['orb_low'] is None else f"{snap['orb_low']:.2f}"
            print(f"  ORB Ready: {snap['orb_ready']}   ORB High: {orb_h}   ORB Low: {orb_l}   ST: {stv}   Trend: {trend_col}{trend}{RESET}   Flip: {sig}")

            pos = snap['position'] or '-'
            pos_col = WHITE if pos == '-' else (GREEN if pos == 'CE' else RED)
            idx_entry = '-' if snap['entry_price'] is None else f"{snap['entry_price']:.2f}"
            strike = '-' if snap['entry_strike'] is None else str(snap['entry_strike'])
            active_trade = '-' if pos == '-' else f"{inst['option_prefix']} ATM {pos} {strike}"
            opt_entry = '-' if snap.get('entry_option_price') is None else f"{snap['entry_option_price']:.2f}"
            opt_last = '-' if snap.get('current_option_price') is None else f"{snap['current_option_price']:.2f}"
            qty = '-'
            if snap.get('entry_lot_size'):
                qty = str(int(snap.get('entry_lot_size', 0)) * max(1, int(snap.get('entry_lots', 1))))
            print(f"  Active Trade: {pos_col}{active_trade}{RESET}   Entry(Index): {idx_entry}   Entry Prem: {opt_entry}   Last Prem: {opt_last}   Qty: {qty}")
            print(f"  P&L: {snap['unrealized_pnl']:.2f} prem / ₹{snap.get('unrealized_pnl_rupees', 0.0):.2f} unreal   {snap['realized_pnl']:.2f} prem / ₹{snap.get('realized_pnl_rupees', 0.0):.2f} realized")
            print(f"  Note: {YELLOW}{snap['note']}{RESET}")

            print('\n  Last closed 3m candles')
            print('  ' + '─' * 86)
            print('   Time    Open       High       Low      Close')
            hist = snap['history_3m'][-SHOW_3M_HISTORY:]
            if not hist:
                print('   -')
            else:
                for cd in hist:
                    print(f"   {epoch_to_local_str(cd['bucket'], with_seconds=False):5}  {cd['open']:9.2f}  {cd['high']:9.2f}  {cd['low']:9.2f}  {cd['close']:9.2f}")

            print('\n  Recent paper trades (live option premium with lot-size ₹ P&L)')
            print('  ' + '─' * 86)
            if not snap['trade_log']:
                print('   -')
            else:
                for row in snap['trade_log'][:SHOW_TRADE_LOG]:
                    print(f'   {row}')

        print('\n  Rules now applied: ORB uses 09:18-09:21 and 09:21-09:24; entries after 09:24 only; pre-10 clean ORB breakout; exits on ORB SL or ST cross; post-10 ST-only directional flips; CE/PE premium is tracked from Dhan Option Chain and ₹ P&L uses lot size.')


def main():
    app = App()

    def _handle_sig(_sig, _frame):
        app.stop()
        raise SystemExit(0)

    signal.signal(signal.SIGINT, _handle_sig)
    signal.signal(signal.SIGTERM, _handle_sig)
    app.start()


if __name__ == '__main__':
    main()