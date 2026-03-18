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


# ── credentials loaded lazily so GUI can show dialog first ──────────────────
# Do NOT raise SystemExit here — the GUI handles missing credentials gracefully.
if getattr(sys, 'frozen', False):
    _BASE_DIR_EARLY = str(__import__('pathlib').Path(sys.executable).parent)
else:
    _BASE_DIR_EARLY = str(__import__('pathlib').Path(__file__).parent)

_load_dotenv_fallback(_BASE_DIR_EARLY + '/.env')
_load_dotenv_fallback('.env')

DHAN_CLIENT_ID    = os.getenv('DHAN_CLIENT_ID', '').strip()
DHAN_ACCESS_TOKEN = os.getenv('DHAN_ACCESS_TOKEN', '').strip()
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN', '').strip()
TELEGRAM_CHAT_ID   = os.getenv('TELEGRAM_CHAT_ID', '').strip()

# Placeholders keep the rest of the module from crashing at import time.
# The GUI will replace these before starting the engine.
if not DHAN_CLIENT_ID:
    DHAN_CLIENT_ID = '__PLACEHOLDER__'
if not DHAN_ACCESS_TOKEN:
    DHAN_ACCESS_TOKEN = '__PLACEHOLDER__'

WS_URL = (
    f"wss://api-feed.dhan.co?version=2"
    f"&token={DHAN_ACCESS_TOKEN}&clientId={DHAN_CLIENT_ID}&authType=2"
)

# ---------------- Config ----------------
# TRADE_MODE options:
#   Single : NIFTY | BANKNIFTY | SENSEX | FINNIFTY | MIDCPNIFTY |
#            DIXON | KAYNES | BAJAJ_AUTO | MARUTI | EICHERMOT | HEROMOTOCO |
#            BSE | MCX | ADANIENT | LTIM | PERSISTENT | OFSS | INDIGO |
#            TVSMOTOR | ULTRACEMCO | BRITANNIA | APOLLOHOSP | RELIANCE |
#            TATAELXSI | POLYCAB
#   Groups : ALL_INDEX  (NIFTY + BANKNIFTY + SENSEX + FINNIFTY + MIDCPNIFTY)
#            ALL_STOCKS (all 20 stocks)
#            COMMODITY  (CRUDEOILM)
#            ALL        (ALL_INDEX + ALL_STOCKS + COMMODITY)
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
        'default_lot_size': int(os.getenv('NIFTY_LOT_SIZE', '65')),
        'lots': int(os.getenv('NIFTY_LOTS', '1')),
    },
    'BANKNIFTY': {
        'key': 'BANKNIFTY', 'name': 'BANKNIFTY',
        'exchange': 'IDX_I', 'security_id': '25',
        'instrument_type': 'INDEX',
        'display_prec': 2, 'strike_step': 100, 'option_prefix': 'BANKNIFTY',
        'fno_exchange': 'NSE_FNO',
        'default_lot_size': int(os.getenv('BANKNIFTY_LOT_SIZE', '30')),
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
    'FINNIFTY': {
        'key': 'FINNIFTY', 'name': 'FINNIFTY',
        'exchange': 'IDX_I', 'security_id': '27',
        'instrument_type': 'INDEX',
        'display_prec': 2, 'strike_step': 50, 'option_prefix': 'FINNIFTY',
        'fno_exchange': 'NSE_FNO',
        'default_lot_size': int(os.getenv('FINNIFTY_LOT_SIZE', '60')),
        'lots': int(os.getenv('FINNIFTY_LOTS', '1')),
    },
    'MIDCPNIFTY': {
        'key': 'MIDCPNIFTY', 'name': 'MIDCAP NIFTY',
        'exchange': 'IDX_I', 'security_id': '442',
        'instrument_type': 'INDEX',
        'display_prec': 2, 'strike_step': 25, 'option_prefix': 'MIDCPNIFTY',
        'fno_exchange': 'NSE_FNO',
        'default_lot_size': int(os.getenv('MIDCPNIFTY_LOT_SIZE', '120')),
        'lots': int(os.getenv('MIDCPNIFTY_LOTS', '1')),
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
    'CRUDEOILM': {
        # MCX Mini Crude Oil futures — security_id resolved at runtime from scrip master
        # Correct exchange segment string for MCX is 'MCX_COMM' (double M)
        'key': 'CRUDEOILM', 'name': 'CRUDE OIL MINI', 'mcx_symbol': 'CRUDEOILM',
        'exchange': 'MCX_COMM', 'security_id': '0',
        'instrument_type': 'FUTCOM',
        'display_prec': 2, 'strike_step': 100, 'option_prefix': 'CRUDEOILM',
        'fno_exchange': 'MCX_COMM',
        'default_lot_size': int(os.getenv('CRUDEOILM_LOT_SIZE', '10')),
        'lots': int(os.getenv('CRUDEOILM_LOTS', '1')),
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
    'TATAELXSI': {
        'key': 'TATAELXSI', 'name': 'TATA ELXSI', 'nse_symbol': 'TATAELXSI',
        'exchange': 'NSE_EQ', 'security_id': '3411',
        'instrument_type': 'EQUITY',
        'display_prec': 2, 'strike_step': 100, 'option_prefix': 'TATAELXSI',
        'fno_exchange': 'NSE_FNO',
        'default_lot_size': int(os.getenv('TATAELXSI_LOT_SIZE', '100')),
        'lots': int(os.getenv('TATAELXSI_LOTS', '1')),
    },
    'POLYCAB': {
        'key': 'POLYCAB', 'name': 'POLYCAB INDIA', 'nse_symbol': 'POLYCAB',
        'exchange': 'NSE_EQ', 'security_id': '9590',
        'instrument_type': 'EQUITY',
        'display_prec': 2, 'strike_step': 100, 'option_prefix': 'POLYCAB',
        'fno_exchange': 'NSE_FNO',
        'default_lot_size': int(os.getenv('POLYCAB_LOT_SIZE', '125')),
        'lots': int(os.getenv('POLYCAB_LOTS', '1')),
    },
}

# Instrument groupings for TRADE_MODE
TRADE_MODE_GROUPS: Dict[str, List[str]] = {
    'ALL_INDEX':  ['NIFTY', 'BANKNIFTY', 'SENSEX', 'FINNIFTY', 'MIDCPNIFTY'],
    'ALL_STOCKS': ['DIXON', 'KAYNES', 'BAJAJ_AUTO', 'MARUTI', 'EICHERMOT',
                   'HEROMOTOCO', 'BSE', 'MCX', 'ADANIENT', 'LTIM',
                   'PERSISTENT', 'OFSS', 'INDIGO', 'TVSMOTOR', 'ULTRACEMCO',
                   'BRITANNIA', 'APOLLOHOSP', 'RELIANCE', 'TATAELXSI', 'POLYCAB'],
}
TRADE_MODE_GROUPS['COMMODITY'] = ['CRUDEOILM']
TRADE_MODE_GROUPS['ALL'] = TRADE_MODE_GROUPS['ALL_INDEX'] + TRADE_MODE_GROUPS['ALL_STOCKS'] + TRADE_MODE_GROUPS['COMMODITY']


# Dhan WebSocket exchange segment byte → exchange string mapping
# Source: Dhan market_data reference + official docs
_EXCH_SEG_MAP: Dict[int, str] = {
    0: 'IDX_I',        # Indices
    1: 'NSE_EQ',       # NSE Cash Equity
    2: 'NSE_FNO',      # NSE F&O
    3: 'NSE_CURRENCY', # NSE Currency
    4: 'BSE_EQ',       # BSE Cash Equity
    5: 'MCX_COMM',     # MCX Commodity  ← correct string is MCX_COMM (double M)
    7: 'BSE_CURRENCY', # BSE Currency
    8: 'BSE_FNO',      # BSE F&O
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


def is_market_time(dt: datetime, exchange: str = 'NSE_EQ') -> bool:
    hm = dt.hour * 60 + dt.minute
    if exchange == 'MCX_COMM':
        # MCX commodity market: 09:00 – 23:30 IST
        return 9 * 60 <= hm <= 23 * 60 + 30
    # NSE/BSE equity + F&O: 09:15 – 15:30 IST
    return 9 * 60 + 15 <= hm <= 15 * 60 + 30


def now_local() -> datetime:
    return datetime.now().astimezone()


def current_market_phase(exchange: str = 'NSE_EQ') -> str:
    dt = now_local()
    hm = dt.hour * 60 + dt.minute
    if exchange == 'MCX_COMM':
        # MCX commodity market: 09:00 – 23:30 IST
        if hm < 9 * 60:
            return 'PREOPEN'
        if hm <= 23 * 60 + 30:
            return 'POST10'   # MCX has no ORB concept — goes straight to POST10 logic
        return 'POSTMARKET'
    # NSE/BSE equity + F&O: 09:15 – 15:30 IST
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
        # Build two maps:
        # 1. NSE equity: EXCH_ID=NSE, SEGMENT=E, INSTRUMENT=EQUITY → keyed by UNDERLYING_SYMBOL
        # 2. MCX futures: EXCH_ID=MCX, INSTRUMENT=FUTCOM → keyed by UNDERLYING_SYMBOL
        #    MCX near-month contract is the one with earliest expiry (EXPIRY_FLAG=Y or lowest date)
        eq_map:  Dict[str, str] = {}  # symbol → security_id  (NSE equity)
        mcx_map: Dict[str, tuple] = {}  # symbol → (security_id, expiry_date, contract_display)
        for row in reader:
            exch  = row.get('EXCH_ID','').strip().upper()
            seg   = row.get('SEGMENT','').strip().upper()
            instr = row.get('INSTRUMENT','').strip().upper()
            sym   = str(row.get('UNDERLYING_SYMBOL') or '').strip().upper()
            sid   = str(row.get('SECURITY_ID') or '').strip()
            if not sym or not sid or sid == '0':
                continue
            # NSE equity
            if exch == 'NSE' and seg == 'E' and instr == 'EQUITY':
                eq_map[sym] = sid
            # MCX futures — keep near-month (earliest expiry)
            if exch == 'MCX' and instr == 'FUTCOM':
                exp = str(row.get('SM_EXPIRY_DATE') or '').strip()
                sym_name = str(row.get('SYMBOL_NAME') or '').strip()
                existing = mcx_map.get(sym)
                if not existing or (exp and exp < existing[1]):
                    mcx_map[sym] = (sid, exp, sym_name)

        resolved = 0
        for key, inst in INSTRUMENTS.items():
            itype = inst.get('instrument_type','')
            if itype == 'EQUITY':
                nse_sym = str(inst.get('nse_symbol', '')).strip().upper()
                if not nse_sym: continue
                sid = eq_map.get(nse_sym)
                if sid:
                    inst['security_id'] = sid
                    resolved += 1
                else:
                    print(f'[SECID] WARNING: could not resolve {key} ({nse_sym}) — fallback {inst["security_id"]}')
            elif itype == 'FUTCOM':
                mcx_sym = str(inst.get('mcx_symbol', inst.get('key',''))).strip().upper()
                if not mcx_sym: continue
                tup = mcx_map.get(mcx_sym)
                if tup:
                    inst['security_id'] = tup[0]
                    inst['contract_display'] = f"{mcx_sym} {tup[1]}"
                    resolved += 1
                    print(f'[SECID] MCX {key}: security_id={tup[0]} contract={tup[2]} expiry={tup[1]}')
                else:
                    print(f'[SECID] WARNING: could not resolve MCX {key} ({mcx_sym}) — fallback {inst["security_id"]}')
        print(f'[SECID] Resolved {resolved} security IDs from scrip master.')
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


# ---------------- telegram alerts ----------------
import queue as _queue

class TelegramAlerter:
    """Non-blocking Telegram alert sender.
    Uses a background thread + queue so trade execution is never delayed
    by network latency. If Telegram is not configured, silently does nothing.
    """
    _SEND_URL = 'https://api.telegram.org/bot{token}/sendMessage'

    def __init__(self, bot_token: str, chat_id: str):
        self._token   = bot_token.strip()
        self._chat_id = chat_id.strip()
        self._enabled = bool(self._token and self._chat_id)
        self._q: '_queue.Queue[str]' = _queue.Queue(maxsize=100)
        if self._enabled:
            t = threading.Thread(target=self._worker, daemon=True)
            t.start()

    def _worker(self):
        while True:
            text = self._q.get()
            try:
                requests.post(
                    self._SEND_URL.format(token=self._token),
                    json={'chat_id': self._chat_id,
                          'text': text,
                          'parse_mode': 'HTML'},
                    timeout=10,
                )
            except Exception:
                pass  # never crash the trading engine due to Telegram issues
            self._q.task_done()

    def send(self, text: str):
        if not self._enabled:
            return
        try:
            self._q.put_nowait(text)
        except _queue.Full:
            pass  # drop if queue is full (shouldn't happen in normal use)

    def send_entry(self, inst_name: str, side: str, strike: int,
                   index_price: float, option_price: float,
                   qty: int, expiry: str, reason: str, time_str: str):
        emoji = '🟢' if side == 'CE' else '🔴'
        side_word = 'CALL (CE)' if side == 'CE' else 'PUT (PE)'
        self.send(
            f"{emoji} <b>ENTER — {inst_name}</b>\n"
            f"📋 {side_word}  Strike: <b>{strike}</b>\n"
            f"📈 Index: <b>{index_price:,.2f}</b>\n"
            f"💰 Premium: <b>₹{option_price:.2f}</b>  |  Qty: {qty}\n"
            f"📅 Expiry: {expiry}\n"
            f"⏰ {time_str}  |  {reason}"
        )

    def send_exit(self, inst_name: str, side: str, strike: int,
                  index_price: float, option_price: float,
                  pnl_prem: float, pnl_rupees: float,
                  qty: int, reason: str, time_str: str):
        pnl_emoji = '✅' if pnl_rupees >= 0 else '🛑'
        sgn = '+' if pnl_rupees >= 0 else ''
        reason_upper = reason.upper()
        if 'SL' in reason_upper or 'STOP' in reason_upper or 'ORB' in reason_upper and 'SL' in reason_upper:
            action_emoji = '🛑 SL HIT'
        elif 'REVERSE' in reason_upper:
            action_emoji = '🔄 REVERSE'
        else:
            action_emoji = '🏁 EXIT'
        self.send(
            f"{pnl_emoji} <b>{action_emoji} — {inst_name}</b>\n"
            f"📋 {'CALL (CE)' if side == 'CE' else 'PUT (PE)'}  Strike: <b>{strike}</b>\n"
            f"📉 Index: <b>{index_price:,.2f}</b>\n"
            f"💸 Exit Prem: <b>₹{option_price:.2f}</b>  |  Qty: {qty}\n"
            f"{'💚' if pnl_rupees >= 0 else '❤️'} P&L: <b>{sgn}₹{pnl_rupees:,.2f}</b> "
            f"({sgn}{pnl_prem:.2f} pts)\n"
            f"📌 Reason: {reason}\n"
            f"⏰ {time_str}"
        )


# Singleton alerter — initialised once, used by all engines
_tg_alerter: Optional['TelegramAlerter'] = None

def _get_alerter() -> 'TelegramAlerter':
    global _tg_alerter, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
    if _tg_alerter is None:
        _tg_alerter = TelegramAlerter(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID)
    return _tg_alerter


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
        self.option_tick_count: int = 0            # total WS option ticks received for active position
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
                self.last_option_tick_wc = time.time()
                self.option_tick_count += 1

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

        if not is_market_time(dt, self.instrument.get('exchange', 'NSE_EQ')):
            self.completed_3m.append({'bucket': bucket, 'open': o, 'high': h, 'low': l, 'close': c})
            # Do NOT feed Supertrend with pre/post-market candles — they pollute
            # the ATR calculation and cause values to diverge from TradingView.
            # ST is only updated with candles that fall within market hours.
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
        """Cache-only read — NEVER makes HTTP calls (lock may be held by strategy thread).
        Returns True if option_chain_map has usable data, False if empty.
        All HTTP fetching is done by poll_option_prices() in the background thread.
        """
        return bool(self.option_chain_map)

    def _resolve_option_snapshot_locked(self, side: str, price: float, force_refresh: bool = False) -> Optional[Dict[str, Any]]:
        if side not in ('CE', 'PE'):
            return None
        # option_chain_map is populated by poll_option_prices() background thread.
        # If empty, return None — bg thread will populate it and entry retries next candle.
        if not self.option_chain_map:
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
        # Phase 1: read state needed for the fetch (lock held briefly)
        with self.lock:
            if self.position is None or self.entry_strike is None:
                return
            strike   = self.entry_strike
            position = self.position
            ltp      = self.last_ltp or self.entry_price or 0.0
            expiry   = self.option_chain_expiry
            now_ts   = time.time()
            # Respect cooldown (rate-limit) and min interval without making HTTP call
            if now_ts < float(self.option_chain_cooldown_until):
                return
            if self.option_chain_map and (now_ts - float(self.option_chain_last_fetch) < self.option_chain_min_interval):
                # Cache is fresh enough — just read from it, no HTTP needed
                snap = self.option_chain_map.get((int(strike), str(position)))
                if snap:
                    self.current_option_price = float(snap['premium'])
                return

        # Phase 2: fetch expiry if needed (NO lock held — network I/O)
        if not expiry:
            expiry = fetch_option_expiry(self.instrument['security_id'], self.instrument['exchange'])
            if not expiry:
                return

        # Phase 3: fetch option chain (NO lock held — network I/O)
        chain = fetch_option_chain(self.instrument['security_id'], self.instrument['exchange'], expiry)

        # Phase 4: process result and update state (lock held briefly)
        with self.lock:
            if self.position is None:  # position may have closed during the fetch
                return
            if isinstance(chain, dict) and chain.get('_status_code'):
                sc = int(chain.get('_status_code', 0))
                if sc == 429:
                    self.option_chain_cooldown_until = time.time() + 8.0
                return
            if not chain:
                return
            oc = chain.get('oc') or {}
            now_ts2 = time.time()
            # Parse the freshly fetched chain
            for strike_key, node in oc.items():
                try:
                    sk = int(round(float(strike_key)))
                except Exception:
                    continue
                if not isinstance(node, dict):
                    continue
                for side in ('CE', 'PE'):
                    leg = node.get(side) or node.get(side.lower()) or {}
                    if not isinstance(leg, dict):
                        continue
                    premium = leg.get('last_price', leg.get('lastPrice', leg.get('ltp')))
                    secid   = leg.get('security_id', leg.get('securityId', leg.get('sid')))
                    if premium is None or secid in (None, ''):
                        continue
                    try:
                        prem_val = float(premium)
                        sid_str  = str(secid)
                    except Exception:
                        continue
                    lot_size = resolve_lot_size_from_master(sid_str, safe_int(self.instrument.get('default_lot_size'), 1))
                    self.option_chain_map[(sk, side)] = {
                        'strike': sk, 'side': side, 'premium': prem_val,
                        'security_id': sid_str, 'lot_size': int(lot_size),
                        'expiry': expiry,
                    }
            self.option_chain_last_fetch = now_ts2
            self.option_chain_expiry = expiry
            # Now update current_option_price from fresh cache
            snap = self.option_chain_map.get((int(self.entry_strike or 0), str(self.position or '')))
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
        # Fire Telegram alert (non-blocking)
        self._send_telegram_alert(action, side, strike, price, bucket, note)

    def _send_telegram_alert(self, action: str, side: str, strike: Optional[int],
                              price: float, bucket: int, note: str):
        """Called from _log_trade_locked — fires Telegram alert for ENTER/EXIT."""
        try:
            alerter = _get_alerter()
            if not alerter._enabled:
                return
            inst_name = self.instrument.get('name', self.instrument.get('key', '?'))
            time_str  = epoch_to_local_str(bucket, with_seconds=False)
            qty = int((self.entry_lot_size or 1) * max(1, int(self.entry_lots)))
            if action == 'ENTER':
                alerter.send_entry(
                    inst_name   = inst_name,
                    side        = side,
                    strike      = strike or 0,
                    index_price = price,
                    option_price= self.entry_option_price or 0.0,
                    qty         = qty,
                    expiry      = self.entry_expiry or '—',
                    reason      = note,
                    time_str    = time_str,
                )
            elif action == 'EXIT':
                # Parse pnl from note: format "reason | pnl=X.XX prem | ₹Y.YY"
                pnl_prem  = 0.0
                pnl_rupees = 0.0
                try:
                    if 'pnl=' in note:
                        pnl_part = note.split('pnl=')[1].split(' ')[0]
                        pnl_prem = float(pnl_part)
                    if '₹' in note:
                        rp_part = note.split('₹')[1].split('|')[0].strip()
                        pnl_rupees = float(rp_part)
                except Exception:
                    pass
                alerter.send_exit(
                    inst_name   = inst_name,
                    side        = side,
                    strike      = strike or 0,
                    index_price = price,
                    option_price= self.current_option_price or 0.0,
                    pnl_prem    = pnl_prem,
                    pnl_rupees  = pnl_rupees,
                    qty         = qty,
                    reason      = note.split(' | ')[0],   # just the reason part
                    time_str    = time_str,
                )
        except Exception:
            pass  # never let Telegram code crash the engine

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
        self.option_tick_count = 0
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
            # Before market open: next evaluation depends on exchange
            if phase == 'PREOPEN':
                exch = self.instrument.get('exchange', 'NSE_EQ')
                open_h, open_m = (9, 0) if exch == 'MCX_COMM' else (9, 15)
                dt = now_local().replace(hour=open_h, minute=open_m, second=0, microsecond=0)
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
            phase = current_market_phase(self.instrument.get('exchange', 'NSE_EQ'))
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
                'option_tick_count': self.option_tick_count,
                'last_option_tick_wc': self.last_option_tick_wc,
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
        # Always read from os.environ so GUI mode changes take effect at runtime
        mode = os.getenv('TRADE_MODE', 'NIFTY').strip().upper()
        if mode in TRADE_MODE_GROUPS:
            keys = TRADE_MODE_GROUPS[mode]
        elif mode in INSTRUMENTS:
            keys = [mode]
        else:
            valid = list(INSTRUMENTS.keys()) + list(TRADE_MODE_GROUPS.keys())
            raise SystemExit(f'TRADE_MODE={mode!r} is invalid. Valid options: {valid}')
        selected = [INSTRUMENTS[k] for k in keys]
        # Warn if any instrument has security_id still '0' (resolver failed)
        for inst in selected:
            if str(inst.get('security_id', '0')) == '0':
                print(f"[WARN] {inst['key']}: security_id is 0 — scrip master lookup failed. This instrument will not receive ticks.")
        return selected

    def on_open(self, ws):
        with self.stats_lock:
            self.last_ws_connect_time = int(time.time())
            self.last_ws_error = None

        # Reset subscribed_secids on every (re)connect so option contracts
        # get resubscribed — WS reconnect silently drops all option subscriptions.
        self.subscribed_secids = set(
            _engine_key(str(inst['security_id']), inst['exchange'])
            for inst in self.selected
        )

        # Underlying instruments + any live option positions
        instrument_list = [
            {'ExchangeSegment': inst['exchange'], 'SecurityId': str(inst['security_id'])}
            for inst in self.selected
        ]
        for inst in self.selected:
            eng = self.engines.get(_engine_key(inst['security_id'], inst['exchange']))
            if eng is None: continue
            with eng.lock:
                sid    = eng.entry_option_security_id
                fno_ex = inst.get('fno_exchange', 'NSE_FNO')
                has    = eng.position is not None
            if has and sid:
                instrument_list.append({'ExchangeSegment': fno_ex, 'SecurityId': str(sid)})
                self.subscribed_secids.add(_engine_key(str(sid), fno_ex))

        sub = {
            'RequestCode': REQ_SUB_TICKER,
            'InstrumentCount': len(instrument_list),
            'InstrumentList': instrument_list,
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
#  GUI — CustomTkinter  |  Two-page layout: CREDENTIALS  ↔  LIVE TRADING
# ══════════════════════════════════════════════════════════════════════════════
import tkinter as tk
import tkinter.messagebox as mb
from pathlib import Path

try:
    import customtkinter as ctk
except ImportError:
    import tkinter as _tk; _tk.Tk().withdraw()
    mb.showerror("Missing Package", "Run: pip install customtkinter")
    raise SystemExit(1)

try:
    import pyotp as _pyotp
except ImportError:
    _pyotp = None

if getattr(sys, 'frozen', False):
    BASE_DIR = Path(sys.executable).parent
else:
    BASE_DIR = Path(__file__).parent
ENV_PATH = BASE_DIR / '.env'

# ── palette ───────────────────────────────────────────────────────────────────
BG='#0d1117'; PANEL='#161b22'; DARK='#1c2128'; BORDER='#30363d'
ACCENT='#58a6ff'; GREEN='#3fb950'; RED='#f85149'
YELLOW='#d29922'; WHITE='#e6edf3'; MUTED='#8b949e'
MONO='Courier New'

def _fmt(v,p=2): return f"{v:,.{p}f}" if v is not None else '—'
def _pnl_col(v): return GREEN if (v or 0)>0 else (RED if (v or 0)<0 else WHITE)
def _sgn(v):     return '+' if (v or 0)>=0 else ''

def _save_env(**kw):
    ex={}
    try:
        for raw in ENV_PATH.read_text(encoding='utf-8').splitlines():
            s=raw.strip()
            if not s or s.startswith('#') or '=' not in s: continue
            k,v=s.split('=',1); ex[k.strip()]=v.strip()
    except Exception: pass
    ex.update(kw)
    ENV_PATH.write_text('\n'.join(f"{k}={v}" for k,v in ex.items())+'\n', encoding='utf-8')
    for k,v in kw.items(): os.environ[k]=v

def _creds_ok():
    c=os.getenv('DHAN_CLIENT_ID','').strip()
    t=os.getenv('DHAN_ACCESS_TOKEN','').strip()
    return bool(c and t and c!='__PLACEHOLDER__' and t!='__PLACEHOLDER__')

# ── dashboard table columns ────────────────────────────────────────────────────
COL_W={'name':130,'ltp':100,'chg':85,'orb_h':78,'orb_l':78,'st':80,
       'trend':65,'pos':160,'entry':68,'last_p':68,'unreal':95,'real':95,
       'opt_src':90,'note':170}
COLS=list(COL_W.keys())
COL_HDR={'name':'INSTRUMENT','ltp':'LTP','chg':'CHG%','orb_h':'ORB H',
         'orb_l':'ORB L','st':'ST','trend':'TREND','pos':'POSITION',
         'entry':'ENTRY P','last_p':'LAST P','unreal':'UNREAL ₹',
         'real':'REAL ₹','opt_src':'PREM SRC','note':'STATUS'}


# ══════════════════════════════════════════════════════════════════════════════
#  DASHBOARD TABLE ROW
# ══════════════════════════════════════════════════════════════════════════════
class DashboardRow:
    def __init__(self, parent, inst, idx):
        self.inst=inst
        bg=DARK if idx%2==0 else PANEL
        self.frame=ctk.CTkFrame(parent, fg_color=bg, corner_radius=0, height=32)
        self.frame.pack(fill='x')
        self.frame.pack_propagate(False)
        self.cells={}
        for col in COLS:
            anch='w' if col in ('name','pos','note','trend') else 'e'
            lbl=ctk.CTkLabel(self.frame, text='—', width=COL_W[col], anchor=anch,
                font=ctk.CTkFont(MONO,11), text_color=WHITE)
            lbl.pack(side='left', padx=2)
            self.cells[col]=lbl
        self.cells['name'].configure(text=inst['name'], text_color=ACCENT)

    def update(self, snap):
        inst=snap['instrument']; ltp=snap['last_ltp']; prev=snap['prev_close']
        if ltp is not None:
            self.cells['ltp'].configure(text=f"{ltp:,.2f}", text_color=WHITE)
        if ltp is not None and prev:
            chg=(ltp-prev)/prev*100
            col=GREEN if chg>=0 else RED
            self.cells['chg'].configure(text=f"{_sgn(chg)}{chg:.2f}%", text_color=col)
        orb_rdy=snap['orb_ready']
        self.cells['orb_h'].configure(text=_fmt(snap['orb_high']),
            text_color=GREEN if orb_rdy else MUTED)
        self.cells['orb_l'].configure(text=_fmt(snap['orb_low']),
            text_color=RED if orb_rdy else MUTED)
        self.cells['st'].configure(text=_fmt(snap['st_value']))
        st_dir=snap['st_dir']
        if st_dir is not None:
            up=int(st_dir)>0
            self.cells['trend'].configure(text='▲ UP' if up else '▼ DN',
                text_color=GREEN if up else RED)
        else:
            self.cells['trend'].configure(text='—', text_color=MUTED)
        pos=snap['position']
        if pos:
            self.cells['pos'].configure(
                text=f"{inst['option_prefix']} {pos} {snap['entry_strike']}",
                text_color=GREEN if pos=='CE' else RED)
            self.cells['entry'].configure(text=_fmt(snap.get('entry_option_price')), text_color=MUTED)
            self.cells['last_p'].configure(text=_fmt(snap.get('current_option_price')), text_color=WHITE)
            ur=snap.get('unrealized_pnl_rupees') or 0.0
            rr=snap.get('realized_pnl_rupees') or 0.0
            self.cells['unreal'].configure(text=f"{_sgn(ur)}₹{ur:,.0f}", text_color=_pnl_col(ur))
            self.cells['real'].configure(text=f"{_sgn(rr)}₹{rr:,.0f}", text_color=_pnl_col(rr))
        else:
            self.cells['pos'].configure(text='No position', text_color=MUTED)
            for c in ('entry','last_p','unreal'): self.cells[c].configure(text='—', text_color=MUTED)
            rr=snap.get('realized_pnl_rupees') or 0.0
            self.cells['real'].configure(
                text=f"{_sgn(rr)}₹{rr:,.0f}" if rr else '—', text_color=_pnl_col(rr))
        self.cells['note'].configure(text=(snap.get('note') or '')[:28], text_color=YELLOW)

        # Option WS tick counter
        pos = snap.get('position')
        if pos:
            ticks = snap.get('option_tick_count', 0)
            wc    = snap.get('last_option_tick_wc', 0.0)
            age   = int(time.time() - wc) if wc else 0
            if ticks == 0:
                src_txt = 'WS: pending'
                src_col = YELLOW
            else:
                src_txt = f'WS {ticks}t ({age}s ago)'
                src_col = GREEN
            self.cells['opt_src'].configure(text=src_txt, text_color=src_col)
        else:
            self.cells['opt_src'].configure(text='—', text_color=MUTED)


# ══════════════════════════════════════════════════════════════════════════════
#  MAIN WINDOW  —  two pages via a segmented button switcher in the top bar
# ══════════════════════════════════════════════════════════════════════════════
class MainWindow(ctk.CTk):
    def __init__(self):
        super().__init__()
        self.title('Dhan ORB + Supertrend Paper Trader  |  Balfund Trading')
        self.geometry('1400x860')
        self.minsize(1100,640)
        self.configure(fg_color=BG)
        ctk.set_appearance_mode('dark')
        ctk.set_default_color_theme('blue')

        self._app=None; self._running=False; self._rows={}

        self._build_topbar()
        # page frames
        self.page_creds   = ctk.CTkFrame(self, fg_color=BG, corner_radius=0)
        self.page_trading = ctk.CTkFrame(self, fg_color=BG, corner_radius=0)
        self._build_creds_page()
        self._build_trading_page()
        self._build_statusbar()
        self._show_page('credentials' if not _creds_ok() else 'trading')
        self.protocol('WM_DELETE_WINDOW', self._on_close)
        self._tick()

    # ── top bar ──────────────────────────────────────────────────────────────
    def _build_topbar(self):
        bar=ctk.CTkFrame(self, fg_color=PANEL, corner_radius=0, height=54)
        bar.pack(fill='x')
        bar.pack_propagate(False)

        ctk.CTkLabel(bar, text='◈  DHAN ORB PAPER TRADER',
            font=ctk.CTkFont(MONO,15,'bold'), text_color=ACCENT).pack(side='left', padx=16)

        self.lbl_clock=ctk.CTkLabel(bar, text='',
            font=ctk.CTkFont(MONO,12), text_color=MUTED)
        self.lbl_clock.pack(side='left', padx=10)

        # page switcher
        self.page_seg=ctk.CTkSegmentedButton(bar,
            values=['⚙  Credentials','📊  Live Trading'],
            font=ctk.CTkFont(MONO,12),
            fg_color=DARK, selected_color=ACCENT, selected_hover_color='#79c0ff',
            unselected_color=DARK, unselected_hover_color=BORDER,
            text_color=WHITE, text_color_disabled=MUTED,
            command=self._on_page_switch)
        self.page_seg.pack(side='left', padx=20, pady=10)

        # start/stop (right side)
        self.btn_start=ctk.CTkButton(bar, text='▶  START', width=130,
            font=ctk.CTkFont(MONO,13,'bold'),
            fg_color=GREEN, text_color=BG, hover_color='#56d364',
            command=self._toggle)
        self.btn_start.pack(side='right', padx=8, pady=9)

        all_modes=list(INSTRUMENTS.keys())+list(TRADE_MODE_GROUPS.keys())
        self.mode_var=ctk.StringVar(value=os.getenv('TRADE_MODE','NIFTY'))
        ctk.CTkOptionMenu(bar, values=all_modes, variable=self.mode_var,
            width=180, font=ctk.CTkFont(MONO,12),
            fg_color=DARK, button_color=BORDER,
            dropdown_fg_color=PANEL, text_color=WHITE).pack(side='right', padx=4, pady=9)
        ctk.CTkLabel(bar, text='MODE:', font=ctk.CTkFont(MONO,11),
            text_color=MUTED).pack(side='right', padx=(8,0))

    def _on_page_switch(self, val):
        if 'Credentials' in val: self._show_page('credentials')
        else:                      self._show_page('trading')

    def _show_page(self, name):
        self.page_creds.pack_forget()
        self.page_trading.pack_forget()
        if name=='credentials':
            self.page_creds.pack(fill='both', expand=True)
            self.page_seg.set('⚙  Credentials')
        else:
            self.page_trading.pack(fill='both', expand=True)
            self.page_seg.set('📊  Live Trading')

    # ══════════════════════════════════════════════════════════════════════════
    #  CREDENTIALS PAGE
    # ══════════════════════════════════════════════════════════════════════════
    def _build_creds_page(self):
        # Wrap everything in a scrollable frame so Save button is always reachable
        scroll = ctk.CTkScrollableFrame(self.page_creds, fg_color=BG, corner_radius=0)
        scroll.pack(fill='both', expand=True)
        p = scroll

        ctk.CTkLabel(p, text='Dhan API Credentials',
            font=ctk.CTkFont(MONO,18,'bold'), text_color=ACCENT).pack(pady=(32,4))
        ctk.CTkLabel(p, text='web.dhan.co  →  Profile  →  API Access',
            font=ctk.CTkFont(MONO,12), text_color=MUTED).pack(pady=(0,20))

        frm=ctk.CTkFrame(p, fg_color=PANEL, corner_radius=12, width=540)
        frm.pack(pady=0)
        frm.pack_propagate(False)

        def _row(label, env_key, show='', w=320):
            r=ctk.CTkFrame(frm, fg_color='transparent')
            r.pack(fill='x', padx=20, pady=7)
            ctk.CTkLabel(r, text=label, width=160, anchor='w',
                font=ctk.CTkFont(MONO,12), text_color=MUTED).pack(side='left')
            e=ctk.CTkEntry(r, show=show, width=w,
                font=ctk.CTkFont(MONO,12),
                fg_color=DARK, border_color=BORDER, text_color=WHITE)
            val=os.getenv(env_key,'')
            if val and val not in ('__PLACEHOLDER__',): e.insert(0,val)
            e.pack(side='left')
            return e

        self.e_cid  = _row('Client ID',       'DHAN_CLIENT_ID')
        self.e_pin  = _row('PIN (4-digit)',    'DHAN_PIN',       '•')
        self.e_totp = _row('TOTP Secret',      'DHAN_TOTP_SECRET','•')

        # Token row with inline Generate button
        tr=ctk.CTkFrame(frm, fg_color='transparent')
        tr.pack(fill='x', padx=20, pady=7)
        ctk.CTkLabel(tr, text='Access Token', width=160, anchor='w',
            font=ctk.CTkFont(MONO,12), text_color=MUTED).pack(side='left')
        self.e_tok=ctk.CTkEntry(tr, show='•', width=220,
            font=ctk.CTkFont(MONO,12),
            fg_color=DARK, border_color=BORDER, text_color=WHITE)
        val=os.getenv('DHAN_ACCESS_TOKEN','')
        if val and val not in ('__PLACEHOLDER__',): self.e_tok.insert(0,val)
        self.e_tok.pack(side='left')
        self.btn_gen=ctk.CTkButton(tr, text='⚡ Generate', width=96,
            font=ctk.CTkFont(MONO,11,'bold'),
            fg_color=YELLOW, text_color=BG, hover_color='#e3b341',
            command=self._gen_token)
        self.btn_gen.pack(side='left', padx=(6,0))

        # Status + info
        self.lbl_cred_status=ctk.CTkLabel(p, text='',
            font=ctk.CTkFont(MONO,12), text_color=MUTED)
        self.lbl_cred_status.pack(pady=(12,0))

        self.lbl_cred_err=ctk.CTkLabel(p, text='',
            font=ctk.CTkFont(MONO,11), text_color=RED)
        self.lbl_cred_err.pack(pady=(2,0))

        self.cred_info=ctk.CTkTextbox(p, height=52, width=540,
            fg_color=DARK, border_color=BORDER,
            font=ctk.CTkFont(MONO,10), text_color=MUTED, state='disabled')
        self.cred_info.pack(pady=(8,0))

        # Telegram section
        ctk.CTkLabel(p, text='── Telegram Alerts (optional) ──',
            font=ctk.CTkFont(MONO,11), text_color=MUTED).pack(pady=(14,4))

        tg_frm=ctk.CTkFrame(p, fg_color=PANEL, corner_radius=12, width=540)
        tg_frm.pack()
        tg_frm.pack_propagate(False)

        def _tg_row(label, env_key, show='', w=320):
            r=ctk.CTkFrame(tg_frm, fg_color='transparent')
            r.pack(fill='x', padx=20, pady=6)
            ctk.CTkLabel(r, text=label, width=160, anchor='w',
                font=ctk.CTkFont(MONO,12), text_color=MUTED).pack(side='left')
            e=ctk.CTkEntry(r, show=show, width=w,
                font=ctk.CTkFont(MONO,12),
                fg_color=DARK, border_color=BORDER, text_color=WHITE)
            val=os.getenv(env_key,'')
            if val: e.insert(0,val)
            e.pack(side='left')
            return e

        self.e_tg_token = _tg_row('Bot Token',  'TELEGRAM_BOT_TOKEN', '•')
        self.e_tg_chat  = _tg_row('Chat ID',    'TELEGRAM_CHAT_ID')

        ctk.CTkLabel(p,
            text='Get token: @BotFather → /newbot    |    Get Chat ID: message bot then check api.telegram.org/bot<TOKEN>/getUpdates',
            font=ctk.CTkFont(MONO,9), text_color=MUTED).pack(pady=(4,0))

        self.btn_tg_test = ctk.CTkButton(p, text='📨 Send Test Alert', width=180,
            font=ctk.CTkFont(MONO,11,'bold'),
            fg_color=BORDER, hover_color=DARK, text_color=WHITE,
            command=self._test_telegram)
        self.btn_tg_test.pack(pady=(8,0))

        self.lbl_tg_status = ctk.CTkLabel(p, text='',
            font=ctk.CTkFont(MONO,11), text_color=MUTED)
        self.lbl_tg_status.pack(pady=(4,0))

        # Save button
        ctk.CTkButton(p, text='💾  Save Credentials', height=40, width=240,
            font=ctk.CTkFont(MONO,13,'bold'),
            fg_color=ACCENT, text_color=BG, hover_color='#79c0ff',
            command=self._save_creds).pack(pady=20)

        # Token validity badge
        self.lbl_tok_badge=ctk.CTkLabel(p, text='',
            font=ctk.CTkFont(MONO,12,'bold'), text_color=MUTED)
        self.lbl_tok_badge.pack()
        self._refresh_token_badge()

    def _refresh_token_badge(self):
        if _creds_ok():
            self.lbl_tok_badge.configure(
                text='✅  Token saved — ready to trade', text_color=GREEN)
        else:
            self.lbl_tok_badge.configure(
                text='⚠  No valid token — generate or paste one above', text_color=YELLOW)

    def _set_cred_info(self, text):
        self.cred_info.configure(state='normal')
        self.cred_info.delete('1.0','end')
        self.cred_info.insert('end', text)
        self.cred_info.configure(state='disabled')

    def _gen_token(self):
        if _pyotp is None:
            self.lbl_cred_err.configure(text='pyotp not installed — run: pip install pyotp')
            return
        cid   = self.e_cid.get().strip()
        pin   = self.e_pin.get().strip()
        totp_s= self.e_totp.get().strip()
        if not cid:
            self.lbl_cred_err.configure(text='Client ID required'); return
        if not pin or not totp_s:
            self.lbl_cred_err.configure(text='PIN + TOTP Secret required'); return
        self.lbl_cred_err.configure(text='')
        self.btn_gen.configure(state='disabled', text='⏳…')
        self.lbl_cred_status.configure(text='Generating token…', text_color=YELLOW)

        def _do():
            try:
                code=_pyotp.TOTP(totp_s).now()
                url=(f"https://auth.dhan.co/app/generateAccessToken"
                     f"?dhanClientId={cid}&pin={pin}&totp={code}")
                r=requests.post(url, timeout=15)
                data=r.json()
                if 'accessToken' in data:
                    tok=data['accessToken']; exp=data.get('expiryTime','')
                    name=data.get('dhanClientName','')
                    def _update():
                        self.e_tok.delete(0,'end'); self.e_tok.insert(0,tok)
                        self.lbl_cred_status.configure(
                            text='✅ Token generated!', text_color=GREEN)
                        self.lbl_cred_err.configure(text='')
                        self._set_cred_info(f"Client: {name}\nExpiry: {exp}\nTOTP:   {code}")
                        self.btn_gen.configure(state='normal', text='⚡ Generate')
                    self.after(0, _update)
                else:
                    err=str(data)
                    self.after(0, lambda: [
                        self.lbl_cred_err.configure(text=f"Failed: {err[:80]}"),
                        self.lbl_cred_status.configure(text='', text_color=MUTED),
                        self.btn_gen.configure(state='normal', text='⚡ Generate')])
            except Exception as e:
                msg=str(e)
                self.after(0, lambda: [
                    self.lbl_cred_err.configure(text=f"Error: {msg[:80]}"),
                    self.lbl_cred_status.configure(text='', text_color=MUTED),
                    self.btn_gen.configure(state='normal', text='⚡ Generate')])
        threading.Thread(target=_do, daemon=True).start()

    def _test_telegram(self):
        token = self.e_tg_token.get().strip()
        chat  = self.e_tg_chat.get().strip()
        if not token or not chat:
            self.lbl_tg_status.configure(
                text='Enter Bot Token and Chat ID first', text_color=YELLOW); return
        self.btn_tg_test.configure(state='disabled', text='Sending…')
        def _do():
            try:
                r = requests.post(
                    f'https://api.telegram.org/bot{token}/sendMessage',
                    json={'chat_id': chat,
                          'text': '✅ <b>Dhan ORB Trader</b>\n\nTelegram alerts are working!\n🟢 ENTER — NIFTY CE 24200 @ ₹84.00\n(This is a test message)',
                          'parse_mode': 'HTML'},
                    timeout=10)
                data = r.json()
                if data.get('ok'):
                    self.after(0, lambda: [
                        self.lbl_tg_status.configure(text='✅ Test message sent!', text_color=GREEN),
                        self.btn_tg_test.configure(state='normal', text='📨 Send Test Alert')])
                else:
                    err = data.get('description','Unknown error')
                    self.after(0, lambda: [
                        self.lbl_tg_status.configure(text=f'❌ {err}', text_color=RED),
                        self.btn_tg_test.configure(state='normal', text='📨 Send Test Alert')])
            except Exception as e:
                msg=str(e)
                self.after(0, lambda: [
                    self.lbl_tg_status.configure(text=f'❌ {msg[:60]}', text_color=RED),
                    self.btn_tg_test.configure(state='normal', text='📨 Send Test Alert')])
        threading.Thread(target=_do, daemon=True).start()

    def _save_creds(self):
        cid =self.e_cid.get().strip()
        tok =self.e_tok.get().strip()
        pin =self.e_pin.get().strip()
        totp=self.e_totp.get().strip()
        tg_token = self.e_tg_token.get().strip()
        tg_chat  = self.e_tg_chat.get().strip()
        if not cid:
            self.lbl_cred_err.configure(text='Client ID required'); return
        if not tok:
            self.lbl_cred_err.configure(
                text='Access Token required — click ⚡ Generate or paste it'); return
        kw = dict(DHAN_CLIENT_ID=cid, DHAN_ACCESS_TOKEN=tok,
                  DHAN_PIN=pin, DHAN_TOTP_SECRET=totp)
        if tg_token: kw['TELEGRAM_BOT_TOKEN'] = tg_token
        if tg_chat:  kw['TELEGRAM_CHAT_ID']   = tg_chat
        _save_env(**kw)
        # Refresh global Telegram alerter with new credentials
        global TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, _tg_alerter
        TELEGRAM_BOT_TOKEN = tg_token
        TELEGRAM_CHAT_ID   = tg_chat
        _tg_alerter = None  # reset so it's rebuilt with new creds on next alert
        _load_dotenv_fallback(str(ENV_PATH))
        self.lbl_cred_status.configure(
            text='✅ Saved! Switch to Live Trading to start.', text_color=GREEN)
        self.lbl_cred_err.configure(text='')
        self._refresh_token_badge()

    # ══════════════════════════════════════════════════════════════════════════
    #  LIVE TRADING PAGE
    # ══════════════════════════════════════════════════════════════════════════
    def _build_trading_page(self):
        p=self.page_trading

        # ── instrument table ─────────────────────────────────────────────────
        tbl_outer=ctk.CTkFrame(p, fg_color=PANEL, corner_radius=8)
        tbl_outer.pack(fill='x', padx=8, pady=(8,2))

        hdr=ctk.CTkFrame(tbl_outer, fg_color=DARK, corner_radius=0, height=26)
        hdr.pack(fill='x'); hdr.pack_propagate(False)
        for col in COLS:
            anch='w' if col in ('name','pos','note','trend') else 'e'
            ctk.CTkLabel(hdr, text=COL_HDR[col], width=COL_W[col], anchor=anch,
                font=ctk.CTkFont(MONO,9,'bold'), text_color=MUTED).pack(side='left', padx=2)

        self.table_scroll=ctk.CTkScrollableFrame(
            tbl_outer, fg_color='transparent', corner_radius=0, height=280)
        self.table_scroll.pack(fill='x')

        # ── bottom split ──────────────────────────────────────────────────────
        bot=ctk.CTkFrame(p, fg_color='transparent')
        bot.pack(fill='both', expand=True, padx=8, pady=4)

        # trade log (left)
        log_frm=ctk.CTkFrame(bot, fg_color=PANEL, corner_radius=8)
        log_frm.pack(side='left', fill='both', expand=True, padx=(0,4))
        ctk.CTkLabel(log_frm, text='Trade Log — All Instruments',
            font=ctk.CTkFont(MONO,11,'bold'), text_color=ACCENT, anchor='w').pack(
            fill='x', padx=10, pady=(6,2))
        self.log_box=ctk.CTkTextbox(log_frm, fg_color=DARK, border_color=BORDER,
            font=ctk.CTkFont(MONO,11), text_color=WHITE, state='disabled')
        self.log_box.pack(fill='both', expand=True, padx=8, pady=(0,8))

        # forming candles (right)
        fc_frm=ctk.CTkFrame(bot, fg_color=PANEL, corner_radius=8, width=500)
        fc_frm.pack(side='right', fill='y', padx=(4,0))
        fc_frm.pack_propagate(False)
        ctk.CTkLabel(fc_frm, text='Forming 3m Candles',
            font=ctk.CTkFont(MONO,11,'bold'), text_color=ACCENT, anchor='w').pack(
            fill='x', padx=10, pady=(6,2))
        self.forming_box=ctk.CTkTextbox(fc_frm, fg_color=DARK, border_color=BORDER,
            font=ctk.CTkFont(MONO,11), text_color=WHITE, state='disabled')
        self.forming_box.pack(fill='both', expand=True, padx=8, pady=(0,8))

    def _build_rows(self, selected):
        for w in self.table_scroll.winfo_children(): w.destroy()
        self._rows.clear()
        for i, inst in enumerate(selected):
            row=DashboardRow(self.table_scroll, inst, i)
            self._rows[_engine_key(inst['security_id'], inst['exchange'])]=row

    # ── status bar ────────────────────────────────────────────────────────────
    def _build_statusbar(self):
        bar=ctk.CTkFrame(self, fg_color=PANEL, corner_radius=0, height=28)
        bar.pack(fill='x', side='bottom'); bar.pack_propagate(False)
        self.lbl_ws=ctk.CTkLabel(bar, text='● WS: offline',
            font=ctk.CTkFont(MONO,11), text_color=RED)
        self.lbl_ws.pack(side='left', padx=12, pady=4)
        self.lbl_pkts=ctk.CTkLabel(bar, text='Packets T/P/O/D: 0/0/0/0',
            font=ctk.CTkFont(MONO,11), text_color=MUTED)
        self.lbl_pkts.pack(side='left', padx=12)
        self.lbl_err=ctk.CTkLabel(bar, text='',
            font=ctk.CTkFont(MONO,11), text_color=RED)
        self.lbl_err.pack(side='left', padx=8)
        self.lbl_phase=ctk.CTkLabel(bar, text='',
            font=ctk.CTkFont(MONO,11), text_color=YELLOW)
        self.lbl_phase.pack(side='right', padx=12)

    # ── start / stop ──────────────────────────────────────────────────────────
    def _toggle(self):
        if self._running: self._stop()
        else:             self._start()

    def _start(self):
        if not _creds_ok():
            self._show_page('credentials')
            self.lbl_cred_err.configure(
                text='Generate or paste your Access Token first, then Save.')
            return
        mode=self.mode_var.get().strip().upper()
        os.environ['TRADE_MODE']=mode
        _save_env(TRADE_MODE=mode)
        self.btn_start.configure(text='⏳ Starting…', state='disabled',
            fg_color=YELLOW, text_color=BG)
        self.lbl_err.configure(text='')
        self.update()

        def _init():
            try:
                _load_dotenv_fallback(str(ENV_PATH))
                global DHAN_CLIENT_ID, DHAN_ACCESS_TOKEN, WS_URL
                DHAN_CLIENT_ID    = os.getenv('DHAN_CLIENT_ID','').strip()
                DHAN_ACCESS_TOKEN = os.getenv('DHAN_ACCESS_TOKEN','').strip()
                WS_URL=(f"wss://api-feed.dhan.co?version=2"
                        f"&token={DHAN_ACCESS_TOKEN}"
                        f"&clientId={DHAN_CLIENT_ID}&authType=2")
                app=App()
                app.bootstrap()
                self._app=app; self._running=True
                self.after(0, lambda: self._build_rows(app.selected))
                self.after(0, lambda: self._show_page('trading'))
                self.after(0, lambda: self.btn_start.configure(
                    text='◼  STOP', state='normal',
                    fg_color=RED, text_color=WHITE))
                t=threading.Thread(target=app.run_ws_loop, daemon=True)
                t.start(); app.ws_thread=t
                # Start background option polling thread (off main tkinter thread)
                bg=threading.Thread(target=self._start_bg_loop, daemon=True)
                bg.start()
            except Exception as e:
                msg=str(e)
                self._running=False
                self.after(0, lambda: [
                    self.btn_start.configure(text='▶  START', state='normal',
                        fg_color=GREEN, text_color=BG),
                    self.lbl_err.configure(text=f"Error: {msg[:100]}")])
        threading.Thread(target=_init, daemon=True).start()

    def _stop(self):
        if self._app: self._app.stop(); self._app=None
        self._running=False
        self.btn_start.configure(text='▶  START',
            fg_color=GREEN, text_color=BG, state='normal')
        self.lbl_ws.configure(text='● WS: offline', text_color=RED)

    def _start_bg_loop(self):
        """Background thread: option subscriptions + fallback polling.
        Kept off the main tkinter thread so GUI never blocks on network I/O."""
        while self._running:
            try:
                if self._app:
                    self._app.process_option_subscriptions()
                    now_ts = time.time()
                    for inst in self._app.selected:
                        key = _engine_key(inst['security_id'], inst['exchange'])
                        eng = self._app.engines.get(key)
                        if eng is None: continue
                        with eng.lock:
                            wc     = eng.last_option_tick_wc
                            has    = eng.position is not None
                            sid    = eng.entry_option_security_id
                            fno_ex = inst.get('fno_exchange', 'NSE_FNO')
                        if has:
                            ws_age = now_ts - wc
                            # WS-only premium tracking — no REST fallback.
                            # If no tick arrived, price simply hasn't changed (valid for
                            # illiquid stock options that can be silent for minutes).
                            # Only re-queue WS subscription if silent for 5 minutes —
                            # that indicates a genuine subscription loss, not just
                            # a quiet market.
                            if ws_age > 300.0 and sid:
                                self._app.subscribed_secids.discard(
                                    _engine_key(str(sid), fno_ex))
                                with eng.lock:
                                    eng._pending_option_sub_req = {
                                        'security_id': str(sid),
                                        'exchange': fno_ex,
                                    }
            except Exception:
                pass
            time.sleep(1.0)

    # ── 1-second GUI tick (pure GUI work — no API calls) ─────────────────────
    def _tick(self):
        try:
            self.lbl_clock.configure(
                text=now_local().strftime('%Y-%m-%d  %H:%M:%S'))

            if self._app and self._running:
                with self._app.stats_lock:
                    pc  = dict(self._app.packet_counts)
                    err = self._app.last_ws_error
                    ct  = self._app.last_ws_connect_time

                self.lbl_ws.configure(
                    text=f"● WS: online  {int(time.time()-ct)}s" if ct else '● WS: connecting…',
                    text_color=GREEN if ct else YELLOW)
                self.lbl_pkts.configure(
                    text=f"Packets T/P/O/D: {pc['ticker']}/{pc['prev_close']}/{pc['other']}/{pc['disconnect']}")
                self.lbl_err.configure(text=f"WS: {err}" if err else '')

                phase_map = {'PREOPEN':'Pre-open','ORB_WAIT':'ORB window (09:18–09:24)',
                             'PRE10':'Pre-10 ORB mode','POST10':'Post-10 ST mode',
                             'POSTMARKET':'Market closed'}
                self.lbl_phase.configure(text=phase_map.get(current_market_phase(),''))

                # Collect snapshots (no API calls — pure in-memory reads)
                snaps = []
                for inst in self._app.selected:
                    key = _engine_key(inst['security_id'], inst['exchange'])
                    eng = self._app.engines.get(key)
                    if eng is None: continue
                    snap = eng.snapshot()
                    snaps.append((key, snap))
                    row = self._rows.get(key)
                    if row: row.update(snap)

                # Trade log — refresh every 3 ticks to reduce CPU
                self._tick_count = getattr(self, '_tick_count', 0) + 1
                if self._tick_count % 3 == 0:
                    lines = []
                    for _, s in snaps:
                        nm = s['instrument']['name']
                        for r in s['trade_log'][:5]:
                            lines.append(f"[{nm}]  {r}")
                    lines.sort(reverse=True)
                    self.log_box.configure(state='normal')
                    self.log_box.delete('1.0','end')
                    self.log_box.insert('end', '\n'.join(lines) if lines else 'No trades yet.')
                    self.log_box.configure(state='disabled')

                    fc_lines = []
                    for _, s in snaps:
                        nm  = s['instrument']['name']
                        c3  = s['current_3m']
                        nxt = s.get('next_eval') or '—'
                        if c3:
                            t = epoch_to_local_str(c3['bucket'], False)
                            fc_lines.append(
                                f"{nm:<18} [{t}] O:{c3['open']:.1f} H:{c3['high']:.1f} "
                                f"L:{c3['low']:.1f} C:{c3['close']:.1f} "
                                f"{c3.get('parts',0)}/3  next:{nxt}")
                        else:
                            fc_lines.append(f"{nm:<18} awaiting…  next:{nxt}")
                    self.forming_box.configure(state='normal')
                    self.forming_box.delete('1.0','end')
                    self.forming_box.insert('end', '\n'.join(fc_lines) if fc_lines else '—')
                    self.forming_box.configure(state='disabled')
            else:
                self.lbl_ws.configure(text='● WS: offline', text_color=RED)
        except Exception:
            pass
        self.after(1000, self._tick)

    def _on_close(self):
        self._stop(); self.destroy()


# ══════════════════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════
def main():
    if getattr(sys,'frozen',False):
        _log=Path(sys.executable).parent/'dhan_orb_crash.log'
    else:
        _log=Path(__file__).parent/'dhan_orb_crash.log'
    try:
        _load_dotenv_fallback(str(ENV_PATH))
        win=MainWindow()
        win.mainloop()
    except Exception as _e:
        import traceback as _tb
        crash=_tb.format_exc()
        try: _log.write_text(crash, encoding='utf-8')
        except Exception: pass
        try:
            _r=tk.Tk(); _r.withdraw()
            mb.showerror('Crash', f"{_e}\n\nSee: {_log}"); _r.destroy()
        except Exception: pass
        sys.exit(1)


if __name__=='__main__':
    main()
