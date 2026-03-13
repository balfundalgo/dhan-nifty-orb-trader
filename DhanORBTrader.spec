# -*- mode: python ; coding: utf-8 -*-
# DhanORBTrader.spec

a = Analysis(
    ['nifty_papertrade_orb_supertrend_OPTIONS_v9_ws_optionticks.py'],
    pathex=[],
    binaries=[],
    datas=[],
    hiddenimports=[
        'websocket',
        'websocket._core',
        'websocket._abnf',
        'websocket._exceptions',
        'requests',
        'pyotp',
        'csv',
        'struct',
        'json',
        'threading',
        'collections',
    ],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
)

pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.datas,
    [],
    name='DhanORBTrader',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=True,      # console app — shows terminal output
    disable_windowed_traceback=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)
