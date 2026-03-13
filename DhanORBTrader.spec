# -*- mode: python ; coding: utf-8 -*-
from PyInstaller.utils.hooks import collect_data_files, collect_submodules
a = Analysis(['dhan_orb_trader.py'], pathex=[], binaries=[],
    datas=collect_data_files('customtkinter'),
    hiddenimports=(collect_submodules('customtkinter') + [
        'websocket','websocket._core','websocket._abnf','websocket._exceptions',
        'requests','pyotp','tkinter','tkinter.messagebox',
        'csv','struct','json','threading','collections']),
    hookspath=[], runtime_hooks=[], excludes=[], noarchive=False)
pyz = PYZ(a.pure)
exe = EXE(pyz, a.scripts, a.binaries, a.datas, [],
    name='DhanORBTrader', debug=False, strip=False, upx=True,
    console=True, runtime_tmpdir=None)
