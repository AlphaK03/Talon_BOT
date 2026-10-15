# -*- mode: python ; coding: utf-8 -*-


a = Analysis(
    ['D:/CHC_CONSULTORES/Repositorios/Talon_BOT/v2/src/main.py'],
    pathex=['D:/CHC_CONSULTORES/Repositorios/Talon_BOT/v2/src'],
    binaries=[],
    datas=[('D:/CHC_CONSULTORES/Repositorios/Talon_BOT/v2/Names_TicaPortal.xlsx', '.')],
    hiddenimports=[
        'playwright',
        'playwright.async_api',
        'playwright.sync_api',
        'playwright._impl._api_types',
        'playwright._impl._browser_context',
        'playwright._impl._browser',
        'playwright._impl._page',
    ],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
    optimize=0,
)
pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.datas,
    [],
    name='TalonBot_v5.4',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=False,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)
