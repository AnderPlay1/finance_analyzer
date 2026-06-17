@echo off
chcp 65001 >nul
setlocal

cd /d "%~dp0" || (
    echo ERROR: cannot enter project directory.>&2
    exit /b 1
)

if not defined MYSQL_CMD set "MYSQL_CMD=mysql"
if not defined PYTHON_CMD set "PYTHON_CMD=python"

if not defined DB_HOST set "DB_HOST=127.0.0.1"
if not defined DB_PORT set "DB_PORT=3306"
if not defined DB_NAME set "DB_NAME=finance_analyzer"
if not defined DB_USER set "DB_USER=root"
if not defined APP_HOST set "APP_HOST=127.0.0.1"
if not defined APP_PORT set "APP_PORT=5000"
set "APP_URL=http://%APP_HOST%:%APP_PORT%/"

set "PYTHONUTF8=1"

echo(%DB_NAME%| findstr /r /x "[A-Za-z0-9_][A-Za-z0-9_]*" >nul
if errorlevel 1 (
    echo ERROR: DB_NAME may contain only latin letters, digits and underscore.>&2
    exit /b 1
)

if exist "%MYSQL_CMD%" goto mysql_found
where "%MYSQL_CMD%" >nul 2>&1
if not errorlevel 1 goto mysql_found

for /d %%D in ("C:\Program Files\MySQL\MySQL Server *") do (
    if exist "%%D\bin\mysql.exe" (
        set "MYSQL_CMD=%%D\bin\mysql.exe"
        goto mysql_found
    )
)

for /d %%D in ("C:\Program Files (x86)\MySQL\MySQL Server *") do (
    if exist "%%D\bin\mysql.exe" (
        set "MYSQL_CMD=%%D\bin\mysql.exe"
        goto mysql_found
    )
)

for /f "delims=" %%F in ('where /r "C:\Program Files\MySQL" mysql.exe 2^>nul') do (
    set "MYSQL_CMD=%%F"
    goto mysql_found
)

echo ERROR: mysql client was not found.>&2
echo MYSQL_CMD=%MYSQL_CMD%>&2
echo Install MySQL Client or set MYSQL_CMD to the full path of mysql.exe.>&2
echo Try in PowerShell: Get-ChildItem "C:\Program Files\MySQL" -Recurse -Filter mysql.exe>&2
exit /b 1

:mysql_found
echo Using MySQL client: %MYSQL_CMD%

"%PYTHON_CMD%" -c "import pymysql" >nul 2>&1
if errorlevel 1 (
    echo ERROR: Python package pymysql was not found.>&2
    echo Run: "%PYTHON_CMD%" -m pip install pymysql>&2
    exit /b 1
)

echo Creating MySQL database %DB_NAME%...
if defined DB_PASSWORD (
    "%MYSQL_CMD%" ^
        -h "%DB_HOST%" ^
        -P "%DB_PORT%" ^
        -u "%DB_USER%" ^
        "-p%DB_PASSWORD%" ^
        -e "CREATE DATABASE IF NOT EXISTS `%DB_NAME%` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"
) else (
    "%MYSQL_CMD%" ^
        -h "%DB_HOST%" ^
        -P "%DB_PORT%" ^
        -u "%DB_USER%" ^
        -e "CREATE DATABASE IF NOT EXISTS `%DB_NAME%` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"
)

if errorlevel 1 (
    echo ERROR: failed to create or access MySQL database.>&2
    exit /b 1
)

if not defined DATABASE_URL (
    if defined DB_PASSWORD (
        set "DATABASE_URL=mysql+pymysql://%DB_USER%:%DB_PASSWORD%@%DB_HOST%:%DB_PORT%/%DB_NAME%?charset=utf8mb4"
    ) else (
        set "DATABASE_URL=mysql+pymysql://%DB_USER%@%DB_HOST%:%DB_PORT%/%DB_NAME%?charset=utf8mb4"
    )
)

if not defined SECRET_KEY (
    set "SECRET_KEY=dev-secret-key-change-me"
)

echo Checking existing database data...
"%PYTHON_CMD%" "scripts\mysql_db_has_data.py"
if errorlevel 1 (
    echo Database is empty, incomplete or missing tables. Importing missing CSV data...
    "%PYTHON_CMD%" "scripts\setup_mysql_db.py"
    if errorlevel 1 (
        echo ERROR: database setup failed.>&2
        exit /b 1
    )
) else (
    echo Skipping setup.
)

echo Starting Flask app at %APP_URL%
start "Finance Analyzer Backend" "%PYTHON_CMD%" "backend.py"

echo Waiting for the server...
set "RETRY=0"

:wait_loop
powershell -NoProfile -ExecutionPolicy Bypass -Command "try { Invoke-WebRequest -Uri '%APP_URL%' -UseBasicParsing -TimeoutSec 1 | Out-Null; exit 0 } catch { exit 1 }" >nul 2>&1
if not errorlevel 1 goto server_ready
set /a RETRY+=1
if %RETRY% GEQ 45 goto server_timeout
timeout /t 1 /nobreak >nul
goto wait_loop

:server_timeout
echo WARNING: server did not respond within 45 seconds. Open manually: %APP_URL%>&2
goto finish

:server_ready
echo Opening browser: %APP_URL%
start "" "%APP_URL%"

:finish

endlocal
