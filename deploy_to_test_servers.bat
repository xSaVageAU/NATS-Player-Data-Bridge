@echo off
setlocal enabledelayedexpansion

:: --- CONFIGURATION ---
set "PROJECT_ROOT=%~dp0"
set "FABRIC_DIR=%PROJECT_ROOT%deps\NATS-Fabric"
set "BRIDGE_DIR=%PROJECT_ROOT%"


:: --- BUILD PHASE ---
echo [1/4] Publishing NATS-Fabric to MavenLocal...
cd /d "%FABRIC_DIR%"
call gradlew.bat clean publishToMavenLocal
if %ERRORLEVEL% neq 0 (
    echo [ERROR] NATS-Fabric publish failed!
    pause
    exit /b %ERRORLEVEL%
)

echo [2/4] Building NATS-Player-Data-Bridge (with JiJ)...
cd /d "%BRIDGE_DIR%"
call gradlew.bat clean build
if %ERRORLEVEL% neq 0 (
    echo [ERROR] NATS-Player-Data-Bridge build failed!
    pause
    exit /b %ERRORLEVEL%
)

:: --- DEPLOY PHASE (DISABLED) ---
:: echo [3/4] Locating jars...
:: ... (The rest is commented out below)

echo.
echo ========================================
echo   Build and Local Publish Complete!
echo ========================================
echo.
pause
exit /b 0
