@echo off
setlocal enabledelayedexpansion

set "programName=Script Server"
set "anaconda_env=script-server"
set "installFileDir=D:\Install Files"

rem Get version input from user
set /p version="Enter the version number (e.g., 1.3.2.8): "

echo =======================================================================
echo ================= Starting the deployment process =====================
echo =======================================================================

set "installFilePath=!installFileDir!\!programName!\!version!"
set "initialPath=%CD%"
echo Initial Path: !initialPath!

setlocal enabledelayedexpansion
chcp 65001 > nul 2>&1

rem Step 0: 가상환경 실행 상태 확인
for /f "tokens=4" %%i in ('conda info ^| findstr /C:"active environment"') do (
    set conda_info=%%i
)
set conda_info=!conda_info: =!
echo Current virtual environment: !conda_info!

if "!conda_info!" neq "!anaconda_env!" (
    echo This script must be executed with an active Anaconda virtual environment.
    echo Please activate Anaconda virtual environment and run it again.
    pause
    echo Please press any key to terminate deployment process.
    exit /b 1
)

echo [Anaconda Environment]
for /f "tokens=*" %%a in ('conda info') do (
    echo %%a
)
timeout /t 3 > nul
echo.

rem Step 1: 실행 파일 생성
echo Creating executable file...
pyinstaller main.spec --noconfirm
if %errorlevel% neq 0 (
    echo Error occurred during pyinstaller. Aborting deployment...
    pause
    exit /b %errorlevel%
)
timeout /t 3 > nul
echo.

rem Step 2: 파일 위치 변경
echo Changing directory...
cd "dist\!programName!"
move "_internal\script-server.yaml" ".\"
move "_internal\static" ".\"

echo ========================================================================
echo ============ The deployment has been successfully completed ============
echo ========================================================================

rem Step 3: dist 폴더를 D:\install_files 경로로 복사
cd !initialPath!
if not exist "!installFilePath!" mkdir "!installFilePath!"
xcopy "dist" "!installFilePath!" /E /R /Y

endlocal
