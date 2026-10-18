@echo off
setlocal

set PYTHON=.venv\Scripts\python.exe
set COMPOSE_FILE=docker\docker-compose.yml

if "%1"=="" goto help
if "%1"=="help" goto help
if "%1"=="setup" goto setup
if "%1"=="up" goto up
if "%1"=="down" goto down
if "%1"=="preflight" goto preflight
if "%1"=="stream" goto stream
if "%1"=="simulate" goto simulate
if "%1"=="api" goto api
if "%1"=="ui" goto ui
if "%1"=="ui-build" goto ui_build
if "%1"=="test" goto test
if "%1"=="test-integration" goto test_integration
if "%1"=="verify" goto verify
if "%1"=="demo" goto demo
if "%1"=="demo-ready" goto demo_ready
if "%1"=="demo-stop" goto demo_stop

echo Unknown target: %1
goto help

:help
echo CrisisGuard (v1.0-final-9) Operational Targets:
echo   make setup             - Install dependencies and build frontend
echo   make up                - Start Docker cluster services
echo   make down              - Stop Docker cluster services
echo   make preflight         - Run preflight service and asset checklist
echo   make stream            - Run Spark streaming scorer
echo   make simulate          - Run post replay simulator
echo   make api               - Start FastAPI backend
echo   make ui                - Start frontend development server
echo   make ui-build          - Build production frontend SPA
echo   make test              - Run pytest suite and client logic tests
echo   make test-integration  - Run end-to-end integration tests
echo   make verify            - Run tests and consistency verification
echo   make demo              - Run interactive terminal demo
echo   make demo-ready        - Start services, replay, API, and launch dashboard
echo   make demo-stop         - Stop background demo processes
goto end

:setup
%PYTHON% -m pip install -r requirements.txt
cd frontend && call npm install && call npm run build && cd ..
goto end

:up
docker compose -f %COMPOSE_FILE% up -d
goto end

:down
docker compose -f %COMPOSE_FILE% down
goto end

:preflight
%PYTHON% scripts\preflight.py
goto end

:stream
%PYTHON% streaming\spark_stream_scorer.py
goto end

:simulate
%PYTHON% ingestion\replay_simulator.py
goto end

:api
%PYTHON% -m uvicorn backend.main:app --host 0.0.0.0 --port 8000
goto end

:ui
cd frontend && call npm run dev
goto end

:ui_build
cd frontend && call npm run build && cd ..
goto end

:test
%PYTHON% -m pytest tests\test_real_pipeline.py tests\test_dashboard_upgrade.py tests\test_ui_cleanliness.py -v
cd frontend && call npm test && cd ..
goto end

:test_integration
%PYTHON% -m pytest tests\test_integration.py -v
goto end

:verify
%PYTHON% -m pytest tests\test_real_pipeline.py tests\test_dashboard_upgrade.py tests\test_ui_cleanliness.py -v
if %ERRORLEVEL% NEQ 0 goto end
cd frontend && call npm test && cd ..
if %ERRORLEVEL% NEQ 0 goto end
%PYTHON% scripts\check_doc_consistency.py
goto end

:demo
%PYTHON% scripts\demo_terminal.py
goto end

:demo_ready
%PYTHON% scripts\demo_ready.py
goto end

:demo_stop
%PYTHON% scripts\demo_stop.py
goto end

:end
endlocal
