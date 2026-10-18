@echo off
setlocal

set PYTHON=.venv\Scripts\python.exe
set COMPOSE_FILE=docker\docker-compose.yml

if "%1"=="" goto help
if "%1"=="help" goto help
if "%1"=="setup" goto setup
if "%1"=="up" goto up
if "%1"=="down" goto down
if "%1"=="test" goto test
if "%1"=="test-integration" goto test_integration
if "%1"=="verify" goto verify
if "%1"=="stream" goto stream
if "%1"=="simulate" goto simulate
if "%1"=="api" goto api
if "%1"=="ui" goto ui
if "%1"=="ui-build" goto ui_build
if "%1"=="demo" goto demo
if "%1"=="demo-ui" goto demo_ui

echo Unknown target: %1
goto help

:help
echo CrisisGuard AI (v1.0-final-9) Operational Commands:
echo   make setup             - Install Python dependencies and build frontend SPA
echo   make up                - Start Docker big data cluster services (Kafka, HDFS, Hive)
echo   make down              - Tear down Docker big data cluster services
echo   make test              - Run fast unit and logic tests
echo   make test-integration  - Run end-to-end integration tests (Kafka/HDFS/Hive)
echo   make verify            - Run test suite and documentation consistency audit
echo   make stream            - Launch Spark Structured Streaming real-time scorer
echo   make simulate          - Launch Kafka live replay stream simulator
echo   make api               - Start FastAPI operations gateway (:8000)
echo   make ui                - Start Vite development server (:3000)
echo   make ui-build          - Compile production frontend SPA into frontend/dist
echo   make demo              - Launch full terminal pipeline demonstration
echo   make demo-ui           - Launch FastAPI gateway and serve dashboard at http://127.0.0.1:8000/
goto end

:setup
%PYTHON% -m pip install -r requirements.txt
cd frontend && npm install && npm run build && cd ..
goto end

:up
docker compose -f %COMPOSE_FILE% up -d
goto end

:down
docker compose -f %COMPOSE_FILE% down
goto end

:test
%PYTHON% -m pytest tests\test_real_pipeline.py tests\test_dashboard_upgrade.py -v
goto end

:test_integration
%PYTHON% -m pytest tests\test_integration.py -v
goto end

:verify
%PYTHON% -m pytest tests\test_real_pipeline.py tests\test_dashboard_upgrade.py -v
if %ERRORLEVEL% NEQ 0 goto end
%PYTHON% scripts\check_doc_consistency.py
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
cd frontend && npm run dev
goto end

:ui_build
cd frontend && npm run build && cd ..
goto end

:demo
%PYTHON% scripts\demo_terminal.py
goto end

:demo_ui
%PYTHON% -m uvicorn backend.main:app --host 0.0.0.0 --port 8000
goto end

:end
endlocal
