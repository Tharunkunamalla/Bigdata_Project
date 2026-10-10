@echo off
setlocal

set PYTHON=.venv\Scripts\python.exe
set COMPOSE_FILE=docker\docker-compose.yml

if "%1"=="" goto help
if "%1"=="help" goto help
if "%1"=="up" goto up
if "%1"=="down" goto down
if "%1"=="reset" goto reset
if "%1"=="verify-infra" goto verify_infra
if "%1"=="data" goto data
if "%1"=="train" goto train
if "%1"=="stream" goto stream
if "%1"=="simulate" goto simulate
if "%1"=="api" goto api
if "%1"=="ui" goto ui
if "%1"=="test" goto test
if "%1"=="verify" goto verify
if "%1"=="report" goto report
if "%1"=="demo" goto demo

echo Unknown target: %1
goto help

:help
echo Available commands:
echo   make up            - Launch Big Data cluster (Kafka, HDFS, Hive, Spark)
echo   make down          - Stop Big Data cluster
echo   make reset         - Tear down cluster and wipe persistent volumes
echo   make verify-infra  - Verify cluster connectivity and I/O across all tools
echo   make data          - Download and prepare disaster and synthetic datasets
echo   make train         - Train and evaluate Spark MLlib classification pipelines
echo   make stream        - Launch Spark Structured Streaming scoring job
echo   make simulate      - Run the real-time social and hazard replay simulator
echo   make api           - Start the FastAPI backend server
echo   make ui            - Start the React/Vite dashboard
echo   make test          - Run the test suite
echo   make verify        - Run end-to-end validation against rubric criteria
echo   make report        - Generate evaluation metrics, ablation tables and plots
echo   make demo          - Launch full end-to-end demo
goto end

:up
docker compose -f %COMPOSE_FILE% up -d
goto end

:down
docker compose -f %COMPOSE_FILE% down
goto end

:reset
docker compose -f %COMPOSE_FILE% down -v --remove-orphans
goto end

:verify_infra
%PYTHON% tests\verify_infra.py
goto end

:data
%PYTHON% data\download_datasets.py
goto end

:train
%PYTHON% ml\train_models.py
goto end

:stream
%PYTHON% streaming\spark_stream_scorer.py
goto end

:simulate
%PYTHON% ingestion\replay_simulator.py
goto end

:api
%PYTHON% -m uvicorn backend.main:app --host 0.0.0.0 --port 8000 --reload
goto end

:ui
cd frontend && npm run dev
goto end

:test
%PYTHON% -m pytest tests\ -v
goto end

:verify
%PYTHON% tests\verify_infra.py && %PYTHON% -m pytest tests\ -v && %PYTHON% tests\verify_pipeline.py
goto end

:report
%PYTHON% allocation\run_ablation.py
%PYTHON% results\generate_plots.py
goto end

:demo
%PYTHON% scripts\run_demo.py
goto end

:end
endlocal
