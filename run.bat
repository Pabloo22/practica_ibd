@echo off

set DATA_DIR=.\data

:: Check if the directory exists
if not exist "%DATA_DIR%" (
    mkdir "%DATA_DIR%"
)

:: Run Docker Compose
docker compose up --build