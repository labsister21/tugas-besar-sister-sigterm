@echo off
REM Usage: run_new_server.bat <server_id> <server_address>
REM Example: run_new_server.bat node5 node5:5050

IF "%~2"=="" (
    echo Usage: %0 ^<server_id^> ^<server_address^>
    exit /b 1
)

SET SERVER_ID=%1
SET SERVER_ADDR=%2

FOR /F "tokens=2 delims=:" %%A IN ("%SERVER_ADDR%") DO SET PORT=%%A

SET IMAGE_NAME=raft_node_image

echo Building Docker image...
docker build -t %IMAGE_NAME% .

REM Check if raftnet network exists
FOR /F %%i IN ('docker network ls --format "{{.Name}}"') DO (
    IF "%%i"=="raftnet" SET NETWORK_EXISTS=1
)

IF NOT DEFINED NETWORK_EXISTS (
    echo Creating raftnet network...
    docker network create --driver bridge raftnet
)

echo Running Raft server: %SERVER_ID%
docker run -d ^
 --name %SERVER_ID% ^
 --network raftnet ^
 --cap-add=NET_ADMIN ^
 -p %PORT%:5050 ^
 %IMAGE_NAME% ^
 java -cp app.jar org.raft.server.Server %SERVER_ID% %SERVER_ADDR%
