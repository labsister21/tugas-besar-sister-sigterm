@echo off
REM Usage: run_new_client.bat <target_server_address>
REM Example: run_new_client.bat node1:5050

IF "%~1"=="" (
    echo Usage: %0 ^<target_server_address^>
    exit /b 1
)

SET TARGET_SERVER=%1
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

SET CLIENT_ID=raft_client_%RANDOM%

echo Running Raft client targeting %TARGET_SERVER%
docker run -it --rm ^
 --name %CLIENT_ID% ^
 --network raftnet ^
 %IMAGE_NAME% ^
 java -cp app.jar org.raft.client.Client %TARGET_SERVER%
