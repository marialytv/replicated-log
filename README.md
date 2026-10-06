**Installation**
To run the application run these commands in the parent folder:
1. docker compose build --no-cache
2. docker-compose up -d master-service secondary1-service secondary2-service
3. docker compose run grpc-client

Successful installation includes creating 3 services(master and 2 secondaries)
and running console application that prompts adding messages to the service list.

In order to stop adding messages enter:'exit'
To run client console app again run command #3.
**Logging**
Logs from calling get/post messages for each service(master and secondaries) can be found in 
docker desktop-> select container(i.e. master-service)-> Files->app->logs-> 
'grpcservice_log_master.txt'( for secondaries corresponding files are 'grpcservice_log_secondary1.txt',
'grpcservice_log_secondary2.txt'). 
Logs can be found as well on container->Logs screen.

**Changing replication lag(service delay)**
In order to change service delay,
update DelayInSec setting in ServiceConfig block of each service appsetting.*servicename*.json file
(appsettings.Master.json, appsettings.Secondary1.json, appsettings.Secondary2.json)
and save corresponding appsetting file - delay will be updated automatically.

Access appsettings file of each service from container->Bind Mounts.

**Integration Tests**
In order to test different delays(replication lags) programmatically, run  tests from MessageServiceTests project.
