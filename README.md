To run the application run these commands in the parent folder:
1. docker-compose up --build
2. docker-compose up -d master-service secondary1-service secondary2-service
3. docker compose run grpc-client

In order to stop adding messages enter:'exit'
To run console app again run command #3.
Logs from calling get/post messages for each service(master and secondaries) can be found in 
docker desktop-> select container(i.e. master-service)-> Files->app->logs-> 
'grpcservice_log_master.txt'. 

In order to change service delay,
update DelayInSec setting in ServiceConfig block of each service appsetting.*servicename*.json file
(appsettings.Master.json,
appsettings.Secondary1.json,
appsettings.Secondary2.json)
