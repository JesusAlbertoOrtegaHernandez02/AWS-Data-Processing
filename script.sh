export AWS_REGION="us-east-1"
export ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
export BUCKET_NAME="datalate-car-stats-${ACCOUNT_ID}"
export ROLE_ARN=$(aws iam get-role --role-name LabRole --query 'Role.Arn' --output text)

$env:AWS_REGION="us-east-1"
$env:ACCOUNT_ID="654654465031"
$env:BUCKET_NAME="datalate-car-stats-654654465031"
$env:ROLE_ARN="arn:aws:iam::654654465031:role/LabRole"

aws s3 ls

aws s3api put-object --bucket datalate-car-stats-654654465031 --key raw/
aws s3api put-object --bucket datalate-car-stats-654654465031 --key processed/
aws s3api put-object --bucket datalate-car-stats-654654465031 --key config/
aws s3api put-object --bucket datalate-car-stats-654654465031 --key scripts/


aws kinesis create-stream --stream-name car-stats --shard-count 1 
python .\kinesis.py

# Segunda práctica, Firehose

# Paso 1: Crear firehose.py en .zip
zip firehose.zip firehose.py
aws lambda create-function --function-name speed-firehose --runtime python3.11 --role arn:aws:iam::654654465031:role/LabRole --handler firehose.lambda_handler --zip-file fileb://firehose.zip --timeout 60 --memory-size 128
aws lambda update-function-code --function-name speed-firehose --zip-file fileb://firehose.zip


aws firehose create-delivery-stream `                         
>>   --delivery-stream-name car-stats-stream `
>>   --delivery-stream-type KinesisStreamAsSource ` 
>>   --kinesis-stream-source-configuration "KinesisStreamARN=arn:aws:kinesis:us-east-1:654654465031:stream/car-stats,RoleARN=arn:aws:iam::654654465031:role/LabRole" `    
>>   --extended-s3-destination-configuration file://firehose-s3.json          

# Tercera práctica, Glue
aws glue create-database --database-input Name=car_stats_db
aws glue create-crawler 
>>  --name car-raw-crawler 
>>  --role arn:aws:iam::654654465031:role/LabRole 
>>  --database-name car_stats_db 
>>  --targets file://target_raw.json

aws glue create-crawler `                                     
>>   --name car-processed-crawler `                                                                                                           
>>   --role arn:aws:iam::654654465031:role/LabRole `  
>>   --database-name car_stats_db `                                                                                                     
>>   --targets file://target_processed.json  

aws glue start-crawler --name car-raw-crawler
aws glue start-crawler --name car-processed-crawler



aws s3 cp consumo_medio_de_vehiculos.py s3://datalate-car-stats-654654465031/scripts/
aws s3 cp consumo_diario_vehiculo.py s3://datalate-car-stats-654654465031/scripts/

aws glue create-job `
  --name vehicle-daily-consumption `
  --role arn:aws:iam::654654465031:role/LabRole `
  --command file://command_daily.json `
  --default-arguments file://args_daily.json `
  --glue-version 4.0 `
  --number-of-workers 2 `
  --worker-type G.1X

aws glue create-job `
  --name vehicle-speed-range-aggregation `
  --role arn:aws:iam::654654465031:role/LabRole `
  --command file://command_range.json `
  --default-arguments file://args_range.json `
  --glue-version 4.0 `
  --number-of-workers 2 `
  --worker-type G.1X




vehicle-medium-consumption

aws glue start-job-run --job-name vehicle-daily-consumption
aws glue start-job-run --job-name vehicle-speed-range-aggregation