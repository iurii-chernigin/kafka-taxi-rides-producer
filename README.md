# Kafka Producer of NYC Taxi Rides

## Specification

Language: Java
Data serialization format: Avro
Source data: 
  - [fhv_tripdata_2019-01.csv.gz](https://github.com/DataTalksClub/nyc-tlc-data/releases/tag/fhv) 
  - [green_tripdata_2019-01.csv.gz](https://github.com/DataTalksClub/nyc-tlc-data/releases/tag/green)
  
Destination: Kafka in the Confluent cloud
 
## Data consumption
 
Data consumption is implemented with PySpark. For more information: [kafka-taxi-rides-consumer](https://github.com/DataTalksClub/nyc-tlc-data/releases/tag/fhv) 
 
 
