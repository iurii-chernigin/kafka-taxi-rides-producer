# Kafka Producer of NYC Taxi Rides

## Specification

Language: Java
<br>Data serialization format: Avro
<br>Source data:
- [fhv_tripdata_2019-01.csv.gz](https://github.com/DataTalksClub/nyc-tlc-data/releases/tag/fhv)
- [green_tripdata_2019-01.csv.gz](https://github.com/DataTalksClub/nyc-tlc-data/releases/tag/green)

Destination: Kafka in the Confluent cloud

## Data consumption

Data processing from the Kafka is implemented with PySpark. 
<br>For more information: [kafka-taxi-rides-streaming](https://github.com/iurii-chernigin/kafka-taxi-rides-streaming) 
