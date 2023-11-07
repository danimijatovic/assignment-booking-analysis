# Commercial Booking Analysis

The goal of the commercial booking Analysis is to create a dataset to find out what is the top destination country 
per season per weekday for KLM departuring from the Netherlands. 

## Dependencies 
- Spark 3.x.x
- Scala 2.12.x
- java 8 
- sbt  

## run from IDE 
All the resources are specified in ```resouces/application``` of each application. you can the application with 

``` sbt run ``` 
or from the IDE itself. 

## run from Cluster 
```resouces/application.dev.conf``` specifies a potential hdfs source. please update and deploz a fat jar on cluster and run the spark summit command. 

## Discharges handling 
The iata code from bookings is mapped to the airport table to access the country and city. In case of a unmapped value we store that value in the discharge sink for further investigation. 