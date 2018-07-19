# recharge
recharge is a bulk loader for Redis. It supports the following data sources as inputs:

* Flat files: delimited (CSV) and fixed length (FW)
* JSON
* XML
* Data Generator based on Faker

## Building

## Loading a CSV into Redis

```
java -jar recharge-0.0.1-SNAPSHOT.jar load --host=localhost --port=6379 --entities.airline.file.path=https://raw.githubusercontent.com/jpatokal/openflights/master/data/airlines.dat --entities.airline.fields=AirlineID,Name,Alias,IATA,ICAO,Callsign,Country,Active
```