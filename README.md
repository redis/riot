# recharge
recharge is a bulk loader for Redis. It supports the following data sources as inputs and outputs:

* Redis (hashes)
* Flat files: delimited (CSV) and fixed length (FW)
* JSON
* XML
* Relational databases with JDBC

## Building

## Loading a CSV into Redis

```
java -jar recharge-0.0.1-SNAPSHOT.jar load --host=localhost --port=6379 --file.type=delimited --file.path=/Users/jruaux/git/openflights/data/airlines.dat --file.flat.field-names=AirlineID,Name,Alias,IATA,ICAO,Callsign,Country,Active
```