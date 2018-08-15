# recharge
recharge is a bulk loader for Redis. It supports the following data sources as inputs:

* Flat files: delimited (CSV) and fixed length (FW)
* JSON
* XML
* Data Generator based on Faker

## Building
Recharge depends on these two projects:
* [Lettusearch](https://github.com/Redislabs-Solution-Architects/lettusearch)
* [PojoFaker](https://github.com/jruaux/pojofaker)

Build each project first before you build Recharge:
```
mvn clean install
```

## Examples
The [examples](./examples) folder contains a few sample configurations to generate various datasets:
* Openflights.org: [https://openflights.org/data.html](https://openflights.org/data.html)
```
java -jar target/recharge-0.0.1-SNAPSHOT.jar --spring.config.additional-location=examples/openflights.yml
```

* Medical Observations
```
java -jar target/recharge-0.0.1-SNAPSHOT.jar --spring.config.additional-location=examples/medical-observations.yml
```
