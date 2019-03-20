# recharge
ReCharge is a bulk loader for Redis. It supports the following data sources as inputs:

* Databases (JDBC)
* Files
  * Delimited (CSV)
  * Fixed width (FW)
  * JSON
  * XML
* [Faker](https://github.com/DiUS/java-faker)

## Building
Build each project first before you build recharge:
```bash
git clone https://github.com/Redislabs-Solution-Architects/recharge.git
cd recharge
mvn clean install
```

## Examples
The [examples](./examples) folder contains a few sample configurations to import various datasets:

* Openflights.org: [https://openflights.org/data.html](https://openflights.org/data.html)
`java -jar target/recharge-1.0.0.jar --spring.config.location=examples/file/airlines.yml`
