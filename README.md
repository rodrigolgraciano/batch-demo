# Spring Batch demo

This repository is part of the talk Batch Processing in Action presented by Rodrigo Graciano & Hillmer Chona

* Run `mvn verify`

* Rest endpoint

  * **GET:** http://localhost:8080/trigger/importTourResults: Batch job to import from text file to a relational database
  * **GET:** http://localhost:8080/trigger/importTourResultsFaultTolerant: Batch job to import from text file to a relational database applying fault-tolerant features
  * **GET:** http://localhost:8080/trigger/multiJob: Batch job to import from text file to a relational database
  * **GET:** http://localhost:8080/trigger/multiStepsJob: Batch job to import from text file to a relational database


* Rest endpoint asynchronous

  * **GET:** http://localhost:8080/trigger-async/importTourResults: Batch job to import from text file to a relational database
  * **GET:** http://localhost:8080/trigger-async/importTourResultsFaultTolerant: Batch job to import from text file to a relational database applying fault-tolerant features
  * **GET:** http://localhost:8080/trigger-async/multiJob: Batch job to import from text file to a relational database
  * **GET:** http://localhost:8080/trigger-async/multiStepsJob: Batch job to import from text file to a relational database



* Embedded in memory H2 database console access
  
  * http://localhost:8080/h2-console/
