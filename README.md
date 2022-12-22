# Spring Batch demo

Updated to Java 17 and Spring Batch 5.

There are multiple profiles, one for each job.

Profile -> Job  -> Job Description
* simple ->  SimpleBatchJob -> read from file and write to file
* multiFile -> RaceMultiFile -> Reads from multiple files and write to DB
* tour -> TourProcessorJob -> Introduces Processor, Chunk, and Listeners
* tourFault -> TourFaultTolerantJob -> Introduces Error Handling
* multiStep -> RaceMultiStepJob -> Introduces Multiple Steps



* Embedded in memory H2 database console access
  
  * http://localhost:8080/h2-console/
