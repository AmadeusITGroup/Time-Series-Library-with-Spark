# Time Series Operations Library

## Example of integration

### Sample scenario

The customer wants to monitor the number and quality of accesses to data containers by agents.
For this purpose, the customer application creates event logs for each access.

The events have the following schema:

| timestamp | office | userId | container | action | price |
| ------------| ------| ---------| ----------- | -------| --------|
| Timestamp in SQL format | agent identifier | agent identifier | container identifier | action performed on the container | price associated to the action |

Example of events: [events_sample_1.csv](./src/test/resources/events_sample_1.csv)

Event data model described in [CustomEvent.scala](./src/test/scala/com/amadeus/tso/library/sample/CustomEvent.scala)

The functional details of the monitoring/alerting:
* monitoring is computed on a 1-hour time frame, 1-hour sliding interval
* agents are classified in three categories: ATO, B2B, DEFAULT
* raise alerts when an agent manipulates (no matter the type of action) an exceeding number of different containers
    * for ATO: more than 1
    * for B2B: more than 5
    * for DEFAULT: more than 2
* the alert are expected in the specified format

  * access_timestamp: the beginning of the analyzed time frame
  * retriever: concatenation of agent office and userId
  * model: the category of the agent
  * containers: the amount of different containers manipulated by the agent
  * allowedContainers: the threshold that determines the alert for the specific category
  * actions: the list of different actions performed by the agent
  
The alerts should be stored on disk and pushed to Kafka.

Example of expected alerts: [anomalies_sample_1.json](./src/test/resources/anomalies_sample_1.json)

### Sample implementation #1

The client configured the two steps:

* aggregate events to time series: [`CustomAggregator`](./src/test/scala/com/amadeus/tso/library/sample/CustomAggregator.scala)
extends AggregatorTrait
* analyze time series and detect anomalies: [`CustomAnalyzer`](./src/test/scala/com/amadeus/tso/library/sample/CustomAnalyzer.scala)
                                            extends AnalyzerTrait

The client chose to store time series as an intermediate result on disk.

#### Time series

In order to compute time series, the client job just needs to:
* read the input events DataFrame
* invoke the CustomAggregator.aggregate()
* store the output DataFrame to disk

An example of these calls is in the [`CustomEventAggregator` unit test](./src/test/scala/com/amadeus/tso/library/AggregatorTest.scala).

#### Anomalies

The client provided the [agent categorization](./src/test/resources/profiles_sample_1.csv) and 
[expected thresholds](./src/test/resources/reference_sample_1.csv) as reference files.

In order to detect anomalies in time series, the client job just needs to:
* read the input time series DataFrame and the reference files
* invoke the CustomAnalyzer.detectAnomalies()
* store the output DataFrame to disk and push it to Kafka

An example of these calls is in the [`CustomAnalyzer` unit test](./src/test/scala/com/amadeus/tso/library/AnalyzerTest.scala).

### Sample implementation #2

The client chose again to store time series as an intermediate result on disk.

Taking advantage of the [`Aggregator`](./src/main/scala/com/amadeus/tso/library/Aggregator.scala) abstract class,
the client provides the configuration details through a typesafe.Config:

* extend Aggregator in [`CustomAggregatorFromConfig`](./src/test/scala/com/amadeus/tso/library/sample/CustomAggregatorFromConfig.scala)
* provide the configuration file (example in [sampleAggregator.conf](./src/test/resources/sampleAggregator.conf))

 (the class reads the section Aggregator of the config, the rest of the file is ignored).
 
An example of use of this class is in the [`CustomFromConfig` unit test](./src/test/scala/com/amadeus/tso/library/AnalyzerTest.scala).

For the anomaly detection, the solution is the again the [`CustomAnalyzer`](./src/test/scala/com/amadeus/tso/library/sample/CustomAnalyzer.scala) class extending AnalyzerTrait
(check the example #1).

### Sample implementation #3

The client chose to transform events to anomalies in one job, without intermediate results.

The solution is based on the same classes as #1, [`CustomAggregator`](./src/test/scala/com/amadeus/tso/library/sample/CustomAggregator.scala)
                                                 extending AggregatorTrait
                                                 and [`CustomAnalyzer`](./src/test/scala/com/amadeus/tso/library/sample/CustomAnalyzer.scala)
                                                                                             extending AnalyzerTrait.
The only difference is in the client job:
* read the input events DataFrame
* read reference files 
* apply the two transformations on the input, CustomAggregator.aggregate and CustomAnalyzer.detectAnomalies
* store the output DataFrame

An example of these calls is in the [`Full chain` unit test](./src/test/scala/com/amadeus/tso/library/AnalyzerTest.scala).


