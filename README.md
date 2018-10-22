# Spark Scala Interview Exercise

## Exercise 1

In this exercise you're requested to solve a problem where records event time is different than processing time.
Files were written into folders by their processing time. You've been asked to sort the events by their event time and 
write them back in the following folder structure: `year=<YEAR>/month=<MONTH>/day=<DAY>`.

 ### Expected Output
 
 * Files are placed in their correct folder (by event time) in specified folder structure: `year=<YEAR>/month=<MONTH>/day=<DAY>`
 * Each daily folder contains <b> 1 file ONLY </b>
 * Each file's records is sorted by their time in ascending order
 * The 'time' column in the output file should be as a timestamp data type (e.g: `2018-08-06T17:40:20.000+03:00`)

## Test the code
```
sbt test
```

## Build a fat JAR

```
sbt assembly
```

## Run the code

```
java -jar target/scala-2.11/flink-scala-interview-assembly-1.0.jar
```