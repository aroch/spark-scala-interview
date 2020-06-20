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

## Exercise 2

Go to the following URL, and download the dataset on sampled Last.fm usage: [http://ocelma.net/MusicRecommendationDataset/lastfm-1K.html][lastfm-1k] 
Provide a series of Spark Jobs to answers the questions below.
For each question, describe alternative approaches you would have used instead of Spark, and provide the source that you wrote to solve the problem.

### Part A
Create a list of user IDs, along with the number of distinct songs each user has played.

### Part B
Create a list of the 100 most popular songs (artist and title) in the dataset, with the number of times each was played.

### Part C
Say we define a user’s “session” of Last.fm usage to be comprised of one or more songs played by that user, where each song is started within 20 minutes of the previous song’s start time.
Create a list of the top 10 longest sessions, with the following information about each session: userid, timestamp of first and last songs in the session, and the list of songs played in the session (in order of play).

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

[lastfm-1k]: http://ocelma.net/MusicRecommendationDataset/lastfm-1K.html