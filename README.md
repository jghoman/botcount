# botcount
Count the number of bot edits from wikimedia stream in Flink

## Running from the command line and output to terminal

```mvn compile exec:java -Dexec.mainClass="woohoo.BotCount"```

## Running from the command line and writing to kafka topic botcounts
```mvn compile exec:java -Dexec.mainClass="woohoo.BotCount" -Dexec.args="--output kafka"```
