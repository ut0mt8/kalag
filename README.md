# kalag
Kafka lag library

Starting the jvm is slow, so why not writing a small library to get the lag of consummergroup in go ?
The lib use only sarama low level functions, and so it is blazing fast, and also it reports both the lag in offset and in time.
Including a small example cli that mimic kafka-consumer-groups describe command.
