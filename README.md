# Golang + Kafka

Example code for implementing Kafka in golang

Steps to run:

1. Clone this repository
2. Install dependencies : `go mod tidy`
3. Make sure Kafka is running, and change the value of `brokerAddress`, `readTopic` and `writeTopic` to the address of your Kafka instance
4. Run the code : `go run ./service-booking/main.go`
5. Run the code : `go run ./service-payment/main.go`
