package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	amqp "github.com/rabbitmq/amqp091-go"
	
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
)

func main() {
	fmt.Println("Starting Peril server...")
	connectionStr := "amqp://guest:guest@localhost:5672/"
	connection, err := amqp.Dial(connectionStr)
	if err != nil {
		log.Fatalf("Error connecting to server: %v", err)
	}

	defer connection.Close()

	fmt.Println("Connection successful")

	channel, err := connection.Channel()
	if err != nil {
		log.Fatalf("Error establishing connection channel: %v", err)
	}

	err = pubsub.PublishJSON(channel, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{IsPaused: true})
	if err != nil {
		log.Fatalf("Error publishing JSON: %v", err)
	}

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	<-signalChan
	fmt.Println("Program is shutting down...")

	return
}
