package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	amqp "github.com/rabbitmq/amqp091-go"
	
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
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

	gamelogic.PrintServerHelp()

	for {
		input := gamelogic.GetInput()
		if len(input) == 0 {
			continue
		}
		switch input[0] {
		case "pause":
			log.Println("sending pause message")
			err = pubsub.PublishJSON(channel, routing.ExchangePerilDirect,routing.PauseKey,routing.PlayingState{IsPaused: true})
			if err != nil {
				log.Fatalf("Error publishing JSON: %v", err)
			}
		case "resume":
			log.Println("sending resume message")
			err = pubsub.PublishJSON(channel, routing.ExchangePerilDirect,routing.PauseKey,routing.PlayingState{IsPaused: false})
			if err != nil {
				log.Fatalf("Error publishing JSON: %v", err)
			}
		case "quit":
			log.Println("exiting program")
			return
		default:
			log.Println("unkown command")
		}
	}

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	<-signalChan
	fmt.Println("Program is shutting down...")

	return
}
