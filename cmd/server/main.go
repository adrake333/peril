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
	connStr := "amqp://guest:guest@localhost:5672/"
	conn, err := amqp.Dial(connStr)
	if err != nil {
		log.Fatalf("Error connecting to server: %v", err)
	}

	defer conn.Close()

	fmt.Println("Connection successful")

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("Error establishing connection channel: %v", err)
	}

	err = pubsub.PublishJSON(ch,
		routing.ExchangePerilDirect,
		routing.PauseKey,
		routing.PlayingState{IsPaused: true},
	)
	if err != nil {
		log.Fatalf("Error publishing JSON: %v", err)
	}

	err = pubsub.SubscribeGob(
		conn,
		routing.ExchangePerilTopic,
		routing.GameLogSlug,
		routing.GameLogSlug + ".*",
		pubsub.Durable,
		func (gl routing.GameLog) pubsub.Acktype {
			defer fmt.Print("> ")
			err := gamelogic.WriteLog(gl)
			if err != nil {
				log.Printf("Error logging gamelog: %v", err)
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		},
	)
	if err != nil {
		log.Fatalf("Error subscribing gob: %v", err)
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
			err = pubsub.PublishJSON(ch, routing.ExchangePerilDirect,routing.PauseKey,routing.PlayingState{IsPaused: true})
			if err != nil {
				log.Fatalf("Error publishing JSON: %v", err)
			}
		case "resume":
			log.Println("sending resume message")
			err = pubsub.PublishJSON(ch, routing.ExchangePerilDirect,routing.PauseKey,routing.PlayingState{IsPaused: false})
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

	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, os.Interrupt)
	<-signalCh
	fmt.Println("Program is shutting down...")

	return
}
