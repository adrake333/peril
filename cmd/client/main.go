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
	fmt.Println("Starting Peril client...")
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
	
	username, err := gamelogic.ClientWelcome()
	if err != nil {
		fmt.Println(err)
		return
	}

	queueName := routing.PauseKey + "." + username

	gs := gamelogic.NewGameState(username)

	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilDirect,
		queueName,
		routing.PauseKey,
		pubsub.Transient,
		handlerPause(gs),
	)
	if err != nil {
		log.Fatalf("Error subscribing JSON: %v", err)
	}

	mvQueueName := routing.ArmyMovesPrefix + "." + username

	mvKey := routing.ArmyMovesPrefix + ".*"

	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilTopic,
		mvQueueName,
		mvKey,
		pubsub.Transient,
		handlerMove(gs),
	)
	if err != nil {
		log.Fatalf("Error subscribing JSON: %v", err)
	}

	for {
		input := gamelogic.GetInput()
		if len(input) == 0 {
			fmt.Println("Please enter a valid command")
			continue
		}
		switch input[0] {
		case "spawn":
			err = gs.CommandSpawn(input)
			if err != nil {
				fmt.Printf("Error spawning unit: %v\n", err)
				continue
			}
		case "move":
			mv, err := gs.CommandMove(input)
			if err != nil {
				fmt.Printf("Error moving unit: %v\n", err)
				continue
			} else {
				fmt.Println("Moving units successful")
			}
			err = pubsub.PublishJSON(ch, routing.ExchangePerilTopic, routing.ArmyMovesPrefix + "." + mv.Player.Username, mv)
			if err != nil {
				log.Printf("Error publishing move message: %v\n", err)
				continue
			} else {
				log.Println("Move message published successfully")
			}
		case "status":
			gs.CommandStatus()
		case "help":
			gamelogic.PrintClientHelp()
		case "spam":
			fmt.Println("Spamming not allowed yet!")
		case "quit":
			gamelogic.PrintQuit()
			return
		default:
			fmt.Println("Please enter a valid command")
			continue
		}
	}

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	<-signalChan
	fmt.Println("Program is shutting down...")

}
