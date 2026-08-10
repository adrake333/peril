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
	connectionStr := "amqp://guest:guest@localhost:5672/"
	connection, err := amqp.Dial(connectionStr)
	if err != nil {
		log.Fatalf("Error connecting to server: %v", err)
	}

	defer connection.Close()

	fmt.Println("Connection successful")

	username, err := gamelogic.ClientWelcome()
	if err != nil {
		fmt.Println(err)
		return
	}

	queueName := routing.PauseKey + "." + username

	_, _, err = pubsub.DeclareAndBind(
		connection,
		routing.ExchangePerilDirect,
		queueName,
		routing.PauseKey,
		pubsub.Transient,
	)
	if err != nil {
		log.Fatalf("Error declaring queue: %v", err)
	}

	gameState := gamelogic.NewGameState(username)

	for {
		input := gamelogic.GetInput()
		if len(input) == 0 {
			fmt.Println("Please enter a valid command")
			continue
		}
		switch input[0] {
		case "spawn":
			err = gameState.CommandSpawn(input)
			if err != nil {
				fmt.Printf("Error spawning unit: %v\n", err)
				continue
			}
		case "move":
			_, err = gameState.CommandMove(input)
			if err != nil {
				fmt.Printf("Error moving unit: %v\n", err)
			}
			fmt.Println("Moving units successful")
		case "status":
			gameState.CommandStatus()
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
