package main




import (
	"fmt"
	"log"
	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
)




func handlerWar(gs *gamelogic.GameState, ch *amqp.Channel) func(gamelogic.RecognitionOfWar) pubsub.Acktype {
	return func(rw gamelogic.RecognitionOfWar) pubsub.Acktype {
		defer fmt.Print("> ")
		outcome, winner, loser := gs.HandleWar(rw)
		switch outcome {
		case gamelogic.WarOutcomeNotInvolved:
			return pubsub.NackRequeue

		case gamelogic.WarOutcomeNoUnits:
			return pubsub.NackDiscard

		case gamelogic.WarOutcomeOpponentWon:
			msg := fmt.Sprintf("%s won a war against %s", winner, loser)
			err := pubsub.PublishGameLog(ch, gs.Player.Username, msg)
			if err != nil {
				log.Printf("Error publishing gamelog: %v\n", err)
				return pubsub.NackRequeue
			}
			return pubsub.Ack

		case gamelogic.WarOutcomeYouWon:
			msg := fmt.Sprintf("%s won a war against %s", winner, loser)
			err := pubsub.PublishGameLog(ch, gs.Player.Username, msg)
			if err != nil {
				log.Printf("Error publishing gamelog: %v\n", err)
				return pubsub.NackRequeue
			}
			return pubsub.Ack

		case gamelogic.WarOutcomeDraw:
			msg := fmt.Sprintf("A war between %s and %s resulted in a draw", winner, loser)
			err := pubsub.PublishGameLog(ch, gs.Player.Username, msg)
			if err != nil {
				log.Printf("Error publishing gamelog: %v\n", err)
				return pubsub.NackRequeue
			}
			return pubsub.Ack

		default:
			log.Println("Error determining war outcome")
			return pubsub.NackDiscard
		}
	}
}
