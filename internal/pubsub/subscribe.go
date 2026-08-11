package pubsub




import (
	amqp "github.com/rabbitmq/amqp091-go"
	"encoding/json"
	"log"
)




type Acktype int

const (
	Ack = iota
	NackRequeue
	NackDiscard
)


func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T) Acktype,
) error {
	ch, queue, err := DeclareAndBind(
		conn,
		exchange,
		queueName,
		key,
		queueType,
	)
	if err != nil {
		return err
	}
	
	msgs, err := ch.Consume(
		queue.Name,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}

	go func() {
		for msg := range msgs {
			var target T
	
			if err := json.Unmarshal(msg.Body, &target); err != nil {
				log.Printf("could not unmarshal: %v", err)
				continue
			}

			acktype := handler(target)
			switch acktype {
			case Ack:
				msg.Ack(false)
				log.Println("Ack")
			case NackRequeue:
				msg.Nack(false, true)
				log.Println("NackRequeue")
			case NackDiscard:
				msg.Nack(false, false)
				log.Println("NackDiscard")
			}
		}
	}()

	return nil
}
