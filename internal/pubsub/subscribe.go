package pubsub




import (
	amqp "github.com/rabbitmq/amqp091-go"
	"encoding/json"
	"log"
)




func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T),
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
	
	deliveries, err := ch.Consume(
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
		for delivery := range deliveries {
			var target T
	
			if err := json.Unmarshal(delivery.Body, &target); err != nil {
				log.Printf("could not unmarshal: %v", err)
				continue
			}

			handler(target)

			delivery.Ack(false)
		}
	}()

	return nil
}
