package pubsub




import (
	amqp "github.com/rabbitmq/amqp091-go"
	"bytes"
	"encoding/gob"
	"encoding/json"
	"log"
)




type Acktype int

const (
	Ack = iota
	NackRequeue
	NackDiscard
)

func subscribe[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	simpleQueueType SimpleQueueType,
	handler func(T) Acktype,
	unmarshaller func([]byte) (T, error),
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
		defer ch.Close()
		for msg := range msgs {
			target, err := unmarshaller(msg.Body)
			if err != nil {
				log.Printf("Failed to unmarshal: %v\n", err)
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

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T) Acktype,
) error {
	return subscribe[T](
		conn,
		exchange,
		queueName,
		key,
		queueType,
		handler,
		func(data []byte) (T, error) {
			var target T
			err := json.Unmarshal(data, &target)
			return target, err
		},
	)
}

func SubscribeGob[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T) AckType,
) error {
	return subscribe[T](
		conn,
		exchange,
		queueName,
		key,
		queueType,
		handler,
		func(data []byte) (T, error) {
			var target T
			wrappedData := bytes.NewBuffer(data)
			dec := gob.NewDecoder(wrappedData)
			err := dec.Decode(&target)
			return target, err
		},
	)
}
