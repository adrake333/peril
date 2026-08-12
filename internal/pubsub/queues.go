package pubsub

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type SimpleQueueType int

const (
	Durable SimpleQueueType = iota
	Transient
)

func DeclareAndBind(
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
) (*amqp.Channel, amqp.Queue, error) {

	channel, err := conn.Channel()
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("declaring queue: %v", err)
	}

	isDurable := queueType == Durable
	isAutoDelete := queueType == Transient
	isExclusive := queueType == Transient

	queueArgs := amqp.Table{"x-dead-letter-exchange": "peril_dlx"}

	queue, err := channel.QueueDeclare(queueName, isDurable, isAutoDelete, isExclusive, false, queueArgs)
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("declaring queue: %v", err)
	}
	err = channel.QueueBind(queueName, key, exchange, false, nil)
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("declaring queue: %v", err)
	}

	return channel, queue, nil
}
