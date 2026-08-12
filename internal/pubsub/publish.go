package pubsub

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	amqp "github.com/rabbitmq/amqp091-go"
	"time"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
)

func PublishJSON[T any](ch *amqp.Channel, exchange, key string, val T) error {
	jsonData, err := json.Marshal(val)
	if err != nil {
		return err
	}
	err = ch.PublishWithContext(context.Background(),
		exchange,
		key,
		false,
		false,
		amqp.Publishing{ContentType: "application/json", Body: jsonData})
	if err != nil {
		return err
	}
	return nil
}

func PublishGob[T any](ch *amqp.Channel, exchange, key string, val T) error {
	var network bytes.Buffer
	enc := gob.NewEncoder(&network)
	err := enc.Encode(val)
	if err != nil {
		return err
	}
	err = ch.PublishWithContext(
		context.Background(),
		exchange,
		key,
		false,
		false,
		amqp.Publishing{
			ContentType: "application/gob",
			Body:        network.Bytes(),
		},
	)
	if err != nil {
		return err
	}
	return nil
}

func PublishGameLog(ch *amqp.Channel, username, msg string) error {
	key := routing.GameLogSlug + "." + username
	gl := routing.GameLog{
		CurrentTime: time.Now(),
		Message:     msg,
		Username:    username,
	}
	err := PublishGob(ch, routing.ExchangePerilTopic, key, gl)
	if err != nil {
		return err
	}
	return nil
}
