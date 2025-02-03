package main

import (
	"context"
	"fmt"
	"log"

	"github.com/alexandrejuniorc/vehicle-tracking-student-simulator-ms/internal"
	"github.com/segmentio/kafka-go"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func main() {
	mongoStr := "mongodb://admin:admin@localhost:27017/routes?authSource=admin"
	mongoConnection, err := mongo.Connect(context.Background(), options.Client().ApplyURI(mongoStr))

	if err != nil {
		panic(err)
	}

	freightService := internal.NewFreightService()
	routeService := internal.NewRouteService(mongoConnection, freightService)

	channelDriverMoved := make(chan *internal.DriverMovedEvent)
	kafkaBroker := "localhost:9092"

	freightWriter := &kafka.Writer{
		Addr:     kafka.TCP(kafkaBroker),
		Topic:    "freight",
		Balancer: &kafka.LeastBytes{},
	}

	simulatorWriter := &kafka.Writer{
		Addr:     kafka.TCP(kafkaBroker),
		Topic:    "simulator",
		Balancer: &kafka.LeastBytes{},
	}

	// Create a new reader with the kafka broker and topic
	routeReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{kafkaBroker},
		Topic:   "route",     // Topic name
		GroupID: "simulator", // Consumer group name
	})

	hub := internal.NewEventHub(routeService, mongoConnection, channelDriverMoved, freightWriter, simulatorWriter)

	fmt.Println("Starting simulator")
	for {
		messages, err := routeReader.ReadMessage(context.Background())
		if err != nil {
			log.Printf("error: %w", err)
			continue
		}

		go func(message []byte) {
			err = hub.HandleEvent(messages.Value)
			if err != nil {
				log.Printf("error: %w", err)
			}
		}(messages.Value)
	}
}
