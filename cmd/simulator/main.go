package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/alexandrejuniorc/vehicle-tracking-student-simulator-ms/internal"
	"github.com/segmentio/kafka-go"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func main() {
	mongoURI := getEnv("MONGO_URI", "mongodb://admin:admin@mongo:27017/routes?authSource=admin")
	kafkaBroker := getEnv("KAFKA_BROKER", "kafka:9092")
	kafkaRouteTopic := getEnv("KAFKA_ROUTE_TOPIC", "route")
	kafkaFreightTopic := getEnv("KAFKA_FREIGHT_TOPIC", "freight")
	kafkaSimulationTopic := getEnv("KAFKA_SIMULATION_TOPIC", "simulation")
	kafkaGroupID := getEnv("KAFKA_GROUP_ID", "route-group")

	mongoConnection, err := mongo.Connect(context.Background(), options.Client().ApplyURI(mongoURI))

	if err != nil {
		panic(err)
	}

	freightService := internal.NewFreightService()
	routeService := internal.NewRouteService(mongoConnection, freightService)

	channelDriverMoved := make(chan *internal.DriverMovedEvent)

	freightWriter := &kafka.Writer{
		Addr:     kafka.TCP(kafkaBroker),
		Topic:    kafkaFreightTopic,
		Balancer: &kafka.LeastBytes{},
	}

	simulatorWriter := &kafka.Writer{
		Addr:     kafka.TCP(kafkaBroker),
		Topic:    kafkaSimulationTopic,
		Balancer: &kafka.LeastBytes{},
	}

	routeReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{kafkaBroker},
		Topic:   kafkaRouteTopic, // Topic name
		GroupID: kafkaGroupID,    // Consumer group name
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

func getEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}
