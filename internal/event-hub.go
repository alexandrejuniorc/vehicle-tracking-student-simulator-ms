package internal

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
	"go.mongodb.org/mongo-driver/mongo"
)

type EventHub struct {
	routeService       *RouteService
	mongoClient        *mongo.Client
	channelDriverMoved chan *DriverMovedEvent
	freightWriter      *kafka.Writer
	simulatorWriter    *kafka.Writer
}

func NewEventHub(routeService *RouteService, mongoClient *mongo.Client, channelDriverMoved chan *DriverMovedEvent, freightWriter, simulatorWriter *kafka.Writer) *EventHub {
	return &EventHub{
		routeService:       routeService,
		mongoClient:        mongoClient,
		channelDriverMoved: channelDriverMoved,
		freightWriter:      freightWriter,
		simulatorWriter:    simulatorWriter,
	}
}

func (eventHub *EventHub) HandleEvent(message []byte) error {
	var baseEvent struct {
		EventName string `json:"event"`
	}

	err := json.Unmarshal(message, &baseEvent)
	if err != nil {
		return fmt.Errorf("error unmarshalling event: %w", err)
	}

	switch baseEvent.EventName {
	case "RouteCreated":
		var event RouteCreatedEvent

		err := json.Unmarshal(message, &event)
		if err != nil {
			return fmt.Errorf("error unmarshalling event: %w", err)
		}

		return eventHub.HandleRouteCreated(event)

	case "DeliveryStarted":
		var event DeliveryStartedEvent

		err := json.Unmarshal(message, &event)
		if err != nil {
			return fmt.Errorf("error unmarshalling event: %w", err)
		}

		return eventHub.HandleDeliveryStarted(event)

	default:
		return errors.New("unknown event")
	}
}

func (eventHub *EventHub) HandleRouteCreated(event RouteCreatedEvent) error {
	freightCalculatedEvent, err := RouteCreatedHanlder(&event, eventHub.routeService)
	if err != nil {
		return err
	}

	value, err := json.Marshal(freightCalculatedEvent) // Convert event to JSON
	if err != nil {
		return fmt.Errorf("error marshalling event: %w", err)
	}

	err = eventHub.freightWriter.WriteMessages(context.Background(), kafka.Message{
		Key:   []byte(freightCalculatedEvent.RouteID), // all information about the route is in the key
		Value: value,
	})
	if err != nil {
		return fmt.Errorf("error writing message to Kafka: %w", err)
	}
	return nil
}

func (eventHub *EventHub) HandleDeliveryStarted(event DeliveryStartedEvent) error {
	err := DeliveryStartedHandler(&event, eventHub.routeService, eventHub.channelDriverMoved)
	if err != nil {
		return err
	}

	go eventHub.sendDirections() // goroute - light thread managed by go

	// Read the channel and publish the event to Kafka
	return nil
}

// read the channel and publish the event to Kafka
func (eventHub *EventHub) sendDirections() {
	for {
		select {
		case movedEvent := <-eventHub.channelDriverMoved:
			value, err := json.Marshal(movedEvent)
			if err != nil {
				return
			}

			err = eventHub.simulatorWriter.WriteMessages(context.Background(), kafka.Message{
				Key:   []byte(movedEvent.RouteID),
				Value: value,
			})
			if err != nil {
				return
			}
			break
		case <-time.After(500 * time.Millisecond):
			return
		}
	}
}

// Nest.js -----> ROUTE (KAFKA TOPIC) -----> GO
// GO -----> FREIGHT (KAFKA TOPIC) -----> Nest.js
// Nest.js -----> ROUTE (KAFKA TOPIC) -----> GO (DeliveryStarted)
// GO -----> SIMULATOR (KAFKA TOPIC) -----> Nest.js
