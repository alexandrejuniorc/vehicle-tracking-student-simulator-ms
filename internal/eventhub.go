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
	routeService             *RouteService
	mongoClient              *mongo.Client
	channelDriverMoved       chan *DriverMovedEvent
	channelFreightCalculated chan *FreightCalculatedEvent
	freightWriter            *kafka.Writer
	simulationWriter         *kafka.Writer
}

func NewEventHub(
	routeService *RouteService,
	mongoClient *mongo.Client,
	channelDriverMoved chan *DriverMovedEvent,
	channelFreightCalculated chan *FreightCalculatedEvent,
	freightWriter,
	simulationWriter *kafka.Writer,
) *EventHub {
	return &EventHub{
		routeService:             routeService,
		mongoClient:              mongoClient,
		channelDriverMoved:       channelDriverMoved,
		channelFreightCalculated: channelFreightCalculated,
		freightWriter:            freightWriter,
		simulationWriter:         simulationWriter,
	}
}

func (eventHub *EventHub) HandleEvent(message []byte) error {
	var baseEvent struct {
		EventName string `json:"event"`
	}

	if err := json.Unmarshal(message, &baseEvent); err != nil {
		return fmt.Errorf("error unmarshaling base event: %w", err)
	}

	switch baseEvent.EventName {
	case "RouteCreated":
		var event RouteCreatedEvent
		if err := json.Unmarshal(message, &event); err != nil {
			return fmt.Errorf("error unmarshaling RouteCreatedEvent: %w", err)
		}
		return eventHub.HandleRouteCreated(event)

	case "DeliveryStarted":
		var event DeliveryStartedEvent
		if err := json.Unmarshal(message, &event); err != nil {
			return fmt.Errorf("error unmarshaling DeliveryStartedEvent: %w", err)
		}
		return eventHub.HandleDeliveryStarted(event)

	default:
		return errors.New("unknown event")
	}
}

func (eventHub *EventHub) HandleRouteCreated(event RouteCreatedEvent) error {
	freightCalculatedEvent, err := RouteCreatedHanlder(&event, eventHub.routeService, eventHub.mongoClient)
	if err != nil {
		return err
	}
	fmt.Printf("FreightCalculatedEvent created: %+v\n", freightCalculatedEvent)

	value, _ := json.Marshal(freightCalculatedEvent) // Convert event to JSON

	if err := eventHub.freightWriter.WriteMessages(context.Background(), kafka.Message{
		Key:   []byte(freightCalculatedEvent.RouteID),
		Value: value,
	}); err != nil {
		fmt.Printf("Error producing FreightCalculatedEvent: %v\n", err)
	}
	return nil
}

func (eventHub *EventHub) HandleDeliveryStarted(event DeliveryStartedEvent) error {
	err := DeliveryStartedHandler(&event, eventHub.routeService, eventHub.mongoClient, eventHub.channelDriverMoved)
	if err != nil {
		return err
	}

	go func() {
		for {
			select {
			case movedEvent := <-eventHub.channelDriverMoved:
				value, _ := json.Marshal(movedEvent)
				if err := eventHub.simulationWriter.WriteMessages(context.Background(), kafka.Message{
					Key:   []byte(movedEvent.RouteID),
					Value: value,
				}); err != nil {
					fmt.Printf("Error producing DriverMovedEvent: %v\n", err)
				}
			case <-time.After(500 * time.Millisecond):
				return
			}
		}
	}()

	return nil
}
