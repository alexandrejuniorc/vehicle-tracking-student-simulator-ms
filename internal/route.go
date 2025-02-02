package internal

import (
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Directions struct {
	Lat float64
	Lng float64
}

type Route struct {
	ID           string
	Distance     int
	Directions   []Directions
	FreightPrice float64
}

type RouteService struct {
	mongo *mongo.Client
}

func (routeService *RouteService) CreateRoute(route Route) (Route, error) {
	// MONGO UPDATE STATEMENT
	update := bson.M{
		"$set": bson.M{
			"distance":     route.Distance,
			"directions":   route.Directions,
			"freightPrice": route.FreightPrice,
		},
	}

	// MONGO FILTER
	filter := bson.M{"_id": route.ID}

	// IF NOT EXISTS CREATE NEW ROUTE
	options := options.Update().SetUpsert(true)

	// UPDATE ROUTE
	_, err := routeService.mongo.Database("routes").Collection("rotues").UpdateOne(nil, filter, update, options)

	if err != nil {
		return Route{}, err
	}

	return route, err
}
