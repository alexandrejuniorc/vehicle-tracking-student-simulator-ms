package internal

import (
	"math"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Directions struct {
	Lat float64 `bson:"lat" json:"lat"`
	Lng float64 `bson:"lng" json:"lng"`
}

type Route struct {
	ID           string       `bson:"_id" json:"id"`
	Distance     int          `bson:"distance" json:"distance"`
	Directions   []Directions `bson:"directions" json:"directions"`
	FreightPrice float64      `bson:"freight_price" json:"freight_price"`
}

func NewRoute(id string, distance int, directions []Directions) *Route {
	return &Route{
		ID:         id,
		Distance:   distance,
		Directions: directions,
	}
}

type FreightService struct{}

func NewFreightService() *FreightService {
	return &FreightService{}
}

func (freightService *FreightService) CalculateFreight(distance int) float64 {
	// FAKE CALCULATION
	return math.Floor((float64(distance)*0.15+0.3)*100) / 100
}

type RouteService struct {
	mongo          *mongo.Client
	freightService *FreightService
}

func NewRouteService(mongo *mongo.Client, freightService *FreightService) *RouteService {
	return &RouteService{
		mongo:          mongo,
		freightService: freightService,
	}
}

func (routeService *RouteService) CreateRoute(route *Route) (*Route, error) {
	route.FreightPrice = routeService.freightService.CalculateFreight(route.Distance)

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
		return nil, err
	}

	return route, err
}

func (routeService *RouteService) GetRoute(id string) (Route, error) {
	var route Route
	filter := bson.M{"_id": id}
	err := routeService.mongo.Database("routes").Collection("routes").FindOne(nil, filter).Decode(&route) // DECODE IS USED TO CHANGE ROUTE VARIABLE TO BSON

	if err != nil {
		return Route{}, err
	}

	return route, nil
}
