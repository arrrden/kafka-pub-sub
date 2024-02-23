package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/arrrden/kafka/examples/router/handlers/listings"
	"github.com/arrrden/kafka/messaging/kafka"
	"github.com/arrrden/kafka/messaging/kafka/router"
)

func main() {
	ctx := context.Background()
	k, err := kafka.NewKafkaClient("localhost:9094", nil)
	if err != nil {
		log.Fatal(err)
	}

	conn, err := k.NewConnection()
	if err != nil {
		log.Fatal(err)
	}

	defer conn.Close()

	go func() {
		count := 0
		for {
			msg, _ := kafka.NewMessage("beans", "new-listing", listings.NewListingReq{Name: fmt.Sprintf("listing=%d", count)})
			conn.Produce(ctx, "listing-recommendation-requests", msg)

			count += 1
			time.Sleep(5 * time.Second)
		}
	}()

	rtr := router.NewRouter(ctx, "boobies", k)

	listingsHandler := listings.NewListingsHandler(&listings.Listings{})

	recs := rtr.NewRouteGroup("listing-recommendation-requests", listingsHandler.DefaultHandler)
	recs.HandleMsg("new-listing", listingsHandler.NewListing)
	recs.HandleMsg("updated-listing", listingsHandler.UpdatedListing)

	if err := rtr.Listen(); err != nil {
		log.Fatalf("failed to listen: %v", err.Error())
	}

	fmt.Scanln()
	if err := rtr.Close(); err != nil {
		panic(err)
	}
}
