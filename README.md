# eventbus

eventbus supports local communication and cross-host communication. eventbus broadcasts event to subscribers and subscribers receive event to callback.

Install:
```
go get github.com/visforest/eventbus
```

Define your event handler:
```go
type InventoryLogic struct{}

func (l *InventoryLogic) Name() string {
	return "inventory"
}

func (l *InventoryLogic) OnEvent(e basic.Event) error {
	fmt.Printf("%s processed %s \n", l.Name(), e.ID)
	return nil
}

type PurchaseLogic struct{}

func (l *PurchaseLogic) Name() string {
	return "purchase"
}

func (l *PurchaseLogic) OnEvent(e basic.Event) error {
	fmt.Printf("%s processed %s \n", l.Name(), e.ID)
	return nil
}
```

Local communication:
```go
package main

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/visforest/eventbus"
	"github.com/visforest/eventbus/basic"
	"log"
)

func main() {
	bus := eventbus.NewLocalBus()
	bus.Register("order", &InventoryLogic{},&PurchaseLogic{})

	router := gin.Default()
	router.POST("/order/create", func(context *gin.Context) {
		// some business here
		bus.Notify("order", "123456")
	})

	err := router.Run(":8000")
	if err != nil {
		log.Fatal(err)
	}
}
```

Cross-host communication:
```go
package main

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/visforest/eventbus"
	"github.com/visforest/eventbus/basic"
	"log"
)

func main() {
	bus, err := eventbus.NewKafkaBus(&config.KafkaBrokerConfig{
		Endpoints:       []string{"127.0.0.1:9092"},
		TopicPartitions: 3,
		TopicPrefix:     "mysass",
	})
	if err != nil {
		panic(err)
	}
	if err=bus.Register("order", &InventoryLogic{}, &PurchaseLogic{});err!=nil{
		panic(err)
    }

	router := gin.Default()
	router.POST("/order/create", func(context *gin.Context) {
		// some business here
		bus.Notify("order", "123456")
	})

	err = router.Run(":8000")
	if err != nil {
		log.Fatal(err)
	}
}
```