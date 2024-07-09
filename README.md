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
	if e.OriginalTopic == "order.create" {
		fmt.Printf("%s got new order: %s \n", l.Name(), e.Payload.(string))
	} else {
		fmt.Printf("%s got canceled order: %s \n", l.Name(), e.Payload.(string))
	}
	return nil
}

type PurchaseLogic struct{}

func (l *PurchaseLogic) Name() string {
	return "purchase"
}

func (l *PurchaseLogic) OnEvent(e basic.Event) error {
	fmt.Printf("%s processed %s \n", l.Name(), e.ID)
	fmt.Printf("%s got new order: %s \n", l.Name(), e.Payload.(string))
	if e.ExpireAt > 0 {
		fmt.Printf("%s needs to handle before %s", l.Name(), time.Unix(e.ExpireAt, 0).String())
	}
	return nil
}
```

Local communication:
```go
func main() {
	bus := eventbus.NewLocalBus()
	var err error
	err = bus.Register("order.create", &InventoryLogic{}, &PurchaseLogic{})
	if err != nil {
		panic(err)
	}
	err = bus.Register("order.cancel", &InventoryLogic{})
	if err != nil {
		panic(err)
	}

	router := gin.Default()
	router.POST("/order/create", func(context *gin.Context) {
		// some business here
		bus.Notify("order.create", "123456", eventbus.WithExpireDuration(24*time.Hour))
	})
	router.POST("/order/cancel", func(context *gin.Context) {
		// some business here
		bus.Notify("order.cancel", "123456")
	})

	err = router.Run(":8000")
	if err != nil {
		log.Fatal(err)
	}
}
```

Cross-host communication:
```go
func main() {
	bus, err := eventbus.NewKafkaBus(&config.KafkaBrokerConfig{
		Endpoints:       []string{"10.211.55.4:9092"},
		TopicPartitions: 3,
		TopicPrefix:     "mysass",
	})
	if err != nil {
		panic(err)
	}
	err = bus.Register("order.create", &InventoryLogic{}, &PurchaseLogic{})
	if err != nil {
		panic(err)
	}
	err = bus.Register("order.cancel", &InventoryLogic{})
	if err != nil {
		panic(err)
	}

	router := gin.Default()
	router.POST("/order/create", func(context *gin.Context) {
		// some business here
		bus.Notify("order.create", "123456", eventbus.WithExpireDuration(24*time.Hour))
	})
	router.POST("/order/cancel", func(context *gin.Context) {
		// some business here
		bus.Notify("order.cancel", "123456")
	})

	go func() {
		err = router.Run(":8000")
		if err != nil {
			log.Fatal(err)
		}
	}()

	go func() {
		bus.Listen()
	}()

	quit := make(chan os.Signal)
	signal.Notify(quit, os.Interrupt, os.Kill)
	<-quit
}
```