package broker

import (
	"github.com/visforest/eventbus/basic"
)

type Broker interface {
	Addrs() []string                            // broker addresses
	Connect() error                             // connect to the broker
	Disconnect() error                          // disconnect from the broker
	Subscribe(string, basic.EventHandler) error // subscribe topic
	Unsubscribe(string) error                   // unsubscribe topic
	Subscribed() []string                       // return subscribed topics
	Write(basic.Event) error                    // write an event into broker
	Consume()                                   // read event msgs and handle
}
