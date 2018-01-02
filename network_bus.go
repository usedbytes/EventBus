package EventBus

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/rpc"
	"sync"
)

// NetworkBus - object capable of subscribing to remote event buses in addition to remote event
// busses subscribing to it's local event bus. Compoed of a server and cliet
type NetworkBus struct {
	Bus
	*Client
	*Server
	service   *NetworkBusService
	remoteBus Bus
	address   string
	path      string
}

// NewNetworkBus - returns a new network bus object at the server address and path
func NewNetworkBus(address, path string) *NetworkBus {
	bus := new(NetworkBus)
	bus.remoteBus = New()
	bus.Bus = New()
	bus.Server = NewServer(address, path, bus.remoteBus)
	bus.Client = NewClient(address, path, bus.Bus)
	bus.service = &NetworkBusService{&sync.WaitGroup{}, false}
	bus.address = address
	bus.path = path
	return bus
}

// EventBus - returns wrapped event bus
func (networkBus *NetworkBus) EventBus() Bus {
	return Bus(networkBus)
}

// NetworkBusService - object capable of serving the network bus
type NetworkBusService struct {
	wg      *sync.WaitGroup
	started bool
}

// Start - helper method to serve a network bus service
func (networkBus *NetworkBus) Start() error {
	var err error
	service := networkBus.service
	clientService := networkBus.Client.service
	serverService := networkBus.Server.service
	if !service.started {
		server := rpc.NewServer()
		server.RegisterName("ServerService", serverService)
		server.RegisterName("ClientService", clientService)
		server.HandleHTTP(networkBus.path, "/debug"+networkBus.path)
		l, e := net.Listen("tcp", networkBus.address)
		if e != nil {
			err = fmt.Errorf("listen error: %v", e)
		}
		service.wg.Add(1)
		go http.Serve(l, nil)
	} else {
		err = errors.New("Server bus already started")
	}
	return err
}

// Stop - signal for the service to stop serving
func (networkBus *NetworkBus) Stop() {
	service := networkBus.service
	if service.started {
		service.wg.Done()
		service.started = false
	}
}

func (networkBus *NetworkBus) Publish(topic string, args ...interface{}) {
	networkBus.Bus.Publish(topic, args...)
	networkBus.remoteBus.Publish(topic, args...)
}

func (networkBus *NetworkBus) HasCallback(topic string) bool {
	return networkBus.Bus.HasCallback(topic) || networkBus.remoteBus.HasCallback(topic)
}
