package EventBus

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/rpc"
	"sync"
)

// SubscribeType - how the client intends to subscribe
type SubscribeType int

const (
	// Subscribe - subscribe to all events
	Subscribe SubscribeType = iota
	// SubscribeOnce - subscribe to only one event
	SubscribeOnce
)

const (
	// RegisterService - Server subscribe service method
	RegisterService = "ServerService.Register"
)

// SubscribeArg - object to hold subscribe arguments from remote event handlers
type SubscribeArg struct {
	ClientAddr    string
	ClientPath    string
	ServiceMethod string
	SubscribeType SubscribeType
	Topic         string
}

type ClientAddress struct {
	addr, path string
}

type Subscriber struct {
	client *rpc.Client
	topics map[string]*SubscribeArg
}

// Server - object capable of being subscribed to by remote handlers
type Server struct {
	eventBus    Bus
	address     string
	path        string
	subscribers map[ClientAddress]*Subscriber
	service     *ServerService
}

// NewServer - create a new Server at the address and path
func NewServer(address, path string, eventBus Bus) *Server {
	server := new(Server)
	server.eventBus = eventBus
	server.address = address
	server.path = path
	server.subscribers = make(map[ClientAddress]*Subscriber)
	server.service = &ServerService{server, &sync.WaitGroup{}, false}
	return server
}

// EventBus - returns wrapped event bus
func (server *Server) EventBus() Bus {
	return server.eventBus
}

func (server *Server) rpcCallback(sub *Subscriber, subscribeArg *SubscribeArg) func(args ...interface{}) {
	return func(args ...interface{}) {
		if sub.client == nil {
			client, err := rpc.DialHTTPPath("tcp", subscribeArg.ClientAddr, subscribeArg.ClientPath)
			if err != nil {
				fmt.Println(err)
				return
			}

			sub.client = client
		}

		clientArg := new(ClientArg)
		clientArg.Topic = subscribeArg.Topic
		clientArg.Args = args
		var reply bool
		err := sub.client.Call(subscribeArg.ServiceMethod, clientArg, &reply)
		if err != nil {
			fmt.Println("dialing: %v", err)
		}
	}
}

// HasClientSubscribed - True if a client subscribed to this server with the same topic
func (server *Server) HasClientSubscribed(arg *SubscribeArg) bool {
	client := ClientAddress{arg.ClientAddr, arg.ClientPath}

	if sub, ok := server.subscribers[client]; ok {
		_, ok := sub.topics[arg.Topic]
		return ok
	}

	return false
}

// Start - starts a service for remote clients to subscribe to events
func (server *Server) Start() error {
	var err error
	service := server.service
	if !service.started {
		rpcServer := rpc.NewServer()
		rpcServer.Register(service)
		rpcServer.HandleHTTP(server.path, "/debug"+server.path)
		l, e := net.Listen("tcp", server.address)
		if e != nil {
			err = e
			fmt.Errorf("listen error: %v", e)
		}
		service.started = true
		service.wg.Add(1)
		go http.Serve(l, nil)
	} else {
		err = errors.New("Server bus already started")
	}
	return err
}

// Stop - signal for the service to stop serving
func (server *Server) Stop() {
	service := server.service
	if service.started {
		service.wg.Done()
		service.started = false
	}
}

// ServerService - service object to listen to remote subscriptions
type ServerService struct {
	server  *Server
	wg      *sync.WaitGroup
	started bool
}

// Register - Registers a remote handler to this event bus
// for a remote subscribe - a given client address only needs to subscribe once
// event will be republished in local event bus
func (service *ServerService) Register(arg *SubscribeArg, success *bool) error {
	var sub *Subscriber
	var ok bool

	subscribers := service.server.subscribers
	client := ClientAddress{arg.ClientAddr, arg.ClientPath}

	if sub, ok = subscribers[client]; !ok {
		sub = &Subscriber{
			topics: make(map[string]*SubscribeArg),
		}
		subscribers[client] = sub
	}

	if _, ok := sub.topics[arg.Topic]; !ok {
		rpcCallback := service.server.rpcCallback(sub, arg)
		sub.topics[arg.Topic] = arg
		switch arg.SubscribeType {
		case Subscribe:
			service.server.eventBus.Subscribe(arg.Topic, rpcCallback)
		case SubscribeOnce:
			service.server.eventBus.SubscribeOnce(arg.Topic, rpcCallback)
		}
	}

	*success = true
	return nil
}
