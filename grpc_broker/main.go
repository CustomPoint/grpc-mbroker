package main

import (
	"fmt"
	pb "github.com/CustomPoint/grpc-mbroker/mbroker"
	"google.golang.org/grpc"
	"io"
	"log"
	"net"
	"sync"
)

const (
	brokerName        = "broker007"
	gRPCServerAddress = "localhost"
	gRPCServerPort    = "9990"
)

type streamData struct {
	incoming chan pb.Data
	outgoing chan pb.Data
}

type StreamManager struct {
	conns       map[string]map[int64]streamData // service_id -> consumer_id -> stream
	openChannel chan int64
	closed      bool
	brokerID    string
}

type BrokerServer struct {
	pb.UnimplementedBrokerServer
	StreamManager
}

func (s *BrokerServer) register(serviceName string) error {
	if _, ok := s.conns[serviceName]; ok {
		return fmt.Errorf("[ServiceTunnel] Service already registered!")
	}
	s.conns[serviceName] = make(map[int64]streamData, 5)
	log.Printf("[ServiceTunnel] Service registered: %+v", s.conns)
	return nil
}

func (s *BrokerServer) createChannel(serviceName string, channel int64) error {
	if _, ok := s.conns[serviceName]; !ok {
		return fmt.Errorf("[ServiceTunnel] Could not find service!")
	}
	s.conns[serviceName][channel] = streamData{
		incoming: make(chan pb.Data),
		outgoing: make(chan pb.Data),
	}
	log.Printf("[ServiceTunnel] Channel created: %+v", s.conns)
	return nil
}

func (s *BrokerServer) deleteChannel(serviceName string, channel int64) error {
	if _, ok := s.conns[serviceName]; !ok {
		return fmt.Errorf("[ServiceTunnel] Could not find service!")
	}
	delete(s.conns[serviceName], channel)
	log.Printf("[ServiceTunnel] Channel deleted: %+v", s.conns)
	return nil
}

func (s *BrokerServer) ServiceTunnel(serviceServer pb.Broker_ServiceTunnelServer) error {
	// create channel for error
	errChan := make(chan error)
	// register serviceServer
	dataReq := &pb.ServiceRequest_RegisterProvider{}
	err := serviceServer.RecvMsg(dataReq)
	if err != nil {
		if err == io.EOF {
			log.Printf("[ServiceTunnel] Error while registering service: %v", err)
		}
		log.Printf("[ServiceTunnel] Error while registering service: %v", err)
		return err
	}
	err = s.register(dataReq.Service)
	if err != nil {
		return err
	}

	go func() {
		for {
			// get notified about channel opening
			log.Printf("[ServiceTunnel] Waiting for a request to a channel...")
			channelID := <-s.openChannel
			resp := &pb.ServiceResponse{
				Msg: &pb.ServiceResponse_ChannelOpened{
					ChannelOpened: &pb.NotifyChannelOpened{
						Channel: channelID,
					}},
			}
			log.Printf("[ServiceTunnel] Sending request for channel to service...")
			if err = serviceServer.Send(resp); err != nil {
				log.Printf("[ServiceTunnel] Error while opening channel: %v", err)
			}

			// read(consumer) -> write(service)
			log.Printf("[ServiceTunnel] Starting read/write procedures from: service <%v> to channel <%v>")
			go func() {
				for {
					data := <-s.conns[dataReq.Service][channelID].incoming
					// send data to the consumer
					dataReply := &pb.ServiceResponse{
						Msg: &pb.ServiceResponse_RecvData{
							RecvData: &pb.Data{
								Channel: channelID,
								Data:    data.Data,
							}},
					}
					log.Printf("[ServiceTunnel] Data incomming from channel: %v", channelID)
					log.Printf("[ServiceTunnel] Sending data to %v: %v", dataReq.Service, string(data.Data))
					err := serviceServer.SendMsg(dataReply)
					if err != nil {
						log.Printf("[ServiceTunnel] Error while sending data: %v", err)
						errChan <- err
						return
					}
				}
			}()

			// read(service) -> write(consumer)
			go func() {
				for {
					// send data to the consumer
					serviceReq, err := serviceServer.Recv()
					if err != nil {
						log.Fatalf("[ServiceTunnel] Error while receiving data: %v", err)
					}
					switch serviceReq.GetMsg().(type) {
					case *pb.ServiceRequest_SendData:
						data := serviceReq.GetSendData()
						log.Printf("[ServiceTunnel] Data from service to channel: %v", data)
						s.conns[dataReq.Service][data.Channel].outgoing <- *data
					}
				}
			}()
		}
	}()

	returnedError := <-errChan
	return returnedError
}

func (s *BrokerServer) ConsumerTunnel(consumerServer pb.Broker_ConsumerTunnelServer) error {
	var consumerDetails *pb.OpenChannel

	// create channel for error
	errChan := make(chan error)

	// everything is ok, start registering reader & writer
	// read(consumer) -> write(service)
	go func() {
		for {
			consumeReq, err := consumerServer.Recv()
			if err != nil {
				log.Fatalf("[ConsumerTun] Error while receiving data: %v", err)
			}
			switch consumeReq.GetMsg().(type) {
			case *pb.ConsumeRequest_SendData:
				data := consumeReq.GetSendData()
				log.Printf("[ConsumerTun] Data from consumer: %v", data)
				serviceID := consumerDetails.Service
				channelID := consumerDetails.Channel
				s.conns[serviceID][channelID].incoming <- *data
			case *pb.ConsumeRequest_OpenChannel:
				consumerDetails = consumeReq.GetOpenChannel()
				log.Printf("[ConsumerTun] Got request to open channel on service: %+v", consumerDetails)
				serviceID := consumerDetails.Service
				channelID := consumerDetails.Channel
				_, ok := s.conns[serviceID]
				if !ok {
					// send closed channel response
					resp := &pb.ConsumeResponse{
						Msg: &pb.ConsumeResponse_ChannelClosed{
							ChannelClosed: &pb.NotifyChannelClosed{
								Channel: channelID,
							}},
					}
					_ = consumerServer.Send(resp)
					errChan <- fmt.Errorf("[ConsumerTun] service not registered: %v", serviceID)
					return
				}
				// create channel for the broker
				err = s.createChannel(serviceID, channelID)
				if err != nil {
					log.Printf("[ConsumerTun] Error while creating channel: %v", err)
					errChan <- err
					return
				}
				// send notifications to the service
				log.Printf("[ConsumerTun] Notify service: channel = %v", channelID)
				s.openChannel <- channelID

				// send open channel response
				log.Printf("[ConsumerTun] Notify consumer: %v", consumerDetails.Channel)
				resp := &pb.ConsumeResponse{
					Msg: &pb.ConsumeResponse_ChannelOpened{
						ChannelOpened: &pb.NotifyChannelOpened{
							Channel: consumerDetails.Channel,
						}},
				}
				_ = consumerServer.Send(resp)

				// read(service) -> write(consumer)
				go func() {
					for {
						log.Printf("[ConsumerTun] Awaiting data from service: %v", consumerDetails)
						data := <-s.conns[serviceID][channelID].outgoing
						// send data to the consumer
						dataReply := &pb.ConsumeResponse{
							Msg: &pb.ConsumeResponse_RecvData{
								RecvData: &pb.Data{
									Channel: consumerDetails.Channel,
									Data: data.Data,
								}},
						}
						log.Printf("[ConsumerTun] Sending data to the consumer: %+v", data)
						err := consumerServer.SendMsg(dataReply)
						if err != nil {
							log.Printf("[ConsumerTun] Error while sending data: %v", err)
							errChan <- err
							return
						}
					}
				}()

			}

		}
	}()

	returnedError := <-errChan
	log.Printf("[ConsumerTun] Error was received: %v", returnedError)
	return returnedError
}

func main() {
	var wg sync.WaitGroup
	// Start gRPC BrokerServer
	wg.Add(1)
	go func() {
		log.Printf("[BrokerServer] Starting the gRPC Server ...")
		lis, err := net.Listen("tcp", net.JoinHostPort(gRPCServerAddress, gRPCServerPort))
		if err != nil {
			log.Fatalf("[BrokerServer] failed to listen: %v", err)
		}
		s := grpc.NewServer()
		bs := BrokerServer{
			StreamManager: StreamManager{
				conns:       make(map[string]map[int64]streamData, 5),
				brokerID:    brokerName,
				openChannel: make(chan int64),
			},
		}
		pb.RegisterBrokerServer(s, &bs)

		if err := s.Serve(lis); err != nil {
			log.Fatalf("[BrokerServer] failed to serve: %v", err)
		}
		wg.Done()
	}()
	wg.Wait()
}
