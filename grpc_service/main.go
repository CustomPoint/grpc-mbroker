package main

import (
	"context"
	pb "github.com/CustomPoint/grpc-mbroker/mbroker"
	"google.golang.org/grpc"
	"io"
	"log"
	"math/rand"
	"strconv"
	"sync"
	"time"
)

const (
	gRPCBrokerAddress = "localhost:9990"
	serviceName       = "service007"
	deviceName        = "device007"
)

var streamManager map[int64]chan []byte

func register(serviceTunClient pb.Broker_ServiceTunnelClient) error {
	// send registering message
	registerReq := &pb.ServiceRequest_RegisterProvider{
		Device:  deviceName,
		Service: serviceName,
	}
	if err := serviceTunClient.SendMsg(registerReq); err != nil {
		log.Fatalf("[ServiceTunnel] Sending registration failed: %+v", err)
		return err
	}
	return nil
}

func ServiceTunnel(gRPCClient pb.BrokerClient) {
	// register to the broker
	log.Printf("[ServiceTunnel] Registering as %v", serviceName)
	// define the control tunnel client
	serviceTunnelClient, err := gRPCClient.ServiceTunnel(context.Background())
	if err != nil {
		log.Fatalf("[ServiceTunnel] Error while creating service: %+v", err)
	}
	// send registering message
	if err := register(serviceTunnelClient); err != nil {
		log.Fatalf("[ServiceTunnel] Error while registering: %+v", err)
	}

	// listen for broker commands
	for {
		// start routine of receiving data
		// receive data from gRPC Stream
		brokerResp, err := serviceTunnelClient.Recv()
		if err != nil {
			if err != io.EOF {
				continue
			}
			log.Printf("[ServiceTunnel] Recv terminated: %+v", err)
			break
		}
		switch brokerResp.GetMsg().(type) {
		case *pb.ServiceResponse_ChannelOpened:
			log.Printf("[ServiceTunnel] Channel opened by the broker: %v", brokerResp)
			channelID := brokerResp.GetChannelOpened().Channel
			if _, ok := streamManager[channelID]; ok {
				log.Printf("[ServiceTunnel] Channel already opened")
				break
			}
			streamManager[channelID] = make(chan []byte)
			// TODO: generate a new session
			// send data to broker
			go func() {
				for {
					time.Sleep(5 * time.Second)
					data := []byte("<service>[" + strconv.Itoa(rand.Int()) + "]")
					dataReq := &pb.ServiceRequest{
						Msg: &pb.ServiceRequest_SendData{
							SendData: &pb.Data{
								Channel: channelID,
								Data:    data,
							},
						},
					}
					log.Printf("[ServiceTunnel] Sending data: %v <channel> [%v]", string(data), channelID)
					if err = serviceTunnelClient.SendMsg(dataReq); err != nil {
						log.Printf("[ServiceTunnel] Sending data error: %v", err)
					}
				}
			}()
		case *pb.ServiceResponse_ChannelClosed:
			log.Printf("[ServiceTunnel] Channel closed: %v", err)
			delete(streamManager, brokerResp.GetChannelClosed().Channel)
			return
		case *pb.ServiceResponse_RecvData:
			recvData := brokerResp.GetRecvData()
			log.Printf("[ServiceTunnel] Data received: %v", recvData)
			// send data to the session
			//channelID := recvData.Channel
			//data := recvData.Data
			//streamManager[channelID] <- data
		}
	}
}

func main() {
	var wg sync.WaitGroup
	// Start gRPC client
	log.Printf("[Service] Dialing to the Broker ...")
	conn, err := grpc.Dial(gRPCBrokerAddress, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatalf("[Service] did not connect: %v", err)
	}
	log.Printf("[Service] Connection established")
	defer conn.Close()

	// create gRPC control client
	gRPCBrokerClient := pb.NewBrokerClient(conn)

	// make streamManager
	streamManager = make(map[int64]chan []byte, 5)

	// create control tunnel
	wg.Add(1)
	go func() {
		ServiceTunnel(gRPCBrokerClient)
		wg.Done()
	}()

	wg.Wait()
}
