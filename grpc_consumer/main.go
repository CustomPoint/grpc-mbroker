package main

import (
	"context"
	"fmt"
	pb "github.com/CustomPoint/grpc-mbroker/mbroker"
	"google.golang.org/grpc"
	"io"
	"log"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"time"
)

const (
	gRPCBrokerAddress = "localhost:9990"
	serviceName       = "service007"
	consumerName      = "consumer007"
)

func openChannel(consumerTunClient pb.Broker_ConsumerTunnelClient, channel int64) error {
	// send request to open channel
	chreq := &pb.ConsumeRequest{
		Msg: &pb.ConsumeRequest_OpenChannel{
			OpenChannel: &pb.OpenChannel{Service: serviceName,
				Channel: channel,
			},
		},
	}
	log.Printf("[openChannel] Requesting to open channel")
	if err := consumerTunClient.Send(chreq); err != nil {
		log.Fatalf("[openChannel] Send request to open channel failed: %v", err)
		return err
	}
	// wait for response from broker
	for {
		brokerResp, err := consumerTunClient.Recv()
		if err != nil {
			log.Printf("[openChannel] Error while receiving data: %v", err)
			return err
		}
		switch brokerResp.GetMsg().(type) {
		case *pb.ConsumeResponse_ChannelOpened:
			log.Printf("[openChannel] Channel opened: %v", channel)
			return nil
		case *pb.ConsumeResponse_ChannelClosed:
			return fmt.Errorf("[openChannel] Channel was closed")
		}
	}
}

func ConsumerTunnel(gRPCClient pb.BrokerClient, channel int64) {
	consumerTunClient, err := gRPCClient.ConsumerTunnel(context.Background())
	if err != nil {
		log.Fatalf("[ConsumerTunnel] could not create: %+v", err)
	}
	// send request to open channel
	if err = openChannel(consumerTunClient, channel); err != nil {
		log.Fatalf("%+v", err)
	}
	log.Printf("[openChannel] Channel opened successfuly.")
	// send data to broker
	go func() {
		for {
			time.Sleep(10 * time.Second)
			data := []byte("<consumer>[" + strconv.Itoa(rand.Int()) + "]")
			dataReq := &pb.ConsumeRequest{
				Msg: &pb.ConsumeRequest_SendData{
					SendData: &pb.Data{

						Channel: channel,
						Data:    data,
					},
				},
			}
			log.Printf("[ConsumerTunnel] Sending data: %+v", string(data))
			if err = consumerTunClient.SendMsg(dataReq); err != nil {
				log.Printf("[ConsumerTunnel] Sending data error: %v", err)
				return
			}
		}
	}()
	// get data from the Broker and print it out
	for {
		// receive data from gRPC Stream
		brokerResp, err := consumerTunClient.Recv()
		if err != nil {
			if err != io.EOF {
				continue
			}
			log.Printf("[ConsumerTunnel] Recv terminated: %+v", err)
			break
		}
		switch brokerResp.GetMsg().(type) {
		case *pb.ConsumeResponse_ChannelClosed:
			log.Fatalf("[openChannel] Channel closed: %v", err)
			return
		case *pb.ConsumeResponse_RecvData:
			recvData := brokerResp.GetRecvData()
			log.Printf("[ConsumerTunnel] Data received: %+v", string(recvData.Data))
		}
	}
}

func main() {
	// get channel from args
	channel, err := strconv.Atoi(os.Args[1])
	if err != nil {
		log.Fatalf("[Consumer] Please privide an integer: %v", err.Error())
	}
	log.Printf("[Consumer] You have chosen channel: %v", channel)

	var wg sync.WaitGroup
	// Start gRPC client
	conn, err := grpc.Dial(gRPCBrokerAddress, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatalf("[Consumer] did not connect: %v", err)
	}
	log.Printf("[Consumer] Connection established to broker: %v", gRPCBrokerAddress)
	defer conn.Close()

	// create gRPC control client
	gRPCBrokerClient := pb.NewBrokerClient(conn)

	// create control tunnel
	wg.Add(1)
	go func() {
		ConsumerTunnel(gRPCBrokerClient, int64(channel))
		wg.Done()
	}()

	wg.Wait()
}
