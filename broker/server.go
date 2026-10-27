package broker

import (
	"context"
	"fmt"
	"net"
	pb "wisp/proto"

	"wisp/stream"

	"google.golang.org/grpc"
)

type Server struct {
	pb.UnimplementedEventBrokerServer
}

// gRPC function
// multithreaded from google grpc...
func (s *Server) PublishEvent(ctx context.Context, req *pb.EventRequest) (*pb.PublishResponse, error) {
	fmt.Printf("Received event on topic [%s]: %s\n", req.Topic, string(req.Payload)) //TODO: change to log()
	stream.WriteToStream([]byte(req.Payload), req.Topic)
	return &pb.PublishResponse{Success: true}, nil
}

func main() {
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		panic(err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterEventBrokerServer(grpcServer, &Server{})

	fmt.Println("gRPC Broker listening on port 50051...")
	if err := grpcServer.Serve(lis); err != nil {
		panic(err)
	}
}
