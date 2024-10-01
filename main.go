package main

import (
	"context"
	"database/sql"
	"fmt"
	"go.appointy.com/waqt/appointment/pb"
	"go.opencensus.io/plugin/ocgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"log"
)

const (
	Parent               = "g/c/l"
	URL                  = ":"
	PORT                 = "50051"
	SkipRightHeaderKey   = "X-UaHsPdmE-Header"
	SkipRightHeaderValue = "UhPdmFtUmFaGyZQ"
	DBConnectionString   = "postgres://karthikeyan@appointy.com:@127.0.0.1:9092/mathapp2203?sslmode=disable"
	parent               = "grp_01HA9WW1JPRN80YE0DS6ZJJN88/com_01HK7BZPNFQ3ZAND7R65VBNGW0/loc_01HSB4YH8DZZTNJ74AHHS2Q5HM"
)

type Script struct {
	Parent             string
	DB                 *sql.DB
	AppointmentsClient pb.AppointmentsClient
}

func NewScript(db *sql.DB, appointmentsClient pb.AppointmentsClient) *Script {
	return &Script{
		Parent:             Parent,
		DB:                 db,
		AppointmentsClient: appointmentsClient,
	}
}

func initScript() (*Script, error) {
	conn, err := grpc.Dial(URL+PORT,
		grpc.WithInsecure(),
		grpc.WithBlock(),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(1000000000)), // 1GB
		grpc.WithStatsHandler(&ocgrpc.ClientHandler{}),
		grpc.WithChainUnaryInterceptor(
			func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
				ctx = metadata.AppendToOutgoingContext(ctx, SkipRightHeaderKey, SkipRightHeaderValue)
				ctx = metadata.AppendToOutgoingContext(ctx, "id", "background") // to skip rights in grpc
				return invoker(ctx, method, req, reply, cc, opts...)
			},
		))
	if err != nil {
		log.Fatalf("failed to dial: %v", err)
	}

	db, err := sql.Open("postgres", DBConnectionString)
	if err != nil {
		log.Fatalf("failed to open db: %v", err)
	}

	if err := db.Ping(); err != nil {
		log.Fatalf("failed to ping db: %v", err)
	}

	client := pb.NewAppointmentsClient(conn)
	return NewScript(db, client), nil
}

func main() {
	script, err := initScript()
	if err != nil {
		log.Fatalf("failed to initialize script: %v", err)
	}

	//appointments := make([]*AppointmentWithParentInfo, 0, 250)
	recurringAppointments := make([]*AppointmentWithParentInfo, 0, 250)

	hasNext := true

	//for hasNext {
	//	appointment, hnxt, err := script.ListAppointment(parent, 250, len(appointments))
	//	if err != nil {
	//		log.Fatalf("failed to list appointments: %v", err)
	//	}
	//
	//	err = script.UpdateAppointment(appointment)
	//	if err != nil {
	//		log.Fatalf("failed to update appointments on offset %s, err: %v", len(appointments), err)
	//	}
	//	appointments = append(appointments, appointment...)
	//	fmt.Println("appointment offset: ", len(appointments), " completed")
	//	hasNext = hnxt
	//}

	hasNext = true
	for hasNext {
		appointment, hnxt, err := script.ListRecurringAppointment(parent, 250, len(recurringAppointments))
		if err != nil {
			log.Fatalf("failed to list recurring appointments: %v", err)
		}

		err = script.UpdateRecurringAppointment(appointment)
		if err != nil {
			log.Fatalf("failed to update recurring appointments on offset %s, err: %v", len(recurringAppointments), err)
		}

		recurringAppointments = append(recurringAppointments, appointment...)
		fmt.Println("Recurring appointment offset: ", len(recurringAppointments), " completed")
		hasNext = hnxt
	}

}
