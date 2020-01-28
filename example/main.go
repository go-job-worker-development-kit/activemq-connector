package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"os"
	"time"

	"github.com/go-job-worker-development-kit/activemq-connector"
	"github.com/go-job-worker-development-kit/jobworker"
	"github.com/go-stomp/stomp"
	uuid "github.com/satori/go.uuid"
)

func main() {

	addr := os.Getenv("ACTIVEMQ_ADDR")
	username := os.Getenv("ACTIVEMQ_USERNAME")
	password := os.Getenv("ACTIVEMQ_PASSWORD")

	s := &activemq.Setting{
		Network: "tcp",
		Addr:    addr,
		Config:  &tls.Config{},
		Opts: []func(*stomp.Conn) error{
			stomp.ConnOpt.Login(username, password),
		},
		AckMode:       stomp.AckClient,
		Persistent:    true,
		NumMaxRetries: 3,
	}

	conn, err := activemq.Open(s)
	if err != nil {
		fmt.Println("open conn error:", err)
		return
	}
	defer func() {
		err := conn.Close()
		if err != nil {
			fmt.Println("close conn error:", err)
		}
	}()

	go func() {
		for {
			_, err := conn.Enqueue(context.Background(), &jobworker.EnqueueInput{
				Queue:   "test",
				Payload: "hello: " + uuid.NewV4().String(),
			})
			if err != nil {
				fmt.Println("could not enqueue a job", err)
			}

			time.Sleep(3 * time.Second)
		}
	}()

	done := make(chan struct{})

	go func() {
		out, err := conn.Subscribe(context.Background(), &jobworker.SubscribeInput{Queue: "test"})
		if err != nil {
			fmt.Println("receive jobs error:", err)
		}
		for job := range out.Subscription.Queue() {
			printJob(job)
			err := job.Complete(context.Background())
			if err != nil {
				fmt.Println("complete jobs error:", err)
			}
		}
		close(done)
	}()

	<-done

}

func printJob(job *jobworker.Job) {
	fmt.Println("# ----------")
	for k, v := range job.Metadata {
		fmt.Println(k, ":", v)
	}
	fmt.Println("# ----------")
	fmt.Println("Body :", job.Args)
	fmt.Println("# ----------")
	fmt.Println("Queue :", job.Queue)
	fmt.Println("# ----------")
}
