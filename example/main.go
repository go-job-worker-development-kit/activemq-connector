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

	s := activemq.Setting{
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
	defer func() {
		err := conn.Close()
		if err != nil {
			fmt.Println("close conn error:", err)
		}
	}()

	go func() {
		for {

			_, err := conn.EnqueueJob(context.Background(), &jobworker.EnqueueJobInput{
				Queue: "test",
				Payload: &jobworker.Payload{
					Class:           "hello",
					Args:            "hello: " + uuid.NewV4().String(),
					DelaySeconds:    0,
					DeduplicationID: "",
					GroupID:         "",
				}})
			if err != nil {
				fmt.Println("could not enqueue a job", err)
			}

			time.Sleep(3 * time.Second)
		}
	}()

	jobChan := make(chan *jobworker.Job)
	done := make(chan struct{})
	go func() {
		_, err = conn.ReceiveJobs(context.Background(), jobChan, done, &jobworker.ReceiveJobsInput{Queue: "test"})
		if err != nil {
			fmt.Println("receive jobs error:", err)
		}
	}()

	for job := range jobChan {
		printJob(job)
		err := job.Complete(context.Background())
		if err != nil {
			fmt.Println("complete jobs error:", err)
		}
	}
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
