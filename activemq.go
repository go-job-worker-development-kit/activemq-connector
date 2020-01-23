package activemq

import (
	"context"
	"github.com/go-job-worker-development-kit/jobworker"
	"github.com/go-stomp/stomp"
)

type Connector struct {
	name       string
	conn *stomp.Conn
}

func (c *Connector) GetName() string {
	panic("implement me")
}

func (c *Connector) ReceiveJobs(ctx context.Context, ch chan<- *jobworker.Job, input *jobworker.ReceiveJobsInput, opts ...func(*jobworker.Option)) (*jobworker.ReceiveJobsOutput, error) {
	panic("implement me")
}

func (c *Connector) EnqueueJob(ctx context.Context, input *jobworker.EnqueueJobInput, opts ...func(*jobworker.Option)) (*jobworker.EnqueueJobOutput, error) {
	panic("implement me")
}

func (c *Connector) EnqueueJobBatch(ctx context.Context, input *jobworker.EnqueueJobBatchInput, opts ...func(*jobworker.Option)) (*jobworker.EnqueueJobBatchOutput, error) {
	panic("implement me")
}

func (c *Connector) CompleteJob(ctx context.Context, input *jobworker.CompleteJobInput, opts ...func(*jobworker.Option)) (*jobworker.CompleteJobOutput, error) {
	panic("implement me")
}

func (c *Connector) FailJob(ctx context.Context, input *jobworker.FailJobInput, opts ...func(*jobworker.Option)) (*jobworker.FailJobOutput, error) {
	panic("implement me")
}

func (c *Connector) Close() error {
	panic("implement me")
}
