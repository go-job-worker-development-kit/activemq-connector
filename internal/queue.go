package internal

import "github.com/go-stomp/stomp"

type Queue struct {
	Name        string
	Subscription *stomp.Subscription
}
