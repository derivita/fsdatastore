// Based on https://github.com/googleapis/google-cloud-go/blob/4289a8c8e48e520ef69ddacec47283433eb7db30/firestore/client.go
package datastore

import (
	"os"
)

var globalClient *client

type client struct {
	projectID string
}

type logEntry struct {
	Severity string `json:"severity,omitempty"`
	TraceID  string `json:"logging.googleapis.com/trace"`
	SpanID   string `json:"logging.googleapis.com/spanId"`
	Message  string `json:"message"`
}

func Init(projectID string) error {
	globalClient = &client{projectID}
	return nil
}

func getClient() *client {
	if globalClient == nil {
		if err := Init(os.Getenv("GOOGLE_CLOUD_PROJECT")); err != nil {
			panic(err)
		}
	}
	return globalClient
}
