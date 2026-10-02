package main

import (
	"context"
	sendclover "github.com/openmirai/sendclover-sdks/go"
	"log"
	"os"
)

func main() {
	baseURL := os.Getenv("SENDCLOVER_BASE_URL")
	if baseURL == "" {
		baseURL = "http://localhost:3000"
	}
	c, err := sendclover.New(sendclover.Config{APIKey: os.Getenv("SENDCLOVER_API_KEY"), BaseURL: baseURL})
	if err != nil {
		log.Fatal(err)
	}
	email, err := c.Emails.Send(context.Background(), sendclover.EmailInput{From: "sender@example.com", To: "recipient@example.net", Subject: "Hello", Text: "Sent with SendClover"}, &sendclover.RequestOptions{IdempotencyKey: "replace-with-a-stable-application-key"})
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("queued %s", email.ID)
}
