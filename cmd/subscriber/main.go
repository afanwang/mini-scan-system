package main

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"

	"cloud.google.com/go/pubsub"
	"github.com/censys/scan-takehome/pkg/scanning"
	_ "github.com/mattn/go-sqlite3"
	"google.golang.org/api/option"
)

type Subscriber interface {
	Receive(ctx context.Context, f func(context.Context, *pubsub.Message)) error
}

func main() {
	projectID := flag.String("project", "test-project", "GCP Project ID")
	topicID := flag.String("topic", "scan-topic", "GCP PubSub Topic ID")
	subName := flag.String("subscription", "subscription-scan-topic", "GCP PubSub Subscription ID")

	dbPath := flag.String("db", "scans.db", "Path to SQLite database")

	ctx := context.Background()

	// Initialize Pub/Sub client
	emulatorHost := os.Getenv("PUBSUB_EMULATOR_HOST")
	fmt.Printf("emulatorHost: %s\n", emulatorHost)

	db := initializeDatabase(ctx, dbPath)
	defer db.Close()

	client, err := pubsub.NewClient(ctx, *projectID,
		option.WithEndpoint(emulatorHost),
		option.WithoutAuthentication(),
	)
	if err != nil {
		log.Fatalf("Failed to create Pub/Sub client: %v", err)
	}
	defer client.Close()

	// NOTE: If each subscriber has a unique name, every subscriber will receive a copy of the message (fan-out)
	// millisString := strconv.FormatInt(time.Now().UnixMilli(), 10)
	// randNum := strconv.FormatInt(int64(rand.Intn(100)), 10)
	// subNameCurrent := fmt.Sprintf("%s-%s-%s", *subName, millisString, randNum)

	// If each subscriber has a different name, the message will be received only by one subscriber
	subNameCurrent := *subName
	sub := createSubcriptionClient(ctx, client, *topicID, subNameCurrent)

	fmt.Printf("subName: %s\n", sub)
	if sub == nil {
		log.Fatalf("Failed to create subscription client")
		return
	}

	// Start message loop
	receiveMessages(ctx, sub, db)
}

// initializeDatabase() initializes the SQLite database
// It will create the table if it doesn't exist
// The table has primary key (ip, port, service) to ensure uniqueness
func initializeDatabase(_ context.Context, dbPath *string) *sql.DB {
	db, err := sql.Open("sqlite3", *dbPath)
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}

	// Create table if not exists
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS scans (
			ip TEXT,
			port INTEGER,
			service TEXT,
			data_version INTEGER,
			last_scanned INTEGER,
			response TEXT,
			PRIMARY KEY (ip, port, service)
		)
	`)
	if err != nil {
		log.Fatalf("Failed to create table: %v", err)
	}

	return db
}

// createSubcriptionClient() creates a subscription client
func createSubcriptionClient(ctx context.Context, client *pubsub.Client, topicID, subName string) *pubsub.Subscription {

	topic := client.Topic(topicID)
	ok, err := topic.Exists(ctx)
	if err != nil || !ok {
		log.Fatalf("Failed to check if topic exists: %v", err)
	}

	// Subscribe to the topic
	// Create a subscription to the topic if it doesn't exist
	sub := client.Subscription(subName)
	ok, err = sub.Exists(ctx)
	if err != nil {
		log.Fatalf("Failed to check if subscription exists: %v", err)
	}
	if !ok {
		if _, err := client.CreateSubscription(ctx, subName, pubsub.SubscriptionConfig{
			Topic: topic,
		}); err != nil {
			log.Fatalf("Failed to create subscription: %v", err)
		}
		log.Printf("Subscription %s created.\n", subName)
	}

	return sub
}

// receiveMessages() receives messages from the subscription
// notice that subscriber is an interface so it is easy to mock
func receiveMessages(ctx context.Context, sub Subscriber, db *sql.DB) {
	// Start listening for messages
	err := sub.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
		processMessage(ctx, msg, db)
	})
	if err != nil {
		log.Fatalf("Failed to receive messages: %v", err)
	}
}

func processMessage(ctx context.Context, msg *pubsub.Message, db *sql.DB) {
	var scan scanning.Scan
	err := json.Unmarshal(msg.Data, &scan)
	if err != nil {
		log.Printf("Error unmarshaling message: %v", err)
		msg.Nack()
		return
	}

	var response string
	switch scan.DataVersion {
	case scanning.V1:
		if v1Data, ok := scan.Data.(map[string]interface{}); ok {
			if responseBytes, ok := v1Data["response_bytes_utf8"].(string); ok {
				decoded, err := base64.StdEncoding.DecodeString(responseBytes)
				if err != nil {
					log.Printf("Error decoding base64: %v", err)
					msg.Nack()
					return
				}
				response = string(decoded)
			} else {
				log.Printf("Invalid V1 data format")
				msg.Nack()
				return
			}
		} else {
			log.Printf("Invalid V1 data type")
			msg.Nack()
			return
		}
	case scanning.V2:
		if v2Data, ok := scan.Data.(map[string]interface{}); ok {
			if responseStr, ok := v2Data["response_str"].(string); ok {
				response = responseStr
			} else {
				log.Printf("Invalid V2 data format")
				msg.Nack()
				return
			}
		} else {
			log.Printf("Invalid V2 data type")
			msg.Nack()
			return
		}
	default:
		log.Printf("Unknown data version: %d", scan.DataVersion)
		msg.Nack()
		return
	}

	// Update database
	_, err = db.Exec(`
				INSERT OR REPLACE INTO scans (ip, port, service, data_version, last_scanned, response)
				VALUES (?, ?, ?, ?, ?, ?)
			`, scan.Ip, scan.Port, scan.Service, scanning.Version, scan.Timestamp, response)
	if err != nil {
		log.Printf("Error updating database: %v", err)
		msg.Nack()
		return
	}

	fmt.Printf("Processed scan: %s:%d (%s): %d - %s\n", scan.Ip, scan.Port, scan.Service, scan.DataVersion, response)
	msg.Ack()
}
