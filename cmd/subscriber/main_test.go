package main

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"os"
	"sync"
	"testing"
	"time"

	"mini-scan/pkg/scanning"

	"cloud.google.com/go/pubsub"
	_ "github.com/mattn/go-sqlite3"
)

// MockSubscription is a mock implementation of pubsub.Subscription
type MockSubscription struct {
	messages []*pubsub.Message
	mu       sync.Mutex
}

func (ms *MockSubscription) Receive(ctx context.Context, f func(context.Context, *pubsub.Message)) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	for _, msg := range ms.messages {
		f(ctx, msg)
	}
	return nil
}

func createMockMessage(t *testing.T, ip string, port int, service string, version int, response string) *pubsub.Message {
	var data map[string]interface{}
	if version == scanning.V1 {
		data = map[string]interface{}{
			"response_bytes_utf8": response,
		}
	} else {
		data = map[string]interface{}{
			"response_str": response,
		}
	}

	scan := scanning.Scan{
		Ip:          ip,
		Port:        uint32(port),
		Service:     service,
		Timestamp:   time.Now().Unix(),
		DataVersion: version,
		Data:        data,
	}

	scanData, err := json.Marshal(scan)
	if err != nil {
		t.Fatalf("Failed to marshal scan data: %v", err)
	}

	return &pubsub.Message{Data: scanData}
}

func verifyMessageInDB(t *testing.T, db *sql.DB, ip string, port int, service, expectedResponse string) {
	var response string
	err := db.QueryRow("SELECT response FROM scans WHERE ip = ? AND port = ? AND service = ?", ip, port, service).Scan(&response)
	if err != nil {
		t.Fatalf("Failed to query database: %v", err)
	}
	if response != expectedResponse {
		t.Errorf("Expected response '%s', got '%s'", expectedResponse, response)
	}
}

func TestReceiveMessages(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Set up temporary database
	tempFile, err := os.CreateTemp("", "test_db_*.db")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tempFile.Name())

	dbPath := tempFile.Name()
	db := initializeDatabase(ctx, &dbPath)
	defer db.Close()

	// Create mock messages
	mockMessages := []*pubsub.Message{
		createMockMessage(t, "192.168.1.1", 80, "http", scanning.V1, "c2VydmljZSByZXNwb25zZTogODI="),
		createMockMessage(t, "192.168.1.2", 443, "https", scanning.V2, "service response: 33"),
	}

	// Create mock subscription
	mockSub := &MockSubscription{messages: mockMessages}

	// Run receiveMessages in a goroutine
	go receiveMessages(ctx, mockSub, db)

	// Wait for messages to be processed
	time.Sleep(2 * time.Second)

	// Verify messages in database
	verifyMessageInDB(t, db, "192.168.1.1", 80, "http", "service response: 82")
	verifyMessageInDB(t, db, "192.168.1.2", 443, "https", "service response: 33")
}

func TestInitializeDatabase(t *testing.T) {
	ctx := context.Background()
	tempFile, err := os.CreateTemp("", "test_db_*.db")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tempFile.Name())

	dbPath := tempFile.Name()
	db := initializeDatabase(ctx, &dbPath)
	defer db.Close()

	// Check if the table was created
	var tableName string
	err = db.QueryRow("SELECT name FROM sqlite_master WHERE type='table' AND name='scans'").Scan(&tableName)
	if err != nil {
		t.Fatalf("Table 'scans' was not created: %v", err)
	}

	if tableName != "scans" {
		t.Errorf("Expected table name 'scans', got '%s'", tableName)
	}
}

func TestProcessMessage(t *testing.T) {
	ctx := context.Background()

	// Set up temporary database
	tempFile, err := os.CreateTemp("", "test_db_*.db")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tempFile.Name())

	dbPath := tempFile.Name()
	db := initializeDatabase(ctx, &dbPath)
	defer db.Close()

	// Test V1 message
	v1Scan := scanning.Scan{
		Ip:          "192.168.1.1",
		Port:        80,
		Service:     "http",
		Timestamp:   time.Now().Unix(),
		DataVersion: scanning.V1,
		Data: map[string]interface{}{
			"response_bytes_utf8": base64.StdEncoding.EncodeToString([]byte("Hello, World!")),
		},
	}
	v1Data, _ := json.Marshal(v1Scan)
	v1Msg := &pubsub.Message{Data: v1Data}

	processMessage(ctx, v1Msg, db)

	// Verify V1 data in database
	var response string
	err = db.QueryRow("SELECT response FROM scans WHERE ip = ? AND port = ? AND service = ?", v1Scan.Ip, v1Scan.Port, v1Scan.Service).Scan(&response)
	if err != nil {
		t.Fatalf("Failed to query database: %v", err)
	}
	if response != "Hello, World!" {
		t.Errorf("Expected response 'Hello, World!', got '%s'", response)
	}

	// Test V2 message
	v2Scan := scanning.Scan{
		Ip:          "192.168.1.2",
		Port:        443,
		Service:     "https",
		Timestamp:   time.Now().Unix(),
		DataVersion: scanning.V2,
		Data: map[string]interface{}{
			"response_str": "SSL Handshake",
		},
	}
	v2Data, _ := json.Marshal(v2Scan)
	v2Msg := &pubsub.Message{Data: v2Data}

	processMessage(ctx, v2Msg, db)

	// Verify V2 data in database
	err = db.QueryRow("SELECT response FROM scans WHERE ip = ? AND port = ? AND service = ?", v2Scan.Ip, v2Scan.Port, v2Scan.Service).Scan(&response)
	if err != nil {
		t.Fatalf("Failed to query database: %v", err)
	}
	if response != "SSL Handshake" {
		t.Errorf("Expected response 'SSL Handshake', got '%s'", response)
	}
}

// TODO:
// This is more of an integration test rather than an unit-test because it relies on the emulator.
// To make it a real unit-test, we would need to mock the Pub/Sub client and its methods
// Something like: https://cloud.google.com/cpp/docs/reference/pubsub/latest/subscriber-mock
// func TestCreateSubscriptionClient(t *testing.T) {
// 	ctx := context.Background()
// 	projectID := "test-project-1"
// 	topicID := "test-topic-1"
// 	subName := "test-subscription-1"

// 	// Set up Pub/Sub emulator
// 	os.Setenv("PUBSUB_EMULATOR_HOST", "localhost:8085")
// 	defer os.Unsetenv("PUBSUB_EMULATOR_HOST")

// 	client, err := pubsub.NewClient(ctx, projectID,
// 		option.WithEndpoint("localhost:8085"),
// 		option.WithoutAuthentication(),
// 	)
// 	if err != nil {
// 		t.Fatalf("Failed to create Pub/Sub client: %v", err)
// 	}
// 	defer client.Close()

// 	// Create topic
// 	topic, err := client.CreateTopic(ctx, topicID)
// 	if err != nil {
// 		t.Fatalf("Failed to create topic: %v", err)
// 	}
// 	defer topic.Delete(ctx)

// 	sub := createSubcriptionClient(ctx, client, topicID, subName)
// 	if sub == nil {
// 		t.Fatalf("Failed to create subscription client")
// 	}

// 	// Check if subscription exists
// 	ok, err := sub.Exists(ctx)
// 	if err != nil {
// 		t.Fatalf("Failed to check if subscription exists: %v", err)
// 	}
// 	if !ok {
// 		t.Errorf("Subscription was not created")
// 	}
// }
