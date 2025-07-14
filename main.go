package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/logger"
	"github.com/redis/go-redis/v9"
)

const (
	PAYMENT_PROCESSOR_DEFAULT_ID  = 1
	PAYMENT_PROCESSOR_FALLBACK_ID = 2

	PAYMENTS_REDIS_KEY               = "payments"
	PENDING_PAYMENTS_REDIS_KEY       = "pending_payments"
	BEST_PAYMENT_PROCESSOR_REDIS_KEY = "best_payment_processor"

	NUMBER_OF_WORKERS = 16

	RFC3339Milli = "2006-01-02T15:04:05.000Z07:00"
)

var (
	redisClient           *redis.Client
	ctx                   = context.Background()
	pendingPaymentsQueue  chan CreatePaymentReq
)

type CreatePaymentReq struct {
	CorrelationID string  `json:"correlationId"`
	Amount        float64 `json:"amount"`
}

type PaymentSummaryReq struct {
	From *string `query:"from"`
	To   *string `query:"to"`
}

type PaymentSummaryRes struct {
	Default  PaymentProcessorSummary `json:"default"`
	Fallback PaymentProcessorSummary `json:"fallback"`
}

type PaymentProcessorSummary struct {
	TotalRequests int     `json:"totalRequests"`
	TotalAmount   float64 `json:"totalAmount"`
}

type PaymentProcessorReq struct {
	CorrelationID    string  `json:"correlationId"`
	Amount           float64 `json:"amount"`
	RequestedAt      string  `json:"requestedAt"`
	PaymentProcessor *int    `json:"paymentProcessor"`
}

type PaymentProcessorHealthCheckRes struct {
	Failing         bool `json:"failing"`
	MinResponseTime int  `json:"minResponseTime"`
}

func connectToRedis() (*redis.Client, error) {
	addr := os.Getenv("REDIS_ADDR")
	password := os.Getenv("REDIS_PASSWORD")

	rdb := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
		DB:       0,
	})

	err := rdb.Ping(ctx).Err()
	if err != nil {
		return nil, err
	}

	log.Println("Connected to Redis at", addr)
	return rdb, nil
}

func storePayment(p PaymentProcessorReq, processorID int) error {
	requestedAt, err := time.Parse(RFC3339Milli, p.RequestedAt)
	if err != nil {
		return fmt.Errorf("invalid time format: %w", err)
	}

	p.PaymentProcessor = &processorID

	data, err := json.Marshal(p)
	if err != nil {
		return fmt.Errorf("marshal error: %w", err)
	}

	zAddRes := redisClient.ZAdd(ctx, PAYMENTS_REDIS_KEY, redis.Z{
		Score:  float64(requestedAt.UnixMilli()),
		Member: data,
	})

	if zAddRes.Err() != nil {
		return zAddRes.Err()
	}

	log.Printf("Stored payment in Redis [CorrelationID=%s, ProcessorID=%d]", p.CorrelationID, processorID)

	return nil
}

func getBestProcessor() *int {
	val, err := redisClient.Get(ctx, BEST_PAYMENT_PROCESSOR_REDIS_KEY).Result()
	if err != nil {
		log.Println("Could not fetch best processor from Redis:", err)

		return nil
	}

	bestProcessorId, err := strconv.Atoi(val)
	if err != nil {
		log.Println("Invalid processor ID in Redis:", err)

		return nil
	}

	log.Printf("Best processor selected: %d", bestProcessorId)

	return &bestProcessorId
}

func callExternalProcessorHealth(processorID int) (*PaymentProcessorHealthCheckRes, error) {
	var url string
	if processorID == PAYMENT_PROCESSOR_DEFAULT_ID {
		url = os.Getenv("PAYMENT_PROCESSOR_DEFAULT_URL")
	} else {
		url = os.Getenv("PAYMENT_PROCESSOR_FALLBACK_URL")
	}

	resp, err := http.Get(url + "/payments/service-health")
	if err != nil {
		log.Printf("Health check failed for processor %d: %v", processorID, err)

		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		log.Printf("Health check for processor %d returned status %d", processorID, resp.StatusCode)

		return nil, fmt.Errorf("status %d", resp.StatusCode)
	}

	var health PaymentProcessorHealthCheckRes
	if err := json.NewDecoder(resp.Body).Decode(&health); err != nil {

		return nil, fmt.Errorf("decode error: %w", err)
	}

	log.Printf("Processor %d health: failing=%v, minResponseTime=%d", processorID, health.Failing, health.MinResponseTime)

	return &health, nil
}

func callExternalProcessorCreatePayment(p *CreatePaymentReq, processorID int) (*PaymentProcessorReq, error) {
	b := &PaymentProcessorReq{
		Amount:        p.Amount,
		CorrelationID: p.CorrelationID,
		RequestedAt:   time.Now().UTC().Format(RFC3339Milli),
	}

	payload, err := json.Marshal(b)
	if err != nil {
		return nil, fmt.Errorf("marshal error: %w", err)
	}

	var url string
	if processorID == PAYMENT_PROCESSOR_DEFAULT_ID {
		url = os.Getenv("PAYMENT_PROCESSOR_DEFAULT_URL")
	} else {
		url = os.Getenv("PAYMENT_PROCESSOR_FALLBACK_URL")
	}

	resp, err := http.Post(url+"/payments", "application/json", bytes.NewBuffer(payload))
	if err != nil {
		return nil, fmt.Errorf("request error: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)

		return nil, fmt.Errorf("processor returned %d: %s", resp.StatusCode, string(body))
	}

	log.Printf("Payment sent to processor %d [CorrelationID=%s]", processorID, p.CorrelationID)

	return b, nil
}

func processPayment(p CreatePaymentReq, pendingPayments chan CreatePaymentReq) {
	processorID := getBestProcessor()

	if processorID == nil {
		log.Printf("No available processor. Retrying later [CorrelationID=%s]", p.CorrelationID)
		go func(p CreatePaymentReq) {
			time.Sleep(2 * time.Second)
			pendingPayments <- p
		}(p)
		return
	}

	req, err := callExternalProcessorCreatePayment(&p, *processorID)
	if err != nil {
		log.Printf("Payment request failed [CorrelationID=%s]: %v", p.CorrelationID, err)
		go func(p CreatePaymentReq) {
			time.Sleep(2 * time.Second)
			pendingPayments <- p
		}(p)
		return
	}

	if err := storePayment(*req, *processorID); err != nil {
		log.Printf("Failed to store payment [CorrelationID=%s]: %v", p.CorrelationID, err)
		return
	}
}

func startWorkers(pendingPayments chan CreatePaymentReq) {
	for i := 0; i < NUMBER_OF_WORKERS; i++ {
		go func(workerID int) {
			log.Printf("Worker %d started", workerID)
			for job := range pendingPayments {
				log.Printf("Worker %d processing CorrelationID=%s", workerID, job.CorrelationID)
				processPayment(job, pendingPayments)
			}
		}(i)
	}
}

func startHealthMonitor() {
	h, err := os.Hostname()
	if err != nil || h != "api01" {
		return
	}

	log.Println("Starting health monitor...")

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			processorIds := [2]int{PAYMENT_PROCESSOR_DEFAULT_ID, PAYMENT_PROCESSOR_FALLBACK_ID}
			processorResponses := make(map[int]int)

			for _, processorID := range processorIds {
				response, err := callExternalProcessorHealth(processorID)
				if err != nil || response.Failing {
					processorResponses[processorID] = math.MaxInt
				}
				processorResponses[processorID] = response.MinResponseTime
			}

			var bestProcessor int
			var bestTime int
			first := true

			for id, time := range processorResponses {
				if first || time < bestTime {
					bestProcessor = id
					bestTime = time
					first = false
				}
			}

			if !first {
				redisClient.Set(ctx, BEST_PAYMENT_PROCESSOR_REDIS_KEY, bestProcessor, 0)
				log.Printf("Updated best processor to %d (MinResponseTime=%dms)", bestProcessor, bestTime)
			}
		}
	}
}

func paymentsSummary(c *fiber.Ctx) error {
	p := new(PaymentSummaryReq)

	if err := c.QueryParser(p); err != nil {
		return err
	}

	fromStr := c.Query("from")
	toStr := c.Query("to")

	var (
		fromTime *time.Time
		toTime   *time.Time
		err      error
	)

	parseTime := func(value string) (*time.Time, error) {
		layouts := []string{
			time.RFC3339,
			"2006-01-02T15:04:05",
		}
		for _, layout := range layouts {
			t, err := time.Parse(layout, value)
			if err == nil {
				return &t, nil
			}
		}
		return nil, fmt.Errorf("invalid datetime: %s", value)
	}

	if fromStr != "" {
		if fromTime, err = parseTime(fromStr); err != nil {
			return fiber.NewError(fiber.StatusBadRequest, "Invalid 'from' datetime format")
		}
	}

	if toStr != "" {
		if toTime, err = parseTime(toStr); err != nil {
			return fiber.NewError(fiber.StatusBadRequest, "Invalid 'to' datetime format")
		}
	}

	var minScore, maxScore string
    if fromTime != nil {
        minScore = fmt.Sprintf("%d", fromTime.UnixMilli())
    } else {
        minScore = "-inf"
    }
    if toTime != nil {
        maxScore = fmt.Sprintf("%d", toTime.UnixMilli())
    } else {
        maxScore = "+inf"
    }

    results, err := redisClient.ZRangeByScore(ctx, PAYMENTS_REDIS_KEY, &redis.ZRangeBy{
        Min: minScore,
        Max: maxScore,
    }).Result()

    if err != nil {
        return fiber.NewError(fiber.StatusInternalServerError, "Failed to fetch payments")
    }

	var payments []PaymentProcessorReq
    for _, res := range results {
        var p PaymentProcessorReq
        if err := json.Unmarshal([]byte(res), &p); err != nil {
            continue
        }
        payments = append(payments, p)
    }

	response := PaymentSummaryRes{
		Default: PaymentProcessorSummary{
			TotalRequests: 0,
			TotalAmount:   0.0,
		},
		Fallback: PaymentProcessorSummary{
			TotalRequests: 0,
			TotalAmount:   0.0,
		},
	}

	 for _, payment := range payments {
		if payment.PaymentProcessor == nil {
			continue
		}

		if *payment.PaymentProcessor == PAYMENT_PROCESSOR_DEFAULT_ID {
			response.Default.TotalAmount += payment.Amount
			response.Default.TotalRequests ++
		} else {
			response.Fallback.TotalAmount += payment.Amount
			response.Fallback.TotalRequests ++
		}
	 }

	return c.JSON(response)
}

func purgePayments(c *fiber.Ctx) error {
	log.Println("Purging all payments from Redis")

	redisClient.Del(ctx, PAYMENTS_REDIS_KEY)

	return c.SendStatus(fiber.StatusNoContent)
}

func createPayment(c *fiber.Ctx) error {
	p := new(CreatePaymentReq)

	if err := c.BodyParser(p); err != nil {
		log.Println("Invalid payment request JSON")

		return fiber.NewError(fiber.StatusBadRequest, "Invalid JSON")
	}

	if p.CorrelationID == "" || p.Amount <= 0 {
		log.Println("Missing or invalid payment fields")

		return fiber.NewError(fiber.StatusBadRequest, "Missing or invalid fields")
	}

	select {
	case pendingPaymentsQueue <- *p:
		log.Printf("Payment received and queued [CorrelationID=%s, Amount=%.2f]", p.CorrelationID, p.Amount)
	default:
		log.Println("Payment queue full, rejecting request")

		return fiber.NewError(fiber.StatusServiceUnavailable, "Payment queue is full, try again later")
	}

	return c.SendStatus(fiber.StatusNoContent)
}

func main() {
	var err error
	redisClient, err = connectToRedis()
	if err != nil {
		log.Fatalf("Failed to connect to Redis: %v", err)
	}

	go startHealthMonitor()

	pendingPaymentsQueue = make(chan CreatePaymentReq, 50000)
	startWorkers(pendingPaymentsQueue)

	app := fiber.New()
	app.Use(logger.New())

	app.Post("/payments", createPayment)
	app.Get("/payments-summary", paymentsSummary)
	app.Post("/purge-payments", purgePayments)

	log.Println("Server running on :8080")
	log.Fatal(app.Listen(":8080"))
}
