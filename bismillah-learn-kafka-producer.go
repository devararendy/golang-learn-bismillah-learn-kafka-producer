package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/gob"
	"encoding/hex"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/oklog/ulid"
)

type Order struct {
	Token string
	NIK   string
	Topup int
}

func generateULID() string {
	// t := time.Unix(1000000, 0)
	t := time.Now().UTC()
	entropy := ulid.Monotonic(rand.New(rand.NewSource(t.UnixNano())), 0)
	tokenUlid := ulid.MustNew(ulid.Timestamp(t), entropy)
	fmt.Println("Generate ULID : " + tokenUlid.String())

	return tokenUlid.String()
	// Output: 0000XSNJG0MQJHBF4QX1EFD6Y3
}
func main() {
	fmt.Println("\tUsage\t: [NIK] [Nominal Topup]")
	fmt.Println("\teg\t: 3276052203950003 100000")

	inputNIK := os.Args[1]
	inputTopup, err := strconv.Atoi(os.Args[2])
	hashKey := sha256.New()

	hashKey.Write([]byte(inputNIK))

	hashKeySum := hex.EncodeToString(hashKey.Sum(nil))
	inputToken := generateULID()
	fmt.Printf("Sending Order => Token : %s, NIK %s, Topup : %d\n\r", inputToken, inputNIK, inputTopup)
	fmt.Printf("SHA256sum key : %s\n\r", hashKeySum)
	// argsWithProg := os.Args
	// argsWithoutProg := os.Args[1:]
	// arg := os.Args[3]
	// fmt.Println(argsWithProg)
	// fmt.Println(argsWithoutProg)
	// fmt.Println(arg)

	// --
	// The topic is passed as a pointer to the Producer, so we can't
	// use a hard-coded literal. And a variable is a nicer way to do
	// it anyway ;-)
	topic := os.Getenv("KAFKA_TOPICS")

	// --
	// Create Producer instance
	// https://docs.confluent.io/current/clients/confluent-kafka-go/index.html#NewProducer

	hostname, err := os.Hostname()
	fmt.Println("Hostname : " + hostname)
	if err != nil {
		fmt.Println(err)
		hostname = "OrderProducer"
	}
	// Store the config
	c := kafka.ConfigMap{
		"bootstrap.servers": os.Getenv("KAFKA_BROKERS"),
		"client.id":         hostname,
		"acks":              "all",
	}

	// Variable p holds the new Producer instance.
	p, e := kafka.NewProducer(&c)

	// Check for errors in creating the Producer
	if e != nil {
		if ke, ok := e.(kafka.Error); ok == true {
			switch ec := ke.Code(); ec {
			case kafka.ErrInvalidArg:
				fmt.Printf("😢 Can't create the producer because you've configured it wrong (code: %d)!\n\t%v\n\nTo see the configuration options, refer to https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md", ec, e)
			default:
				fmt.Printf("😢 Can't create the producer (Kafka error code %d)\n\tError: %v\n", ec, e)
			}
		} else {
			// It's not a kafka.Error
			fmt.Printf("😢 Oh noes, there's a generic error creating the Producer! %v", e.Error())
		}

	} else {

		// For signalling termination from main to go-routine
		termChan := make(chan bool, 1)
		// For signalling that termination is done from go-routine to main
		doneChan := make(chan bool)

		newOrder := Order{Token: inputToken, NIK: inputNIK, Topup: inputTopup}

		//encode struct Order to byte array
		var bufOrder bytes.Buffer // Stand-in for a network connection
		// var networkIn bytes.Buffer // Stand-in for a network connection
		enc := gob.NewEncoder(&bufOrder)

		// Encode (send) the value.
		err := enc.Encode(newOrder)
		if err != nil {
			log.Fatal("encode error:", err)
		}

		//decode back from byte array to struct Order
		networkIn := bytes.NewBuffer(bufOrder.Bytes())
		dec := gob.NewDecoder(networkIn)
		var verifyBytes Order
		err = dec.Decode(&verifyBytes)
		if err != nil {
			log.Fatal("decode error:", err)
		} else {
			fmt.Printf("Decode Struct : %+v\n", verifyBytes)
		}
		networkIn = nil
		// --
		// Send a message using Produce()
		// https://docs.confluent.io/current/clients/confluent-kafka-go/index.html#Producer.Produce
		//
		// Build the message
		m := kafka.Message{TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value: bufOrder.Bytes(),
			Key:   []byte(hashKeySum),
		}

		// Handle any events that we get
		go func() {
			doTerm := false
			for !doTerm {
				// The `select` blocks until one of the `case` conditions
				// are met - therefore we run it in a Go Routine.
				select {
				case ev := <-p.Events():
					// Look at the type of Event we've received
					switch ev.(type) {

					case *kafka.Message:
						// It's a delivery report
						km := ev.(*kafka.Message)
						if km.TopicPartition.Error != nil {
							fmt.Printf("☠️ Failed to send message '%v' to topic '%v'\n\tErr: %v",
								string(km.Value),
								string(*km.TopicPartition.Topic),
								km.TopicPartition.Error)
						} else {
							fmt.Printf("✅ Message '%v' delivered to topic '%v' (partition %d at offset %d)\n",
								string(km.Value),
								string(*km.TopicPartition.Topic),
								km.TopicPartition.Partition,
								km.TopicPartition.Offset)
						}

					case kafka.Error:
						// It's an error
						em := ev.(kafka.Error)
						fmt.Printf("☠️ Uh oh, caught an error:\n\t%v\n", em)
					default:
						// It's not anything we were expecting
						fmt.Printf("Got an event that's not a Message or Error 👻\n\t%v\n", ev)

					}
				case <-termChan:
					doTerm = true

				}
			}
			close(doneChan)
		}()
		// Produce the message
		if e := p.Produce(&m, nil); e != nil {
			fmt.Printf("😢 Darn, there's an error producing the message! %v", e.Error())
		}

		// --
		// Flush the Producer queue
		t := 10000
		if r := p.Flush(t); r > 0 {
			fmt.Printf("\n--\n⚠️ Failed to flush all messages after %d milliseconds. %d message(s) remain\n", t, r)
		} else {
			fmt.Println("\n--\n✨ All messages flushed from the queue")
		}
		// --
		// Stop listening to events and close the producer
		// We're ready to finish
		termChan <- true
		// wait for go-routine to terminate
		<-doneChan
		// Now we can exit
		p.Close()

	}
}
