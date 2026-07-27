package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"net"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

const (
	metadataAPIKey int16 = 3
	produceAPIKey  int16 = 0
	fetchAPIKey    int16 = 1
)

type EventBroker interface {
	Publish(context.Context, string, []byte) error
	Consume(context.Context, string, func([]byte))
}

type nativeKafka struct {
	bootstrapBrokers []string
	clientID         string
	correlationID    atomic.Int32
}

type kafkaMessage struct {
	offset int64
	value  []byte
}

func newNativeKafka(rawBrokers string) (*nativeKafka, error) {
	parts := strings.Split(rawBrokers, ",")
	brokers := make([]string, 0, len(parts))
	for _, part := range parts {
		broker := strings.TrimSpace(part)
		if broker == "" {
			continue
		}
		if _, _, err := net.SplitHostPort(broker); err != nil {
			return nil, fmt.Errorf("invalid Kafka broker %q: %w", broker, err)
		}
		brokers = append(brokers, broker)
	}
	if len(brokers) == 0 {
		return nil, errors.New("at least one Kafka broker is required")
	}
	return &nativeKafka{bootstrapBrokers: brokers, clientID: "cinemaabyss-events-service"}, nil
}

func (kafka *nativeKafka) Publish(ctx context.Context, topic string, value []byte) error {
	leaderAddress, err := kafka.leaderAddress(ctx, topic, 0)
	if err != nil {
		return err
	}

	correlationID := kafka.correlationID.Add(1)
	request := &bytes.Buffer{}
	writeRequestHeader(request, produceAPIKey, 0, correlationID, kafka.clientID)
	writeInt16(request, 1)
	writeInt32(request, 5000)
	writeInt32(request, 1)
	writeString(request, topic)
	writeInt32(request, 1)
	writeInt32(request, 0)

	message := encodeKafkaMessage(value)
	messageSet := &bytes.Buffer{}
	writeInt64(messageSet, 0)
	writeInt32(messageSet, int32(len(message)))
	_, _ = messageSet.Write(message)
	writeInt32(request, int32(messageSet.Len()))
	_, _ = request.Write(messageSet.Bytes())

	response, err := kafka.request(ctx, leaderAddress, request.Bytes())
	if err != nil {
		return fmt.Errorf("produce request: %w", err)
	}
	decoder := newKafkaDecoder(response)
	if _, err = decoder.int32(); err != nil {
		return err
	}
	topicCount, err := decoder.int32()
	if err != nil || topicCount < 1 {
		return errors.New("invalid produce response")
	}
	if _, err = decoder.string(); err != nil {
		return err
	}
	partitionCount, err := decoder.int32()
	if err != nil || partitionCount < 1 {
		return errors.New("invalid produce partition response")
	}
	if _, err = decoder.int32(); err != nil {
		return err
	}
	errorCode, err := decoder.int16()
	if err != nil {
		return err
	}
	if _, err = decoder.int64(); err != nil {
		return err
	}
	if errorCode != 0 {
		return kafkaError("produce", errorCode)
	}
	return nil
}

func (kafka *nativeKafka) Consume(ctx context.Context, topic string, handle func([]byte)) {
	var nextOffset int64
	for ctx.Err() == nil {
		messages, err := kafka.fetch(ctx, topic, 0, nextOffset)
		if err != nil {
			log.Printf("kafka consumer topic=%s error=%v", topic, err)
			waitForContext(ctx, 2*time.Second)
			continue
		}
		if len(messages) == 0 {
			waitForContext(ctx, 500*time.Millisecond)
			continue
		}
		for _, message := range messages {
			if message.offset < nextOffset {
				continue
			}
			handle(message.value)
			nextOffset = message.offset + 1
		}
	}
}

func (kafka *nativeKafka) fetch(ctx context.Context, topic string, partition int32, offset int64) ([]kafkaMessage, error) {
	leaderAddress, err := kafka.leaderAddress(ctx, topic, partition)
	if err != nil {
		return nil, err
	}

	correlationID := kafka.correlationID.Add(1)
	request := &bytes.Buffer{}
	writeRequestHeader(request, fetchAPIKey, 0, correlationID, kafka.clientID)
	writeInt32(request, -1)
	writeInt32(request, 1000)
	writeInt32(request, 1)
	writeInt32(request, 1)
	writeString(request, topic)
	writeInt32(request, 1)
	writeInt32(request, partition)
	writeInt64(request, offset)
	writeInt32(request, 1024*1024)

	response, err := kafka.request(ctx, leaderAddress, request.Bytes())
	if err != nil {
		return nil, fmt.Errorf("fetch request: %w", err)
	}
	decoder := newKafkaDecoder(response)
	if _, err = decoder.int32(); err != nil {
		return nil, err
	}
	topicCount, err := decoder.int32()
	if err != nil || topicCount < 1 {
		return nil, errors.New("invalid fetch response")
	}
	if _, err = decoder.string(); err != nil {
		return nil, err
	}
	partitionCount, err := decoder.int32()
	if err != nil || partitionCount < 1 {
		return nil, errors.New("invalid fetch partition response")
	}
	if _, err = decoder.int32(); err != nil {
		return nil, err
	}
	errorCode, err := decoder.int16()
	if err != nil {
		return nil, err
	}
	if errorCode != 0 {
		return nil, kafkaError("fetch", errorCode)
	}
	if _, err = decoder.int64(); err != nil {
		return nil, err
	}
	messageSet, err := decoder.bytes()
	if err != nil {
		return nil, err
	}
	return decodeMessageSet(messageSet)
}

func (kafka *nativeKafka) leaderAddress(ctx context.Context, topic string, partition int32) (string, error) {
	var lastError error
	for _, bootstrap := range kafka.bootstrapBrokers {
		address, err := kafka.lookupLeader(ctx, bootstrap, topic, partition)
		if err == nil {
			return address, nil
		}
		lastError = err
	}
	return "", fmt.Errorf("cannot find Kafka leader for %s[%d]: %w", topic, partition, lastError)
}

func (kafka *nativeKafka) lookupLeader(ctx context.Context, bootstrap, topic string, partition int32) (string, error) {
	correlationID := kafka.correlationID.Add(1)
	request := &bytes.Buffer{}
	writeRequestHeader(request, metadataAPIKey, 0, correlationID, kafka.clientID)
	writeInt32(request, 1)
	writeString(request, topic)

	response, err := kafka.request(ctx, bootstrap, request.Bytes())
	if err != nil {
		return "", fmt.Errorf("metadata request to %s: %w", bootstrap, err)
	}
	decoder := newKafkaDecoder(response)
	if _, err = decoder.int32(); err != nil {
		return "", err
	}
	brokerCount, err := decoder.int32()
	if err != nil || brokerCount < 1 {
		return "", errors.New("metadata response contains no brokers")
	}
	brokers := make(map[int32]string, brokerCount)
	for i := int32(0); i < brokerCount; i++ {
		nodeID, err := decoder.int32()
		if err != nil {
			return "", err
		}
		host, err := decoder.string()
		if err != nil {
			return "", err
		}
		port, err := decoder.int32()
		if err != nil {
			return "", err
		}
		brokers[nodeID] = net.JoinHostPort(host, strconv.Itoa(int(port)))
	}

	topicCount, err := decoder.int32()
	if err != nil {
		return "", err
	}
	for i := int32(0); i < topicCount; i++ {
		topicError, err := decoder.int16()
		if err != nil {
			return "", err
		}
		topicName, err := decoder.string()
		if err != nil {
			return "", err
		}
		partitionCount, err := decoder.int32()
		if err != nil {
			return "", err
		}
		for j := int32(0); j < partitionCount; j++ {
			partitionError, err := decoder.int16()
			if err != nil {
				return "", err
			}
			partitionID, err := decoder.int32()
			if err != nil {
				return "", err
			}
			leaderID, err := decoder.int32()
			if err != nil {
				return "", err
			}
			if err = decoder.skipInt32Array(); err != nil {
				return "", err
			}
			if err = decoder.skipInt32Array(); err != nil {
				return "", err
			}
			if topicName == topic && partitionID == partition {
				if topicError != 0 {
					return "", kafkaError("metadata topic", topicError)
				}
				if partitionError != 0 {
					return "", kafkaError("metadata partition", partitionError)
				}
				address, ok := brokers[leaderID]
				if !ok {
					return "", fmt.Errorf("metadata references unknown leader %d", leaderID)
				}
				return address, nil
			}
		}
	}
	return "", fmt.Errorf("topic %s partition %d not found", topic, partition)
}

func (kafka *nativeKafka) request(ctx context.Context, address string, body []byte) ([]byte, error) {
	dialer := &net.Dialer{Timeout: 5 * time.Second}
	connection, err := dialer.DialContext(ctx, "tcp", address)
	if err != nil {
		return nil, err
	}
	defer connection.Close()

	deadline := time.Now().Add(10 * time.Second)
	if contextDeadline, ok := ctx.Deadline(); ok && contextDeadline.Before(deadline) {
		deadline = contextDeadline
	}
	if err = connection.SetDeadline(deadline); err != nil {
		return nil, err
	}

	frame := &bytes.Buffer{}
	writeInt32(frame, int32(len(body)))
	_, _ = frame.Write(body)
	if _, err = connection.Write(frame.Bytes()); err != nil {
		return nil, err
	}

	var length int32
	if err = binary.Read(connection, binary.BigEndian, &length); err != nil {
		return nil, err
	}
	if length < 0 || length > 50*1024*1024 {
		return nil, fmt.Errorf("invalid Kafka response length %d", length)
	}
	response := make([]byte, length)
	if _, err = io.ReadFull(connection, response); err != nil {
		return nil, err
	}
	return response, nil
}

func encodeKafkaMessage(value []byte) []byte {
	payload := &bytes.Buffer{}
	_ = payload.WriteByte(0)
	_ = payload.WriteByte(0)
	writeInt32(payload, -1)
	writeInt32(payload, int32(len(value)))
	_, _ = payload.Write(value)

	message := &bytes.Buffer{}
	writeUint32(message, crc32.ChecksumIEEE(payload.Bytes()))
	_, _ = message.Write(payload.Bytes())
	return message.Bytes()
}

func decodeMessageSet(data []byte) ([]kafkaMessage, error) {
	decoder := newKafkaDecoder(data)
	messages := make([]kafkaMessage, 0)
	for decoder.remaining() >= 12 {
		offset, err := decoder.int64()
		if err != nil {
			return nil, err
		}
		messageSize, err := decoder.int32()
		if err != nil {
			return nil, err
		}
		if messageSize < 0 || int(messageSize) > decoder.remaining() {
			break
		}
		messageBytes, err := decoder.raw(int(messageSize))
		if err != nil {
			return nil, err
		}
		messageDecoder := newKafkaDecoder(messageBytes)
		if _, err = messageDecoder.uint32(); err != nil {
			return nil, err
		}
		magic, err := messageDecoder.byte()
		if err != nil || magic != 0 {
			continue
		}
		attributes, err := messageDecoder.byte()
		if err != nil {
			return nil, err
		}
		if attributes&0x07 != 0 {
			continue
		}
		if _, err = messageDecoder.nullableBytes(); err != nil {
			return nil, err
		}
		value, err := messageDecoder.nullableBytes()
		if err != nil || value == nil {
			continue
		}
		messages = append(messages, kafkaMessage{offset: offset, value: value})
	}
	return messages, nil
}

func kafkaError(operation string, code int16) error {
	description := map[int16]string{
		1: "offset out of range",
		2: "corrupt message",
		3: "unknown topic or partition",
		5: "leader not available",
		6: "not leader for partition",
	}[code]
	if description == "" {
		description = "Kafka protocol error"
	}
	return fmt.Errorf("%s: %s (code %d)", operation, description, code)
}

func waitForContext(ctx context.Context, duration time.Duration) {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}

func writeRequestHeader(buffer *bytes.Buffer, apiKey, apiVersion int16, correlationID int32, clientID string) {
	writeInt16(buffer, apiKey)
	writeInt16(buffer, apiVersion)
	writeInt32(buffer, correlationID)
	writeString(buffer, clientID)
}

func writeString(buffer *bytes.Buffer, value string) {
	writeInt16(buffer, int16(len(value)))
	_, _ = buffer.WriteString(value)
}

func writeInt16(buffer *bytes.Buffer, value int16) { _ = binary.Write(buffer, binary.BigEndian, value) }
func writeInt32(buffer *bytes.Buffer, value int32) { _ = binary.Write(buffer, binary.BigEndian, value) }
func writeInt64(buffer *bytes.Buffer, value int64) { _ = binary.Write(buffer, binary.BigEndian, value) }
func writeUint32(buffer *bytes.Buffer, value uint32) {
	_ = binary.Write(buffer, binary.BigEndian, value)
}

type kafkaDecoder struct {
	reader *bytes.Reader
}

func newKafkaDecoder(data []byte) *kafkaDecoder { return &kafkaDecoder{reader: bytes.NewReader(data)} }
func (decoder *kafkaDecoder) remaining() int    { return decoder.reader.Len() }
func (decoder *kafkaDecoder) byte() (byte, error) {
	return decoder.reader.ReadByte()
}
func (decoder *kafkaDecoder) int16() (int16, error) {
	var value int16
	err := binary.Read(decoder.reader, binary.BigEndian, &value)
	return value, err
}
func (decoder *kafkaDecoder) int32() (int32, error) {
	var value int32
	err := binary.Read(decoder.reader, binary.BigEndian, &value)
	return value, err
}
func (decoder *kafkaDecoder) int64() (int64, error) {
	var value int64
	err := binary.Read(decoder.reader, binary.BigEndian, &value)
	return value, err
}
func (decoder *kafkaDecoder) uint32() (uint32, error) {
	var value uint32
	err := binary.Read(decoder.reader, binary.BigEndian, &value)
	return value, err
}
func (decoder *kafkaDecoder) string() (string, error) {
	length, err := decoder.int16()
	if err != nil {
		return "", err
	}
	if length < 0 {
		return "", errors.New("unexpected null Kafka string")
	}
	value, err := decoder.raw(int(length))
	return string(value), err
}
func (decoder *kafkaDecoder) bytes() ([]byte, error) {
	length, err := decoder.int32()
	if err != nil {
		return nil, err
	}
	if length < 0 {
		return nil, nil
	}
	return decoder.raw(int(length))
}
func (decoder *kafkaDecoder) nullableBytes() ([]byte, error) { return decoder.bytes() }
func (decoder *kafkaDecoder) raw(length int) ([]byte, error) {
	if length < 0 || length > decoder.reader.Len() {
		return nil, io.ErrUnexpectedEOF
	}
	value := make([]byte, length)
	_, err := io.ReadFull(decoder.reader, value)
	return value, err
}
func (decoder *kafkaDecoder) skipInt32Array() error {
	length, err := decoder.int32()
	if err != nil {
		return err
	}
	if length < 0 || int64(length)*4 > int64(decoder.reader.Len()) {
		return io.ErrUnexpectedEOF
	}
	_, err = decoder.reader.Seek(int64(length)*4, io.SeekCurrent)
	return err
}
