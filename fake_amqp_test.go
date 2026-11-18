package huma_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rabbitmq/amqp091-go"
)

const (
	amqpFrameMethod    byte = 1
	amqpFrameHeader    byte = 2
	amqpFrameBody      byte = 3
	amqpFrameHeartbeat byte = 8
	amqpFrameEnd       byte = 0xCE

	amqpClassConnection uint16 = 10
	amqpClassChannel    uint16 = 20
	amqpClassQueue      uint16 = 50
	amqpClassBasic      uint16 = 60
	amqpClassConfirm    uint16 = 85

	amqpMethodConnectionStart   uint16 = 10
	amqpMethodConnectionStartOk uint16 = 11
	amqpMethodConnectionTune    uint16 = 30
	amqpMethodConnectionTuneOk  uint16 = 31
	amqpMethodConnectionOpen    uint16 = 40
	amqpMethodConnectionOpenOk  uint16 = 41
	amqpMethodConnectionClose   uint16 = 50
	amqpMethodConnectionCloseOk uint16 = 51
	amqpMethodChannelOpen       uint16 = 10
	amqpMethodChannelOpenOk     uint16 = 11
	amqpMethodChannelClose      uint16 = 40
	amqpMethodChannelCloseOk    uint16 = 41
	amqpMethodQueueDeclare      uint16 = 10
	amqpMethodQueueDeclareOk    uint16 = 11
	amqpMethodBasicQos          uint16 = 10
	amqpMethodBasicQosOk        uint16 = 11
	amqpMethodBasicConsume      uint16 = 20
	amqpMethodBasicConsumeOk    uint16 = 21
	amqpMethodBasicPublish      uint16 = 40
	amqpMethodBasicDeliver      uint16 = 60
	amqpMethodBasicAck          uint16 = 80
	amqpMethodBasicNack         uint16 = 120
	amqpMethodConfirmSelect     uint16 = 10
	amqpMethodConfirmSelectOk   uint16 = 11
	amqpFlagContentType         uint16 = 0x8000
	amqpFlagContentEncoding     uint16 = 0x4000
	amqpFlagHeaders             uint16 = 0x2000
	amqpProtocolHeaderLen              = 8
	amqpChannelMax              uint16 = 2047
	amqpFrameMax                uint32 = 131072
	amqpTestWaitTimeout                = 5 * time.Second
)

type fakePublishedMsg struct {
	Exchange   string
	RoutingKey string
	Body       string
	Headers    amqp091.Table
}

type fakeAckEvent struct {
	DeliveryTag uint64
	Nacked      bool
	Requeue     bool
}

type fakeConsumer struct {
	conn        net.Conn
	writeMu     *sync.Mutex
	channel     uint16
	queue       string
	consumerTag string
}

type fakeAMQPServer struct {
	listener  net.Listener
	mu        sync.Mutex
	conns     []net.Conn
	consumers map[string]*fakeConsumer

	published   []fakePublishedMsg
	publishedCh chan fakePublishedMsg
	ackEvents   chan fakeAckEvent

	nextDeliveryTag atomic.Uint64
	closed          atomic.Bool
}

func startFakeAMQPServer(ctx context.Context, t *testing.T) *fakeAMQPServer {
	t.Helper()
	_ = ctx

	var lc net.ListenConfig
	listener, err := lc.Listen(ctx, "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen fake AMQP server: %v", err)
	}

	srv := &fakeAMQPServer{
		listener:    listener,
		consumers:   make(map[string]*fakeConsumer),
		publishedCh: make(chan fakePublishedMsg, 32),
		ackEvents:   make(chan fakeAckEvent, 32),
	}

	var wg sync.WaitGroup
	wg.Go(func() {
		srv.acceptLoop(ctx)
	})

	t.Cleanup(func() {
		srv.close(context.Background())
		wg.Wait()
	})

	return srv
}

func (s *fakeAMQPServer) addr(ctx context.Context) string {
	_ = ctx
	return s.listener.Addr().String()
}

func (s *fakeAMQPServer) close(ctx context.Context) {
	_ = ctx
	if !s.closed.CompareAndSwap(false, true) {
		return
	}
	_ = s.listener.Close()
	s.mu.Lock()
	conns := append([]net.Conn(nil), s.conns...)
	s.mu.Unlock()
	for _, conn := range conns {
		_ = conn.Close()
	}
}

func (s *fakeAMQPServer) acceptLoop(ctx context.Context) {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			return
		}
		s.mu.Lock()
		s.conns = append(s.conns, conn)
		s.mu.Unlock()

		go s.serveConn(ctx, conn)
	}
}

func (s *fakeAMQPServer) deliver(ctx context.Context, queueName string, body []byte) error {
	deadline := time.Now().Add(amqpTestWaitTimeout)
	for time.Now().Before(deadline) {
		s.mu.Lock()
		consumer, ok := s.consumers[queueName]
		s.mu.Unlock()
		if ok {
			tag := s.nextDeliveryTag.Add(1)
			return consumer.sendDeliver(ctx, tag, body)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(5 * time.Millisecond):
		}
	}
	return fmt.Errorf("consumer for queue %q did not register in time", queueName)
}

func (c *fakeConsumer) sendDeliver(ctx context.Context, deliveryTag uint64, body []byte) error {
	_ = ctx
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	var methodBuf bytes.Buffer
	_ = writeShortstr(&methodBuf, c.consumerTag)
	_ = binary.Write(&methodBuf, binary.BigEndian, deliveryTag)
	_ = methodBuf.WriteByte(0)
	_ = writeShortstr(&methodBuf, "")
	_ = writeShortstr(&methodBuf, c.queue)
	if err := writeMethodFrame(c.conn, c.channel, amqpClassBasic, amqpMethodBasicDeliver, methodBuf.Bytes()); err != nil {
		return err
	}

	var headerBuf bytes.Buffer
	_ = binary.Write(&headerBuf, binary.BigEndian, amqpClassBasic)
	_ = binary.Write(&headerBuf, binary.BigEndian, uint16(0))
	_ = binary.Write(&headerBuf, binary.BigEndian, uint64(len(body)))
	_ = binary.Write(&headerBuf, binary.BigEndian, uint16(0))
	if err := writeFrame(c.conn, amqpFrameHeader, c.channel, headerBuf.Bytes()); err != nil {
		return err
	}

	return writeFrame(c.conn, amqpFrameBody, c.channel, body)
}

type pendingPublish struct {
	exchange   string
	routingKey string
	headers    amqp091.Table
	bodySize   uint64
	body       bytes.Buffer
}

func (s *fakeAMQPServer) serveConn(ctx context.Context, conn net.Conn) {
	defer func() {
		_ = conn.Close()
	}()

	reader := bufio.NewReader(conn)
	proto := make([]byte, amqpProtocolHeaderLen)
	if _, err := io.ReadFull(reader, proto); err != nil {
		return
	}

	var writeMu sync.Mutex
	if err := sendConnectionStart(&writeMu, conn); err != nil {
		return
	}

	confirmChannels := make(map[uint16]*uint64)
	pending := make(map[uint16]*pendingPublish)
	consumerSeq := 0

	for {
		frameType, channel, payload, err := readFrame(reader)
		if err != nil {
			return
		}
		switch frameType {
		case amqpFrameHeartbeat:
			continue
		case amqpFrameMethod:
			if len(payload) < 4 {
				return
			}
			classID := binary.BigEndian.Uint16(payload[0:2])
			methodID := binary.BigEndian.Uint16(payload[2:4])
			args := bytes.NewReader(payload[4:])

			if err := s.handleMethod(
				ctx,
				conn,
				&writeMu,
				channel,
				classID,
				methodID,
				args,
				confirmChannels,
				pending,
				&consumerSeq,
			); err != nil {
				return
			}
		case amqpFrameHeader:
			p := pending[channel]
			if p == nil || len(payload) < 14 {
				continue
			}
			p.bodySize = binary.BigEndian.Uint64(payload[4:12])
			p.headers = parseContentHeaderTable(payload[12:])
			if p.bodySize == 0 {
				if err := s.finishPublish(conn, &writeMu, channel, p, confirmChannels, pending); err != nil {
					return
				}
			}
		case amqpFrameBody:
			p := pending[channel]
			if p == nil {
				continue
			}
			p.body.Write(payload)
			if uint64(p.body.Len()) >= p.bodySize {
				if err := s.finishPublish(conn, &writeMu, channel, p, confirmChannels, pending); err != nil {
					return
				}
			}
		}
	}
}

func (s *fakeAMQPServer) handleMethod(
	ctx context.Context,
	conn net.Conn,
	writeMu *sync.Mutex,
	channel uint16,
	classID, methodID uint16,
	args *bytes.Reader,
	confirmChannels map[uint16]*uint64,
	pending map[uint16]*pendingPublish,
	consumerSeq *int,
) error {
	_ = ctx
	switch {
	case classID == amqpClassConnection && methodID == amqpMethodConnectionStartOk:
		return sendConnectionTune(writeMu, conn)
	case classID == amqpClassConnection && methodID == amqpMethodConnectionTuneOk:
		return nil
	case classID == amqpClassConnection && methodID == amqpMethodConnectionOpen:
		return sendConnectionOpenOk(writeMu, conn)
	case classID == amqpClassConnection && methodID == amqpMethodConnectionClose:
		writeMu.Lock()
		err := writeMethodFrame(conn, 0, amqpClassConnection, amqpMethodConnectionCloseOk, nil)
		writeMu.Unlock()
		return errors.Join(err, errors.New("connection closed"))
	case classID == amqpClassChannel && methodID == amqpMethodChannelOpen:
		var buf bytes.Buffer
		_ = binary.Write(&buf, binary.BigEndian, uint32(0))
		writeMu.Lock()
		err := writeMethodFrame(conn, channel, amqpClassChannel, amqpMethodChannelOpenOk, buf.Bytes())
		writeMu.Unlock()
		return err
	case classID == amqpClassChannel && methodID == amqpMethodChannelClose:
		writeMu.Lock()
		err := writeMethodFrame(conn, channel, amqpClassChannel, amqpMethodChannelCloseOk, nil)
		writeMu.Unlock()
		return err
	case classID == amqpClassConfirm && methodID == amqpMethodConfirmSelect:
		var seq uint64
		confirmChannels[channel] = &seq
		writeMu.Lock()
		err := writeMethodFrame(conn, channel, amqpClassConfirm, amqpMethodConfirmSelectOk, nil)
		writeMu.Unlock()
		return err
	case classID == amqpClassQueue && methodID == amqpMethodQueueDeclare:
		var reserved uint16
		_ = binary.Read(args, binary.BigEndian, &reserved)
		queueName, err := readShortstr(args)
		if err != nil {
			return err
		}
		var buf bytes.Buffer
		_ = writeShortstr(&buf, queueName)
		_ = binary.Write(&buf, binary.BigEndian, uint32(0))
		_ = binary.Write(&buf, binary.BigEndian, uint32(0))
		writeMu.Lock()
		err = writeMethodFrame(conn, channel, amqpClassQueue, amqpMethodQueueDeclareOk, buf.Bytes())
		writeMu.Unlock()
		return err
	case classID == amqpClassBasic && methodID == amqpMethodBasicQos:
		writeMu.Lock()
		err := writeMethodFrame(conn, channel, amqpClassBasic, amqpMethodBasicQosOk, nil)
		writeMu.Unlock()
		return err
	case classID == amqpClassBasic && methodID == amqpMethodBasicConsume:
		var reserved uint16
		_ = binary.Read(args, binary.BigEndian, &reserved)
		queueName, err := readShortstr(args)
		if err != nil {
			return err
		}
		consumerTag, err := readShortstr(args)
		if err != nil {
			return err
		}
		if consumerTag == "" {
			*consumerSeq++
			consumerTag = fmt.Sprintf("ctag-%d", *consumerSeq)
		}
		s.mu.Lock()
		s.consumers[queueName] = &fakeConsumer{
			conn:        conn,
			writeMu:     writeMu,
			channel:     channel,
			queue:       queueName,
			consumerTag: consumerTag,
		}
		s.mu.Unlock()
		var buf bytes.Buffer
		_ = writeShortstr(&buf, consumerTag)
		writeMu.Lock()
		err = writeMethodFrame(conn, channel, amqpClassBasic, amqpMethodBasicConsumeOk, buf.Bytes())
		writeMu.Unlock()
		return err
	case classID == amqpClassBasic && methodID == amqpMethodBasicPublish:
		var reserved uint16
		_ = binary.Read(args, binary.BigEndian, &reserved)
		exchange, err := readShortstr(args)
		if err != nil {
			return err
		}
		routingKey, err := readShortstr(args)
		if err != nil {
			return err
		}
		pending[channel] = &pendingPublish{
			exchange:   exchange,
			routingKey: routingKey,
		}
		return nil
	case classID == amqpClassBasic && methodID == amqpMethodBasicAck:
		var deliveryTag uint64
		if err := binary.Read(args, binary.BigEndian, &deliveryTag); err != nil {
			return err
		}
		s.ackEvents <- fakeAckEvent{DeliveryTag: deliveryTag}
		return nil
	case classID == amqpClassBasic && methodID == amqpMethodBasicNack:
		var deliveryTag uint64
		var bits byte
		if err := binary.Read(args, binary.BigEndian, &deliveryTag); err != nil {
			return err
		}
		_ = binary.Read(args, binary.BigEndian, &bits)
		s.ackEvents <- fakeAckEvent{
			DeliveryTag: deliveryTag,
			Nacked:      true,
			Requeue:     bits&(1<<1) != 0,
		}
		return nil
	default:
		return nil
	}
}

func (s *fakeAMQPServer) finishPublish(
	conn net.Conn,
	writeMu *sync.Mutex,
	channel uint16,
	p *pendingPublish,
	confirmChannels map[uint16]*uint64,
	pending map[uint16]*pendingPublish,
) error {
	delete(pending, channel)
	msg := fakePublishedMsg{
		Exchange:   p.exchange,
		RoutingKey: p.routingKey,
		Body:       p.body.String(),
		Headers:    p.headers,
	}
	s.mu.Lock()
	s.published = append(s.published, msg)
	s.mu.Unlock()
	s.publishedCh <- msg

	if seq := confirmChannels[channel]; seq != nil {
		*seq++
		var ackBuf bytes.Buffer
		_ = binary.Write(&ackBuf, binary.BigEndian, *seq)
		_ = ackBuf.WriteByte(0)
		writeMu.Lock()
		err := writeMethodFrame(conn, channel, amqpClassBasic, amqpMethodBasicAck, ackBuf.Bytes())
		writeMu.Unlock()
		return err
	}
	return nil
}

func sendConnectionStart(writeMu *sync.Mutex, conn net.Conn) error {
	var buf bytes.Buffer
	_ = buf.WriteByte(0)
	_ = buf.WriteByte(9)
	_ = binary.Write(&buf, binary.BigEndian, uint32(0))
	_ = writeLongstr(&buf, "PLAIN")
	_ = writeLongstr(&buf, "en_US")
	writeMu.Lock()
	defer writeMu.Unlock()
	return writeMethodFrame(conn, 0, amqpClassConnection, amqpMethodConnectionStart, buf.Bytes())
}

func sendConnectionTune(writeMu *sync.Mutex, conn net.Conn) error {
	var buf bytes.Buffer
	_ = binary.Write(&buf, binary.BigEndian, amqpChannelMax)
	_ = binary.Write(&buf, binary.BigEndian, amqpFrameMax)
	_ = binary.Write(&buf, binary.BigEndian, uint16(0))
	writeMu.Lock()
	defer writeMu.Unlock()
	return writeMethodFrame(conn, 0, amqpClassConnection, amqpMethodConnectionTune, buf.Bytes())
}

func sendConnectionOpenOk(writeMu *sync.Mutex, conn net.Conn) error {
	var buf bytes.Buffer
	_ = writeShortstr(&buf, "")
	writeMu.Lock()
	defer writeMu.Unlock()
	return writeMethodFrame(conn, 0, amqpClassConnection, amqpMethodConnectionOpenOk, buf.Bytes())
}

func readFrame(r *bufio.Reader) (byte, uint16, []byte, error) {
	header := make([]byte, 7)
	if _, err := io.ReadFull(r, header); err != nil {
		return 0, 0, nil, err
	}
	frameType := header[0]
	channel := binary.BigEndian.Uint16(header[1:3])
	size := binary.BigEndian.Uint32(header[3:7])
	payload := make([]byte, size)
	if _, err := io.ReadFull(r, payload); err != nil {
		return 0, 0, nil, err
	}
	endByte, err := r.ReadByte()
	if err != nil {
		return 0, 0, nil, err
	}
	if endByte != amqpFrameEnd {
		return 0, 0, nil, fmt.Errorf("unexpected AMQP frame end byte %#x", endByte)
	}
	return frameType, channel, payload, nil
}

func writeMethodFrame(w io.Writer, channel, classID, methodID uint16, args []byte) error {
	payload := make([]byte, 4+len(args))
	binary.BigEndian.PutUint16(payload[0:2], classID)
	binary.BigEndian.PutUint16(payload[2:4], methodID)
	copy(payload[4:], args)
	return writeFrame(w, amqpFrameMethod, channel, payload)
}

func writeFrame(w io.Writer, frameType byte, channel uint16, payload []byte) error {
	var frame bytes.Buffer
	_ = frame.WriteByte(frameType)
	_ = binary.Write(&frame, binary.BigEndian, channel)
	_ = binary.Write(&frame, binary.BigEndian, uint32(len(payload)))
	_, _ = frame.Write(payload)
	_ = frame.WriteByte(amqpFrameEnd)
	_, err := w.Write(frame.Bytes())
	return err
}

func writeShortstr(w io.Writer, s string) error {
	if err := binary.Write(w, binary.BigEndian, byte(len(s))); err != nil {
		return err
	}
	_, err := io.WriteString(w, s)
	return err
}

func writeLongstr(w io.Writer, s string) error {
	if err := binary.Write(w, binary.BigEndian, uint32(len(s))); err != nil {
		return err
	}
	_, err := io.WriteString(w, s)
	return err
}

func readShortstr(r *bytes.Reader) (string, error) {
	length, err := r.ReadByte()
	if err != nil {
		return "", err
	}
	buf := make([]byte, length)
	if _, err := io.ReadFull(r, buf); err != nil {
		return "", err
	}
	return string(buf), nil
}

func readLongstr(r *bytes.Reader) (string, error) {
	var length uint32
	if err := binary.Read(r, binary.BigEndian, &length); err != nil {
		return "", err
	}
	buf := make([]byte, length)
	if _, err := io.ReadFull(r, buf); err != nil {
		return "", err
	}
	return string(buf), nil
}

func parseContentHeaderTable(data []byte) amqp091.Table {
	r := bytes.NewReader(data)
	var flags uint16
	if err := binary.Read(r, binary.BigEndian, &flags); err != nil {
		return nil
	}
	if flags&amqpFlagContentType != 0 {
		if _, err := readShortstr(r); err != nil {
			return nil
		}
	}
	if flags&amqpFlagContentEncoding != 0 {
		if _, err := readShortstr(r); err != nil {
			return nil
		}
	}
	if flags&amqpFlagHeaders == 0 {
		return nil
	}
	var tableLen uint32
	if err := binary.Read(r, binary.BigEndian, &tableLen); err != nil {
		return nil
	}
	tableBytes := make([]byte, tableLen)
	if _, err := io.ReadFull(r, tableBytes); err != nil {
		return nil
	}
	tr := bytes.NewReader(tableBytes)
	headers := amqp091.Table{}
	for tr.Len() > 0 {
		key, err := readShortstr(tr)
		if err != nil {
			break
		}
		tag, err := tr.ReadByte()
		if err != nil {
			break
		}
		switch tag {
		case 'S':
			val, err := readLongstr(tr)
			if err != nil {
				return headers
			}
			headers[key] = val
		case 'l':
			var val int64
			if err := binary.Read(tr, binary.BigEndian, &val); err != nil {
				return headers
			}
			headers[key] = val
		case 'I':
			var val int32
			if err := binary.Read(tr, binary.BigEndian, &val); err != nil {
				return headers
			}
			headers[key] = val
		default:
			return headers
		}
	}
	return headers
}
