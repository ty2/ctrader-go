package ctrader

import (
	"bufio"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"github.com/google/uuid"
	"github.com/ty2/ctrader-go/proto/openapi"
	"github.com/vmware/transport-go/bus"
	"google.golang.org/protobuf/proto"
	"io"
	"sync"
	"time"
	"unsafe"
)

const (
	ConnOnClosed = "onClosed"
)

type Conn struct {
	addr           string
	certificate    []tls.Certificate
	conn           *tls.Conn
	connected      bool
	reader         io.Reader
	messageHandler func(b []byte) error
	eventBus       bus.EventBus
	connCloseMutex sync.Mutex
}

func NewConn(addr string, options ...ConnOption) *Conn {
	conn := &Conn{addr: addr, eventBus: bus.NewEventBusInstance()}
	for _, option := range options {
		option(conn)
	}

	conn.eventBus.GetChannelManager().CreateChannel(ConnOnClosed)
	return conn
}

func (conn *Conn) Connect() error {
	if conn.connected {
		return nil
	}

	tlsConfig := &tls.Config{}

	if conn.certificate != nil {
		tlsConfig.Certificates = conn.certificate
	}

	c, err := tls.Dial("tcp", conn.addr, tlsConfig)
	if err != nil {
		return err
	}

	conn.conn = c
	conn.reader = bufio.NewReader(c)
	conn.connected = true
	go conn.messageLoop()
	go conn.keepAlive()
	return nil
}

func (conn *Conn) messageLoop() {
	for {
		err := conn.readMessage()

		if err != nil {
			err := conn.close(err.Error())
			if err != nil {
				panic(err)
			}
			break
		}

	}
}

func (conn *Conn) readMessage() error {
	// read message length
	msgLen := make([]byte, 4)
	_, err := conn.reader.Read(msgLen)
	if err != nil {
		return err
	}

	messageLen := binary.BigEndian.Uint32(msgLen)
	// read message content
	b := make([]byte, messageLen)
	_, err = io.ReadFull(conn.reader, b)
	if err != nil {
		return err
	}

	if conn.messageHandler != nil {
		return conn.messageHandler(b)
	}

	return nil
}

func (conn *Conn) keepAlive() {
	for range time.Tick(time.Second * 10) {
		if conn.connected {
			req := &openapi.ProtoOASubscribeSpotsReq{}
			_, _ = conn.SendMessage(uint32(openapi.ProtoPayloadType_HEARTBEAT_EVENT), req, nil)
		}
	}
}

func (conn *Conn) SendByte(b []byte) error {
	if conn.conn == nil {
		return errors.New("connection is not established")
	}

	size := ToByteArray(len(b))
	Reverse(size)

	if err := conn.conn.SetWriteDeadline(time.Now().Add(time.Second * 5)); err != nil {
		return err
	}

	if _, err := conn.conn.Write(size); err != nil {
		return err
	}
	if err := conn.conn.SetWriteDeadline(time.Now().Add(time.Second * 5)); err != nil {
		return err
	}
	if _, err := conn.conn.Write(b); err != nil {
		return err
	}

	return nil
}

func (conn *Conn) SendMessage(reqType uint32, req proto.Message, clientMsgUuid *uuid.UUID) (*uuid.UUID, error) {
	msgUuid, m := RequestMessageToProtoMessage(reqType, req, clientMsgUuid)
	b, err := proto.Marshal(m)
	if err != nil {
		return msgUuid, err
	}

	return clientMsgUuid, conn.SendByte(b)
}

func (conn *Conn) close(reason string) error {
	conn.connCloseMutex.Lock()
	defer conn.connCloseMutex.Unlock()
	if conn.connected == false {
		return nil
	}

	conn.connected = false

	if err := conn.conn.Close(); err != nil {
		return err
	}

	return conn.eventBus.SendBroadcastMessage(ConnOnClosed, reason)
}

func (conn *Conn) OnClosed() (bus.MessageHandler, error) {
	return conn.eventBus.ListenFirehose(ConnOnClosed)
}

type ConnOption func(conn *Conn)

func TlsCertificatesConnOption(certificates []tls.Certificate) ConnOption {
	return func(conn *Conn) {
		conn.certificate = certificates
	}
}

func ToByteArray(num int) []byte {
	size := 4
	arr := make([]byte, size)
	for i := 0; i < size; i++ {
		byt := *(*uint8)(unsafe.Pointer(uintptr(unsafe.Pointer(&num)) + uintptr(i)))
		arr[i] = byt
	}
	return arr
}

func Reverse(s []byte) {
	for i, j := 0, len(s)-1; i < j; i, j = i+1, j-1 {
		s[i], s[j] = s[j], s[i]
	}
}
