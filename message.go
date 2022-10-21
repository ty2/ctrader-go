package ctrader

import (
	"github.com/google/uuid"
	"github.com/ty2/ctrader-go/proto/openapi"
	"google.golang.org/protobuf/proto"
)

func RequestMessageToProtoMessage(reqType uint32, payload proto.Message, clientMsgUuid *uuid.UUID) (*uuid.UUID, *openapi.ProtoMessage) {
	message := &openapi.ProtoMessage{}
	payloadBytes, _ := proto.Marshal(payload)

	if clientMsgUuid != nil {
		clientMsgId := clientMsgUuid.String()
		message.ClientMsgId = &clientMsgId
	}

	message.Payload = payloadBytes
	message.PayloadType = &reqType
	return clientMsgUuid, message
}
