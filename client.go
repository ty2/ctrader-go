package ctrader

import (
	"fmt"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/ty2/ctrader-go/proto/openapi"
	"github.com/vmware/transport-go/bus"
	"github.com/vmware/transport-go/model"
	"google.golang.org/protobuf/proto"
	"strconv"
	"sync"
	"time"
)

func DefaultErrResponseTypes() []openapi.ProtoOAPayloadType {
	return []openapi.ProtoOAPayloadType{
		openapi.ProtoOAPayloadType_PROTO_OA_ERROR_RES,
	}
}

type Client struct {
	conn         *Conn
	id           string
	secret       string
	accountToken string
	eventBus     bus.EventBus
}

func NewClient(conn *Conn, id string, secret string, accountToken string) *Client {
	client := &Client{
		conn:         conn,
		id:           id,
		secret:       secret,
		accountToken: accountToken,
		eventBus:     bus.NewEventBusInstance(),
	}

	cm := client.eventBus.GetChannelManager()
	for _, v := range openapi.ProtoOAPayloadType_value {
		cm.CreateChannel(strconv.Itoa(int(v)))
	}

	for _, v := range openapi.ProtoPayloadType_value {
		cm.CreateChannel(strconv.Itoa(int(v)))
	}

	conn.messageHandler = client.handleMessage

	//client.handleError()
	//client.OnSpotEvent()
	return client
}

//
//func (client *Client) handleError() {
//	responseHandler, err := client.eventBus.ListenFirehose(strconv.Itoa(int(openapi.ProtoOAPayloadType_PROTO_OA_ERROR_RES)))
//	if err != nil {
//		panic(err)
//	}
//
//	responseHandler.Handle(
//		func(msg *model.Message) {
//			log.Println("err rev", msg.Payload)
//		},
//		func(err error) {
//			log.Println(err)
//		})
//}

func (client *Client) Connect() error {
	if err := client.conn.Connect(); err != nil {
		return err
	}

	return nil
}

func (client *Client) ApplicationAuth() (*openapi.ProtoOAApplicationAuthRes, error) {
	reqType := openapi.ProtoOAPayloadType_PROTO_OA_APPLICATION_AUTH_REQ
	resType := []openapi.ProtoOAPayloadType{openapi.ProtoOAPayloadType_PROTO_OA_APPLICATION_AUTH_RES}
	req := &openapi.ProtoOAApplicationAuthReq{
		ClientId:     &client.id,
		ClientSecret: &client.secret,
	}

	res, err := client.SendRequest(reqType, resType, DefaultErrResponseTypes(), req, nil)
	if err != nil {
		return nil, err
	}

	v, ok := res.(*openapi.ProtoOAApplicationAuthRes)
	if !ok {
		return nil, errors.New("unexpected res object")
	}

	return v, nil
}

func (client *Client) Version() (*openapi.ProtoOAVersionRes, error) {
	reqType := openapi.ProtoOAPayloadType_PROTO_OA_VERSION_REQ
	resType := []openapi.ProtoOAPayloadType{openapi.ProtoOAPayloadType_PROTO_OA_VERSION_RES}

	req := &openapi.ProtoOAVersionReq{}

	res, err := client.SendRequest(reqType, resType, DefaultErrResponseTypes(), req, nil)
	if err != nil {
		return nil, err
	}

	v, ok := res.(*openapi.ProtoOAVersionRes)
	if !ok {
		return nil, errors.New("unexpected res object")
	}

	return v, nil
}

func (client *Client) GetAccountListByAccessToken(accessToken string) (*openapi.ProtoOAGetAccountListByAccessTokenRes, error) {
	reqType := openapi.ProtoOAPayloadType_PROTO_OA_GET_ACCOUNTS_BY_ACCESS_TOKEN_REQ
	resType := []openapi.ProtoOAPayloadType{openapi.ProtoOAPayloadType_PROTO_OA_GET_ACCOUNTS_BY_ACCESS_TOKEN_RES}
	req := &openapi.ProtoOAGetAccountListByAccessTokenReq{
		PayloadType: &reqType,
		AccessToken: &accessToken,
	}

	res, err := client.SendRequest(reqType, resType, DefaultErrResponseTypes(), req, nil)
	if err != nil {
		return nil, err
	}

	v, ok := res.(*openapi.ProtoOAGetAccountListByAccessTokenRes)
	if !ok {
		return nil, errors.New("unexpected res object")
	}

	return v, nil
}

func (client *Client) RefreshToken(refreshToken string) (*openapi.ProtoOARefreshTokenRes, error) {
	reqType := openapi.ProtoOAPayloadType_PROTO_OA_REFRESH_TOKEN_REQ
	resType := []openapi.ProtoOAPayloadType{openapi.ProtoOAPayloadType_PROTO_OA_REFRESH_TOKEN_RES}
	req := &openapi.ProtoOARefreshTokenReq{
		PayloadType:  &reqType,
		RefreshToken: &refreshToken,
	}

	res, err := client.SendRequest(reqType, resType, DefaultErrResponseTypes(), req, nil)
	if err != nil {
		return nil, err
	}

	v, ok := res.(*openapi.ProtoOARefreshTokenRes)
	if !ok {
		return nil, errors.New("unexpected res object")
	}

	return v, nil
}

func (client *Client) Account(accountId int64) (*Account, error) {
	reqType := openapi.ProtoOAPayloadType_PROTO_OA_ACCOUNT_AUTH_REQ
	resType := []openapi.ProtoOAPayloadType{openapi.ProtoOAPayloadType_PROTO_OA_ACCOUNT_AUTH_RES}
	req := &openapi.ProtoOAAccountAuthReq{
		AccessToken:         &client.accountToken,
		CtidTraderAccountId: &accountId,
	}

	res, err := client.SendRequest(reqType, resType, DefaultErrResponseTypes(), req, nil)
	if err != nil {
		return nil, err
	}

	v, ok := res.(*openapi.ProtoOAAccountAuthRes)
	if !ok {
		return nil, errors.New("unexpected res object")
	}

	return NewAccount(client, *v.CtidTraderAccountId)
}

func (client *Client) GetCtidProfileByToken(accessToken string) (*openapi.ProtoOAGetCtidProfileByTokenRes, error) {
	reqType := openapi.ProtoOAPayloadType_PROTO_OA_GET_CTID_PROFILE_BY_TOKEN_REQ
	resType := []openapi.ProtoOAPayloadType{openapi.ProtoOAPayloadType_PROTO_OA_GET_CTID_PROFILE_BY_TOKEN_RES}
	req := &openapi.ProtoOAGetCtidProfileByTokenReq{
		AccessToken: &accessToken,
	}

	res, err := client.SendRequest(reqType, resType, DefaultErrResponseTypes(), req, nil)
	if err != nil {
		return nil, err
	}

	v, ok := res.(*openapi.ProtoOAGetCtidProfileByTokenRes)
	if !ok {
		return nil, errors.New("unexpected res object")
	}

	return v, nil
}

func (client *Client) SendRequest(reqType openapi.ProtoOAPayloadType, resType []openapi.ProtoOAPayloadType, errType []openapi.ProtoOAPayloadType, req proto.Message, clientMsgUuid *uuid.UUID) (interface{}, error) {
	if clientMsgUuid == nil {
		id := uuid.New()
		clientMsgUuid = &id
	}

	resHandler, err := client.responseMessageHandler(resType, clientMsgUuid, errType)
	if err != nil {
		return nil, err
	}
	defer resHandler.Close()

	clientMsgUuid, err = client.conn.SendMessage(uint32(reqType), req, clientMsgUuid)
	if err != nil {
		return nil, err
	}
	return resHandler.waitMessageResponse()
}

func (client *Client) responseMessageHandler(resTypes []openapi.ProtoOAPayloadType, clientMsgUuid *uuid.UUID, errResTypes []openapi.ProtoOAPayloadType) (*responseMessageHandler, error) {
	if resTypes == nil {
		return nil, errors.New("empty res types")
	}

	if errResTypes == nil {
		errResTypes = []openapi.ProtoOAPayloadType{}
	}

	msgCh := make(chan interface{})
	errCh := make(chan error)

	resHandlers := make([]bus.MessageHandler, len(resTypes))
	for i, resType := range resTypes {
		responseHandler, err := client.eventBus.ListenRequestOnceForDestination(strconv.Itoa(int(resType)), clientMsgUuid)
		if err != nil {
			return nil, err
		}

		responseHandler.Handle(
			func(msg *model.Message) {
				msgCh <- msg.Payload
			},
			func(err error) {
				errCh <- err
			})
		resHandlers[i] = responseHandler
	}

	errHandlers := make([]bus.MessageHandler, len(errResTypes))

	for i, errType := range errResTypes {
		errHandler, err := client.eventBus.ListenRequestOnceForDestination(strconv.Itoa(int(errType)), clientMsgUuid)
		if err != nil {
			return nil, err
		}

		errHandler.Handle(
			func(msg *model.Message) {
				errCh <- &ResponseMessageHandlerError{msg}
			},
			func(err error) {
				errCh <- err
			})

		errHandlers[i] = errHandler
	}

	return &responseMessageHandler{
		resHandlers: resHandlers,
		errHandlers: errHandlers,
		msgCh:       msgCh,
		errCh:       errCh,
	}, nil
}

func (resMsgHandler *responseMessageHandler) waitMessageResponse() (interface{}, error) {
	select {
	case v := <-resMsgHandler.msgCh:
		return v, nil
	case err := <-resMsgHandler.errCh:
		return nil, err
	case <-time.After(time.Second * 10):
		return nil, errors.New("timeout")
	}
}

func (client *Client) handleMessage(b []byte) error {
	var protoMessage openapi.ProtoMessage
	err := proto.Unmarshal(b, &protoMessage)
	if err != nil {
		return err
	}

	if protoMessage.ClientMsgId == nil {
		logger.Debug(fmt.Sprintf("message type %v, %v, %v", openapi.ProtoOAPayloadType_name[int32(*protoMessage.PayloadType)], openapi.ProtoPayloadType_name[int32(*protoMessage.PayloadType)], protoMessage.ClientMsgId))
	} else {
		logger.Debug(fmt.Sprintf("message type %v, %v, %v", openapi.ProtoOAPayloadType_name[int32(*protoMessage.PayloadType)], openapi.ProtoPayloadType_name[int32(*protoMessage.PayloadType)], *protoMessage.ClientMsgId))
	}

	if protoMessage.PayloadType == nil {
		return errors.New("nil payload type")
	}

	var resMessage proto.Message
	switch *protoMessage.PayloadType {
	case uint32(openapi.ProtoPayloadType_PROTO_MESSAGE):
		resMessage = &openapi.ProtoMessage{}
	case uint32(openapi.ProtoPayloadType_ERROR_RES):
		resMessage = &openapi.ProtoErrorRes{}
	case uint32(openapi.ProtoPayloadType_HEARTBEAT_EVENT):
		resMessage = &openapi.ProtoHeartbeatEvent{}
	case uint32(openapi.ProtoOAPayloadType_PROTO_OA_APPLICATION_AUTH_RES):
		resMessage = &openapi.ProtoOAApplicationAuthRes{}
	case uint32(openapi.ProtoOAPayloadType_PROTO_OA_ACCOUNT_AUTH_RES):
		resMessage = &openapi.ProtoOAAccountAuthRes{}
	case uint32(openapi.ProtoOAPayloadType_PROTO_OA_VERSION_RES):
		resMessage = &openapi.ProtoOAVersionRes{}
	case uint32(openapi.ProtoOAPayloadType_PROTO_OA_TRAILING_SL_CHANGED_EVENT):
		resMessage = &openapi.ProtoOATrailingSLChangedEvent{}
	case uint32(openapi.ProtoOAPayloadType_PROTO_OA_ASSET_LIST_RES):
		resMessage = &openapi.ProtoOAAssetListRes{}
	case uint32(openapi.ProtoOAPayloadType_PROTO_OA_SYMBOLS_LIST_RES):
		resMessage = &openapi.ProtoOASymbolsListRes{}
	case uint32(openapi.ProtoOAPayloadType_PROTO_OA_SYMBOL_BY_ID_RES):
		resMessage = &openapi.ProtoOASymbolByIdRes{}
	case uint32(openapi.ProtoOAPayloadType_PROTO_OA_SYMBOLS_FOR_CONVERSION_RES):
		resMessage = &openapi.ProtoOASymbolsForConversionRes{}
	case uint32(openapi.ProtoOAPayloadType_PROTO_OA_SYMBOL_CHANGED_EVENT):
		resMessage = &openapi.ProtoOASymbolChangedEvent{}
	case uint32(openapi.ProtoOAPayloadType_PROTO_OA_TRADER_RES):
		resMessage = &openapi.ProtoOATraderRes{}
	case uint32(openapi.ProtoOAPayloadType_PROTO_OA_TRADER_UPDATE_EVENT):
		resMessage = &openapi.ProtoOAMarginCallUpdateEvent{}
	case uint32(openapi.ProtoOAPayloadType_PROTO_OA_RECONCILE_RES):
		resMessage = &openapi.ProtoOAReconcileRes{}
	case uint32(openapi.ProtoOAPayloadType_PROTO_OA_EXECUTION_EVENT):
		resMessage = &openapi.ProtoOAExecutionEvent{}
	case uint32(openapi.ProtoOAPayloadType_PROTO_OA_SUBSCRIBE_SPOTS_RES):
		resMessage = &openapi.ProtoOASubscribeSpotsRes{}
	case uint32(openapi.ProtoOAPayloadType_PROTO_OA_UNSUBSCRIBE_SPOTS_RES):
		resMessage = &openapi.ProtoOAUnsubscribeSpotsRes{}
	case uint32(openapi.ProtoOAPayloadType_PROTO_OA_SPOT_EVENT):
		resMessage = &openapi.ProtoOASpotEvent{}
	case uint32(openapi.ProtoOAPayloadType_PROTO_OA_ORDER_ERROR_EVENT):
		resMessage = &openapi.ProtoOAOrderErrorEvent{}
	case uint32(openapi.ProtoOAPayloadType_PROTO_OA_DEAL_LIST_RES):
		resMessage = &openapi.ProtoOADealListRes{}
	case uint32(openapi.ProtoOAPayloadType_PROTO_OA_GET_TRENDBARS_RES):
		resMessage = &openapi.ProtoOAGetTrendbarsRes{}
	case uint32(openapi.ProtoOAPayloadType_PROTO_OA_EXPECTED_MARGIN_RES):
		resMessage = &openapi.ProtoOAExpectedMarginRes{}
	case uint32(openapi.ProtoOAPayloadType_PROTO_OA_MARGIN_CHANGED_EVENT):
		resMessage = &openapi.ProtoOAMarginChangedEvent{}
	case uint32(openapi.ProtoOAPayloadType_PROTO_OA_ERROR_RES):
		resMessage = &openapi.ProtoOAErrorRes{}
	case uint32(openapi.ProtoOAPayloadType_PROTO_OA_CASH_FLOW_HISTORY_LIST_RES):
		resMessage = &openapi.ProtoOACashFlowHistoryListRes{}
	case uint32(openapi.ProtoOAPayloadType_PROTO_OA_GET_TICKDATA_RES):
		resMessage = &openapi.ProtoOAGetTickDataRes{}
	case uint32(openapi.ProtoOAPayloadType_PROTO_OA_ACCOUNTS_TOKEN_INVALIDATED_EVENT):
		resMessage = &openapi.ProtoOAAccountsTokenInvalidatedEvent{}
	case uint32(openapi.ProtoOAPayloadType_PROTO_OA_CLIENT_DISCONNECT_EVENT):
		resMessage = &openapi.ProtoOAClientDisconnectEvent{}
	case uint32(openapi.ProtoOAPayloadType_PROTO_OA_GET_ACCOUNTS_BY_ACCESS_TOKEN_RES):
		resMessage = &openapi.ProtoOAGetAccountListByAccessTokenRes{}
	case uint32(openapi.ProtoOAPayloadType_PROTO_OA_GET_CTID_PROFILE_BY_TOKEN_RES):
		resMessage = &openapi.ProtoOAGetCtidProfileByTokenRes{}
	case uint32(openapi.ProtoOAPayloadType_PROTO_OA_ASSET_CLASS_LIST_RES):
		resMessage = &openapi.ProtoOAAssetClassListRes{}
	case uint32(openapi.ProtoOAPayloadType_PROTO_OA_DEPTH_EVENT):
		resMessage = &openapi.ProtoOADepthEvent{}
	case uint32(openapi.ProtoOAPayloadType_PROTO_OA_SUBSCRIBE_DEPTH_QUOTES_RES):
		resMessage = &openapi.ProtoOASubscribeDepthQuotesRes{}
	case uint32(openapi.ProtoOAPayloadType_PROTO_OA_UNSUBSCRIBE_DEPTH_QUOTES_RES):
		resMessage = &openapi.ProtoOAUnsubscribeDepthQuotesRes{}
	case uint32(openapi.ProtoOAPayloadType_PROTO_OA_SYMBOL_CATEGORY_RES):
		resMessage = &openapi.ProtoOASymbolCategoryListRes{}
	case uint32(openapi.ProtoOAPayloadType_PROTO_OA_ACCOUNT_LOGOUT_RES):
		resMessage = &openapi.ProtoOAAccountLogoutRes{}
	case uint32(openapi.ProtoOAPayloadType_PROTO_OA_ACCOUNT_DISCONNECT_EVENT):
		resMessage = &openapi.ProtoOAAccountDisconnectEvent{}
	case uint32(openapi.ProtoOAPayloadType_PROTO_OA_SUBSCRIBE_LIVE_TRENDBAR_RES):
		resMessage = &openapi.ProtoOASubscribeLiveTrendbarRes{}
	case uint32(openapi.ProtoOAPayloadType_PROTO_OA_UNSUBSCRIBE_LIVE_TRENDBAR_RES):
		resMessage = &openapi.ProtoOAUnsubscribeLiveTrendbarRes{}
	case uint32(openapi.ProtoOAPayloadType_PROTO_OA_MARGIN_CALL_LIST_RES):
		resMessage = &openapi.ProtoOAMarginCallListRes{}
	case uint32(openapi.ProtoOAPayloadType_PROTO_OA_MARGIN_CALL_UPDATE_RES):
		resMessage = &openapi.ProtoOAMarginCallUpdateRes{}
	case uint32(openapi.ProtoOAPayloadType_PROTO_OA_MARGIN_CALL_UPDATE_EVENT):
		resMessage = &openapi.ProtoOAMarginCallUpdateEvent{}
	case uint32(openapi.ProtoOAPayloadType_PROTO_OA_MARGIN_CALL_TRIGGER_EVENT):
		resMessage = &openapi.ProtoOAMarginCallTriggerEvent{}
	case uint32(openapi.ProtoOAPayloadType_PROTO_OA_REFRESH_TOKEN_RES):
		resMessage = &openapi.ProtoOARefreshTokenRes{}
	case uint32(openapi.ProtoOAPayloadType_PROTO_OA_ORDER_LIST_RES):
		resMessage = &openapi.ProtoOAOrderListRes{}
	case uint32(openapi.ProtoOAPayloadType_PROTO_OA_GET_DYNAMIC_LEVERAGE_RES):
		resMessage = &openapi.ProtoOAGetDynamicLeverageByIDRes{}
	case uint32(openapi.ProtoOAPayloadType_PROTO_OA_DEAL_LIST_BY_POSITION_ID_RES):
		resMessage = &openapi.ProtoOADealListByPositionIdRes{}
	default:
		return errors.New(fmt.Sprintf("unknown message type %v", *protoMessage.PayloadType))
	}

	var clientMsgUUID *uuid.UUID
	if protoMessage.ClientMsgId != nil {
		id, err := uuid.Parse(*protoMessage.ClientMsgId)
		if err != nil {
			return errors.New(fmt.Sprintf("client msg uuid error, %s", err))
		}
		clientMsgUUID = &id
	}

	err = proto.Unmarshal(protoMessage.Payload, resMessage)
	if err != nil {
		return client.eventBus.SendErrorMessage(strconv.Itoa(int(*protoMessage.PayloadType)), err, clientMsgUUID)
	}

	if clientMsgUUID != nil {
		err = client.eventBus.SendRequestMessage(strconv.Itoa(int(*protoMessage.PayloadType)), resMessage, clientMsgUUID)
		if err != nil {
			return client.eventBus.SendErrorMessage(strconv.Itoa(int(*protoMessage.PayloadType)), err, clientMsgUUID)
		}
	} else {
		err = client.eventBus.SendBroadcastMessage(strconv.Itoa(int(*protoMessage.PayloadType)), resMessage)
		if err != nil {
			return client.eventBus.SendErrorMessage(strconv.Itoa(int(*protoMessage.PayloadType)), err, clientMsgUUID)
		}
	}

	return nil
}

func (client *Client) On(payloadType openapi.ProtoOAPayloadType) (bus.MessageHandler, error) {
	return client.eventBus.ListenFirehose(strconv.Itoa(int(payloadType)))
}

func (client *Client) OnConnClose() (bus.MessageHandler, error) {
	return client.conn.OnClosed()
}

func (client *Client) Close() error {
	return client.conn.close("closed by user")
}

type responseMessageHandler struct {
	resHandlers []bus.MessageHandler
	errHandlers []bus.MessageHandler
	msgCh       chan interface{}
	errCh       chan error
	closeOnce   sync.Once
}

func (resMsgHandler *responseMessageHandler) Close() {
	resMsgHandler.closeOnce.Do(func() {
		for _, resHandler := range resMsgHandler.resHandlers {
			resHandler.Close()
		}

		for _, errHandler := range resMsgHandler.errHandlers {
			errHandler.Close()
		}
		close(resMsgHandler.msgCh)
		close(resMsgHandler.errCh)
	})
}

type ResponseMessageHandlerError struct {
	*model.Message
}

func (resMsgHandlerErr *ResponseMessageHandlerError) Error() string {
	switch resMsgHandlerErr.Payload.(type) {
	case *openapi.ProtoOAErrorRes:
		errCode := ""
		if v := resMsgHandlerErr.Payload.(*openapi.ProtoOAErrorRes).ErrorCode; v != nil {
			errCode = *v
		}

		description := ""
		if v := resMsgHandlerErr.Payload.(*openapi.ProtoOAErrorRes).Description; v != nil {
			description = *v
		}

		accountId := "nil"
		if v := resMsgHandlerErr.Payload.(*openapi.ProtoOAErrorRes).CtidTraderAccountId; v != nil {
			accountId = strconv.Itoa(int(*v))
		}

		return fmt.Sprintf("%v - desc: %v; accId: %v",
			errCode,
			description,
			accountId)
	case *openapi.ProtoOAOrderErrorEvent:
		errCode := ""
		if v := resMsgHandlerErr.Payload.(*openapi.ProtoOAOrderErrorEvent).ErrorCode; v != nil {
			errCode = *v
		}

		description := ""
		if v := resMsgHandlerErr.Payload.(*openapi.ProtoOAOrderErrorEvent).Description; v != nil {
			description = *v
		}

		accountId := "nil"
		if v := resMsgHandlerErr.Payload.(*openapi.ProtoOAOrderErrorEvent).CtidTraderAccountId; v != nil {
			accountId = strconv.Itoa(int(*v))
		}

		orderId := "nil"
		if v := resMsgHandlerErr.Payload.(*openapi.ProtoOAOrderErrorEvent).CtidTraderAccountId; v != nil {
			orderId = strconv.Itoa(int(*v))
		}

		positionId := "nil"
		if v := resMsgHandlerErr.Payload.(*openapi.ProtoOAOrderErrorEvent).CtidTraderAccountId; v != nil {
			positionId = strconv.Itoa(int(*v))
		}

		return fmt.Sprintf("%v - desc: %v; accId: %v; orderId: %v; posId: %v",
			errCode,
			description,
			accountId,
			orderId,
			positionId)
	default:
		return fmt.Sprint(resMsgHandlerErr.Payload)
	}
}
