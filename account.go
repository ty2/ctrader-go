package ctrader

import (
	"errors"
	"github.com/ty2/ctrader-go/proto/openapi"
	"github.com/vmware/transport-go/bus"
	"github.com/vmware/transport-go/model"
	"reflect"
	"strconv"
)

type Account struct {
	client   *Client
	id       int64
	eventBus bus.EventBus
}

func NewAccount(client *Client, id int64) (*Account, error) {
	account := &Account{
		client:   client,
		id:       id,
		eventBus: bus.NewEventBusInstance(),
	}

	cm := account.eventBus.GetChannelManager()
	for _, v := range openapi.ProtoOAPayloadType_value {
		cm.CreateChannel(strconv.Itoa(int(v)))
		err := account.createEventListener(openapi.ProtoOAPayloadType(v))
		if err != nil {
			return nil, err
		}
	}

	return account, nil
}

func (account *Account) Id() int64 {
	return account.id
}

func (account *Account) NewOrder(req *openapi.ProtoOANewOrderReq) (*openapi.ProtoOAExecutionEvent, error) {
	if req.CtidTraderAccountId != nil {
		return nil, errors.New("account id must be empty")
	}
	req.CtidTraderAccountId = &account.id

	reqType := openapi.ProtoOAPayloadType_PROTO_OA_NEW_ORDER_REQ
	resTypes := []openapi.ProtoOAPayloadType{openapi.ProtoOAPayloadType_PROTO_OA_EXECUTION_EVENT}
	errTypes := append(DefaultErrResponseTypes(), openapi.ProtoOAPayloadType_PROTO_OA_ORDER_ERROR_EVENT)

	res, err := account.client.SendRequest(reqType, resTypes, errTypes, req, nil)
	if err != nil {
		return nil, err
	}

	v, ok := res.(*openapi.ProtoOAExecutionEvent)
	if !ok {
		return nil, errors.New("unexpected res object")
	}

	return v, nil
}

func (account *Account) CancelOrder(orderId int64) (*openapi.ProtoOAExecutionEvent, error) {
	reqType := openapi.ProtoOAPayloadType_PROTO_OA_CANCEL_ORDER_REQ
	req := &openapi.ProtoOACancelOrderReq{
		CtidTraderAccountId: &account.id,
		OrderId:             &orderId,
	}

	resTypes := []openapi.ProtoOAPayloadType{openapi.ProtoOAPayloadType_PROTO_OA_EXECUTION_EVENT}
	errTypes := append(DefaultErrResponseTypes(), openapi.ProtoOAPayloadType_PROTO_OA_ORDER_ERROR_EVENT)

	res, err := account.client.SendRequest(reqType, resTypes, errTypes, req, nil)
	if err != nil {
		return nil, err
	}

	v, ok := res.(*openapi.ProtoOAExecutionEvent)
	if !ok {
		return nil, errors.New("unexpected res object")
	}

	return v, nil
}

func (account *Account) AmendOrder(req *openapi.ProtoOAAmendOrderReq) (*openapi.ProtoOAExecutionEvent, error) {
	if req.CtidTraderAccountId != nil {
		return nil, errors.New("account id must be empty")
	}

	reqType := openapi.ProtoOAPayloadType_PROTO_OA_AMEND_ORDER_REQ

	req.CtidTraderAccountId = &account.id

	resTypes := []openapi.ProtoOAPayloadType{openapi.ProtoOAPayloadType_PROTO_OA_EXECUTION_EVENT}
	errTypes := append(DefaultErrResponseTypes(), openapi.ProtoOAPayloadType_PROTO_OA_ORDER_ERROR_EVENT)

	res, err := account.client.SendRequest(reqType, resTypes, errTypes, req, nil)
	if err != nil {
		return nil, err
	}

	v, ok := res.(*openapi.ProtoOAExecutionEvent)
	if !ok {
		return nil, errors.New("unexpected res object")
	}

	return v, nil
}

func (account *Account) AmendOrderPositionSlip(req *openapi.ProtoOAAmendPositionSLTPReq) (*openapi.ProtoOAExecutionEvent, error) {
	if req.CtidTraderAccountId != nil {
		return nil, errors.New("account id must be empty")
	}
	req.CtidTraderAccountId = &account.id

	reqType := openapi.ProtoOAPayloadType_PROTO_OA_AMEND_POSITION_SLTP_REQ
	resTypes := []openapi.ProtoOAPayloadType{openapi.ProtoOAPayloadType_PROTO_OA_EXECUTION_EVENT}
	errTypes := append(DefaultErrResponseTypes(), openapi.ProtoOAPayloadType_PROTO_OA_ORDER_ERROR_EVENT)

	res, err := account.client.SendRequest(reqType, resTypes, errTypes, req, nil)
	if err != nil {
		return nil, err
	}

	v, ok := res.(*openapi.ProtoOAExecutionEvent)
	if !ok {
		return nil, errors.New("unexpected res object")
	}

	return v, nil
}

func (account *Account) ClosePosition(positionId int64, volume int64) (*openapi.ProtoOAExecutionEvent, error) {
	reqType := openapi.ProtoOAPayloadType_PROTO_OA_CLOSE_POSITION_REQ
	resType := []openapi.ProtoOAPayloadType{openapi.ProtoOAPayloadType_PROTO_OA_EXECUTION_EVENT}
	errTypes := append(DefaultErrResponseTypes(), openapi.ProtoOAPayloadType_PROTO_OA_ORDER_ERROR_EVENT)

	req := &openapi.ProtoOAClosePositionReq{
		CtidTraderAccountId: &account.id,
		PositionId:          &positionId,
		Volume:              &volume,
	}

	res, err := account.client.SendRequest(reqType, resType, errTypes, req, nil)
	if err != nil {
		return nil, err
	}

	v, ok := res.(*openapi.ProtoOAExecutionEvent)
	if !ok {
		return nil, errors.New("unexpected res object")
	}

	return v, nil
}

func (account *Account) AssetsList() (*openapi.ProtoOAAssetListRes, error) {
	reqType := openapi.ProtoOAPayloadType_PROTO_OA_ASSET_LIST_REQ
	resType := []openapi.ProtoOAPayloadType{openapi.ProtoOAPayloadType_PROTO_OA_ASSET_LIST_RES}
	req := &openapi.ProtoOAAssetListReq{
		CtidTraderAccountId: &account.id,
	}

	res, err := account.client.SendRequest(reqType, resType, DefaultErrResponseTypes(), req, nil)
	if err != nil {
		return nil, err
	}

	v, ok := res.(*openapi.ProtoOAAssetListRes)
	if !ok {
		return nil, errors.New("unexpected res object")
	}

	return v, nil
}

func (account *Account) SymbolList() (*openapi.ProtoOASymbolsListRes, error) {
	reqType := openapi.ProtoOAPayloadType_PROTO_OA_SYMBOLS_LIST_REQ
	resType := []openapi.ProtoOAPayloadType{openapi.ProtoOAPayloadType_PROTO_OA_SYMBOLS_LIST_RES}
	req := &openapi.ProtoOASymbolsListReq{
		CtidTraderAccountId: &account.id,
	}

	res, err := account.client.SendRequest(reqType, resType, DefaultErrResponseTypes(), req, nil)
	if err != nil {
		return nil, err
	}

	v, ok := res.(*openapi.ProtoOASymbolsListRes)
	if !ok {
		return nil, errors.New("unexpected res object")
	}

	return v, nil
}

func (account *Account) SymbolById(ids []int64) (*openapi.ProtoOASymbolByIdRes, error) {
	reqType := openapi.ProtoOAPayloadType_PROTO_OA_SYMBOL_BY_ID_REQ
	resType := []openapi.ProtoOAPayloadType{openapi.ProtoOAPayloadType_PROTO_OA_SYMBOL_BY_ID_RES}
	req := &openapi.ProtoOASymbolByIdReq{
		CtidTraderAccountId: &account.id,
		SymbolId:            ids,
	}

	res, err := account.client.SendRequest(reqType, resType, DefaultErrResponseTypes(), req, nil)
	if err != nil {
		return nil, err
	}

	v, ok := res.(*openapi.ProtoOASymbolByIdRes)
	if !ok {
		return nil, errors.New("unexpected res object")
	}

	return v, nil
}

func (account *Account) SymbolsForConversion(firstAssetId, lastAssetId int64) (*openapi.ProtoOASymbolsForConversionRes, error) {
	reqType := openapi.ProtoOAPayloadType_PROTO_OA_SYMBOLS_FOR_CONVERSION_REQ
	resType := []openapi.ProtoOAPayloadType{openapi.ProtoOAPayloadType_PROTO_OA_SYMBOLS_FOR_CONVERSION_RES}
	req := &openapi.ProtoOASymbolsForConversionReq{
		CtidTraderAccountId: &account.id,
		FirstAssetId:        &firstAssetId,
		LastAssetId:         &lastAssetId,
	}

	res, err := account.client.SendRequest(reqType, resType, DefaultErrResponseTypes(), req, nil)
	if err != nil {
		return nil, err
	}

	v, ok := res.(*openapi.ProtoOASymbolsForConversionRes)
	if !ok {
		return nil, errors.New("unexpected res object")
	}

	return v, nil
}

func (account *Account) Trader() (*openapi.ProtoOATraderRes, error) {
	reqType := openapi.ProtoOAPayloadType_PROTO_OA_TRADER_REQ
	resType := []openapi.ProtoOAPayloadType{openapi.ProtoOAPayloadType_PROTO_OA_TRADER_RES}

	req := &openapi.ProtoOATraderReq{
		PayloadType:         &reqType,
		CtidTraderAccountId: &account.id,
	}

	res, err := account.client.SendRequest(reqType, resType, DefaultErrResponseTypes(), req, nil)
	if err != nil {
		return nil, err
	}

	v, ok := res.(*openapi.ProtoOATraderRes)
	if !ok {
		return nil, errors.New("unexpected res object")
	}

	return v, nil
}

func (account *Account) Reconcile() (*openapi.ProtoOAReconcileRes, error) {
	reqType := openapi.ProtoOAPayloadType_PROTO_OA_RECONCILE_REQ
	resType := []openapi.ProtoOAPayloadType{openapi.ProtoOAPayloadType_PROTO_OA_RECONCILE_RES}
	req := &openapi.ProtoOAReconcileReq{
		PayloadType:         &reqType,
		CtidTraderAccountId: &account.id,
	}

	res, err := account.client.SendRequest(reqType, resType, DefaultErrResponseTypes(), req, nil)
	if err != nil {
		return nil, err
	}

	v, ok := res.(*openapi.ProtoOAReconcileRes)
	if !ok {
		return nil, errors.New("unexpected res object")
	}

	return v, nil
}

func (account *Account) SubscribeSpots(symbolId []int64) (*openapi.ProtoOASubscribeSpotsRes, error) {
	reqType := openapi.ProtoOAPayloadType_PROTO_OA_SUBSCRIBE_SPOTS_REQ
	resType := []openapi.ProtoOAPayloadType{openapi.ProtoOAPayloadType_PROTO_OA_SUBSCRIBE_SPOTS_RES}

	req := &openapi.ProtoOASubscribeSpotsReq{
		CtidTraderAccountId: &account.id,
		SymbolId:            symbolId,
	}

	res, err := account.client.SendRequest(reqType, resType, DefaultErrResponseTypes(), req, nil)
	if err != nil {
		return nil, err
	}

	v, ok := res.(*openapi.ProtoOASubscribeSpotsRes)
	if !ok {
		return nil, errors.New("unexpected res object")
	}

	return v, nil
}

func (account *Account) UnsubscribeSpots(symbolId []int64) (*openapi.ProtoOAUnsubscribeSpotsRes, error) {
	reqType := openapi.ProtoOAPayloadType_PROTO_OA_UNSUBSCRIBE_SPOTS_REQ
	resType := []openapi.ProtoOAPayloadType{openapi.ProtoOAPayloadType_PROTO_OA_UNSUBSCRIBE_SPOTS_RES}

	req := &openapi.ProtoOAUnsubscribeSpotsReq{
		PayloadType:         &reqType,
		CtidTraderAccountId: &account.id,
		SymbolId:            symbolId,
	}

	res, err := account.client.SendRequest(reqType, resType, DefaultErrResponseTypes(), req, nil)
	if err != nil {
		return nil, err
	}

	v, ok := res.(*openapi.ProtoOAUnsubscribeSpotsRes)
	if !ok {
		return nil, errors.New("unexpected res object")
	}

	return v, nil
}

func (account *Account) DealList(fromTimestamp, toTimestamp int64, maxRows *int32) (*openapi.ProtoOADealListRes, error) {
	reqType := openapi.ProtoOAPayloadType_PROTO_OA_DEAL_LIST_REQ
	resType := []openapi.ProtoOAPayloadType{openapi.ProtoOAPayloadType_PROTO_OA_DEAL_LIST_RES}

	req := &openapi.ProtoOADealListReq{
		PayloadType:         &reqType,
		CtidTraderAccountId: &account.id,
		FromTimestamp:       &fromTimestamp,
		ToTimestamp:         &toTimestamp,
		MaxRows:             maxRows,
	}

	res, err := account.client.SendRequest(reqType, resType, DefaultErrResponseTypes(), req, nil)
	if err != nil {
		return nil, err
	}

	v, ok := res.(*openapi.ProtoOADealListRes)
	if !ok {
		return nil, errors.New("unexpected res object")
	}

	return v, nil
}

func (account *Account) SubscribeLiveTrendbar(symbolId int64, period openapi.ProtoOATrendbarPeriod) (*openapi.ProtoOASubscribeLiveTrendbarRes, error) {
	reqType := openapi.ProtoOAPayloadType_PROTO_OA_SUBSCRIBE_LIVE_TRENDBAR_REQ
	resType := []openapi.ProtoOAPayloadType{openapi.ProtoOAPayloadType_PROTO_OA_SUBSCRIBE_LIVE_TRENDBAR_RES}
	req := &openapi.ProtoOASubscribeLiveTrendbarReq{
		CtidTraderAccountId: &account.id,
		SymbolId:            &symbolId,
		Period:              &period,
	}

	res, err := account.client.SendRequest(reqType, resType, DefaultErrResponseTypes(), req, nil)
	if err != nil {
		return nil, err
	}

	v, ok := res.(*openapi.ProtoOASubscribeLiveTrendbarRes)
	if !ok {
		return nil, errors.New("unexpected res object")
	}

	return v, nil
}

func (account *Account) UnsubscribeLiveTrendbar(symbolId int64, period openapi.ProtoOATrendbarPeriod) (*openapi.ProtoOAUnsubscribeLiveTrendbarRes, error) {
	reqType := openapi.ProtoOAPayloadType_PROTO_OA_UNSUBSCRIBE_LIVE_TRENDBAR_REQ
	resType := []openapi.ProtoOAPayloadType{openapi.ProtoOAPayloadType_PROTO_OA_UNSUBSCRIBE_LIVE_TRENDBAR_RES}
	req := &openapi.ProtoOAUnsubscribeLiveTrendbarReq{
		PayloadType:         &reqType,
		CtidTraderAccountId: &account.id,
		SymbolId:            &symbolId,
		Period:              &period,
	}

	res, err := account.client.SendRequest(reqType, resType, DefaultErrResponseTypes(), req, nil)
	if err != nil {
		return nil, err
	}

	v, ok := res.(*openapi.ProtoOAUnsubscribeLiveTrendbarRes)
	if !ok {
		return nil, errors.New("unexpected res object")
	}

	return v, nil
}

func (account *Account) GetTrendbars(fromTimestamp, toTimestamp int64, period openapi.ProtoOATrendbarPeriod, symbolId int64, count uint32) (*openapi.ProtoOAGetTrendbarsRes, error) {
	reqType := openapi.ProtoOAPayloadType_PROTO_OA_GET_TRENDBARS_REQ
	resType := []openapi.ProtoOAPayloadType{openapi.ProtoOAPayloadType_PROTO_OA_GET_TRENDBARS_RES}
	req := &openapi.ProtoOAGetTrendbarsReq{
		PayloadType:         &reqType,
		CtidTraderAccountId: &account.id,
		FromTimestamp:       &fromTimestamp,
		ToTimestamp:         &toTimestamp,
		Period:              &period,
		SymbolId:            &symbolId,
		Count:               &count,
	}

	res, err := account.client.SendRequest(reqType, resType, DefaultErrResponseTypes(), req, nil)
	if err != nil {
		return nil, err
	}

	v, ok := res.(*openapi.ProtoOAGetTrendbarsRes)
	if !ok {
		return nil, errors.New("unexpected res object")
	}

	return v, nil
}

func (account *Account) ExpectedMargin(symbolId int64, volume []int64) (*openapi.ProtoOAExpectedMarginRes, error) {
	reqType := openapi.ProtoOAPayloadType_PROTO_OA_EXPECTED_MARGIN_REQ
	resType := []openapi.ProtoOAPayloadType{openapi.ProtoOAPayloadType_PROTO_OA_EXPECTED_MARGIN_RES}
	req := &openapi.ProtoOAExpectedMarginReq{
		PayloadType:         &reqType,
		CtidTraderAccountId: &account.id,
		SymbolId:            &symbolId,
		Volume:              volume,
	}

	res, err := account.client.SendRequest(reqType, resType, DefaultErrResponseTypes(), req, nil)
	if err != nil {
		return nil, err
	}

	v, ok := res.(*openapi.ProtoOAExpectedMarginRes)
	if !ok {
		return nil, errors.New("unexpected res object")
	}

	return v, nil
}

func (account *Account) CashFlowHistoryList(fromTimestamp, toTimestamp int64) (*openapi.ProtoOACashFlowHistoryListRes, error) {
	reqType := openapi.ProtoOAPayloadType_PROTO_OA_CASH_FLOW_HISTORY_LIST_REQ
	resType := []openapi.ProtoOAPayloadType{openapi.ProtoOAPayloadType_PROTO_OA_CASH_FLOW_HISTORY_LIST_RES}
	req := &openapi.ProtoOACashFlowHistoryListReq{
		PayloadType:         &reqType,
		CtidTraderAccountId: &account.id,
		FromTimestamp:       &fromTimestamp,
		ToTimestamp:         &toTimestamp,
	}

	res, err := account.client.SendRequest(reqType, resType, DefaultErrResponseTypes(), req, nil)
	if err != nil {
		return nil, err
	}

	v, ok := res.(*openapi.ProtoOACashFlowHistoryListRes)
	if !ok {
		return nil, errors.New("unexpected res object")
	}

	return v, nil
}

func (account *Account) GetTickData(symbolId int64, quoteType openapi.ProtoOAQuoteType, fromTimestamp, toTimestamp int64) (*openapi.ProtoOAGetTickDataRes, error) {
	reqType := openapi.ProtoOAPayloadType_PROTO_OA_GET_TICKDATA_REQ
	resType := []openapi.ProtoOAPayloadType{openapi.ProtoOAPayloadType_PROTO_OA_GET_TICKDATA_RES}
	req := &openapi.ProtoOAGetTickDataReq{
		PayloadType:         &reqType,
		CtidTraderAccountId: &account.id,
		SymbolId:            &symbolId,
		Type:                &quoteType,
		FromTimestamp:       &fromTimestamp,
		ToTimestamp:         &toTimestamp,
	}

	res, err := account.client.SendRequest(reqType, resType, DefaultErrResponseTypes(), req, nil)
	if err != nil {
		return nil, err
	}

	v, ok := res.(*openapi.ProtoOAGetTickDataRes)
	if !ok {
		return nil, errors.New("unexpected res object")
	}

	return v, nil
}

func (account *Account) AssetClassList() (*openapi.ProtoOAAssetClassListRes, error) {
	reqType := openapi.ProtoOAPayloadType_PROTO_OA_ASSET_CLASS_LIST_REQ
	resType := []openapi.ProtoOAPayloadType{openapi.ProtoOAPayloadType_PROTO_OA_ASSET_CLASS_LIST_RES}
	req := &openapi.ProtoOAAssetClassListReq{
		CtidTraderAccountId: &account.id,
	}

	res, err := account.client.SendRequest(reqType, resType, DefaultErrResponseTypes(), req, nil)
	if err != nil {
		return nil, err
	}

	v, ok := res.(*openapi.ProtoOAAssetClassListRes)
	if !ok {
		return nil, errors.New("unexpected res object")
	}

	return v, nil
}

func (account *Account) SubscribeDepthQuotes(symbolId []int64) (*openapi.ProtoOASubscribeDepthQuotesRes, error) {
	reqType := openapi.ProtoOAPayloadType_PROTO_OA_SUBSCRIBE_DEPTH_QUOTES_REQ
	resType := []openapi.ProtoOAPayloadType{openapi.ProtoOAPayloadType_PROTO_OA_SUBSCRIBE_DEPTH_QUOTES_RES}

	req := &openapi.ProtoOASubscribeDepthQuotesReq{
		PayloadType:         &reqType,
		CtidTraderAccountId: &account.id,
		SymbolId:            symbolId,
	}

	res, err := account.client.SendRequest(reqType, resType, DefaultErrResponseTypes(), req, nil)
	if err != nil {
		return nil, err
	}

	v, ok := res.(*openapi.ProtoOASubscribeDepthQuotesRes)
	if !ok {
		return nil, errors.New("unexpected res object")
	}

	return v, nil
}

func (account *Account) UnsubscribeDepthQuotes(symbolId []int64) (*openapi.ProtoOAUnsubscribeDepthQuotesRes, error) {
	reqType := openapi.ProtoOAPayloadType_PROTO_OA_UNSUBSCRIBE_DEPTH_QUOTES_REQ
	resType := []openapi.ProtoOAPayloadType{openapi.ProtoOAPayloadType_PROTO_OA_UNSUBSCRIBE_DEPTH_QUOTES_RES}
	req := &openapi.ProtoOAUnsubscribeDepthQuotesReq{
		PayloadType:         &reqType,
		CtidTraderAccountId: &account.id,
		SymbolId:            symbolId,
	}

	res, err := account.client.SendRequest(reqType, resType, DefaultErrResponseTypes(), req, nil)
	if err != nil {
		return nil, err
	}

	v, ok := res.(*openapi.ProtoOAUnsubscribeDepthQuotesRes)
	if !ok {
		return nil, errors.New("unexpected res object")
	}

	return v, nil
}

func (account *Account) SymbolCategoryList() (*openapi.ProtoOASymbolCategoryListRes, error) {
	reqType := openapi.ProtoOAPayloadType_PROTO_OA_SYMBOL_CATEGORY_REQ
	resType := []openapi.ProtoOAPayloadType{openapi.ProtoOAPayloadType_PROTO_OA_SYMBOL_CATEGORY_RES}
	req := &openapi.ProtoOASymbolCategoryListReq{
		PayloadType:         &reqType,
		CtidTraderAccountId: &account.id,
	}

	res, err := account.client.SendRequest(reqType, resType, DefaultErrResponseTypes(), req, nil)
	if err != nil {
		return nil, err
	}

	v, ok := res.(*openapi.ProtoOASymbolCategoryListRes)
	if !ok {
		return nil, errors.New("unexpected res object")
	}

	return v, nil
}

func (account *Account) AccountLogout() (*openapi.ProtoOAAccountLogoutRes, error) {
	reqType := openapi.ProtoOAPayloadType_PROTO_OA_ACCOUNT_LOGOUT_REQ
	resType := []openapi.ProtoOAPayloadType{openapi.ProtoOAPayloadType_PROTO_OA_ACCOUNT_LOGOUT_RES}
	req := &openapi.ProtoOAAccountLogoutReq{
		PayloadType:         &reqType,
		CtidTraderAccountId: &account.id,
	}

	res, err := account.client.SendRequest(reqType, resType, DefaultErrResponseTypes(), req, nil)
	if err != nil {
		return nil, err
	}

	v, ok := res.(*openapi.ProtoOAAccountLogoutRes)
	if !ok {
		return nil, errors.New("unexpected res object")
	}

	return v, nil
}

func (account *Account) MarginCallList() (*openapi.ProtoOAMarginCallListRes, error) {
	reqType := openapi.ProtoOAPayloadType_PROTO_OA_MARGIN_CALL_LIST_REQ
	resType := []openapi.ProtoOAPayloadType{openapi.ProtoOAPayloadType_PROTO_OA_MARGIN_CALL_LIST_RES}

	req := &openapi.ProtoOAMarginCallListReq{
		PayloadType:         &reqType,
		CtidTraderAccountId: &account.id,
	}

	res, err := account.client.SendRequest(reqType, resType, DefaultErrResponseTypes(), req, nil)
	if err != nil {
		return nil, err
	}

	v, ok := res.(*openapi.ProtoOAMarginCallListRes)
	if !ok {
		return nil, errors.New("unexpected res object")
	}

	return v, nil
}

func (account *Account) MarginCallUpdate(marginCallType openapi.ProtoOANotificationType, marginLevelThreshold float64) (*openapi.ProtoOAMarginCallUpdateRes, error) {
	reqType := openapi.ProtoOAPayloadType_PROTO_OA_MARGIN_CALL_UPDATE_REQ
	resType := []openapi.ProtoOAPayloadType{openapi.ProtoOAPayloadType_PROTO_OA_MARGIN_CALL_UPDATE_RES}

	req := &openapi.ProtoOAMarginCallUpdateReq{
		CtidTraderAccountId: &account.id,
		MarginCall:          &openapi.ProtoOAMarginCall{MarginCallType: &marginCallType, MarginLevelThreshold: &marginLevelThreshold},
	}

	res, err := account.client.SendRequest(reqType, resType, DefaultErrResponseTypes(), req, nil)
	if err != nil {
		return nil, err
	}

	v, ok := res.(*openapi.ProtoOAMarginCallUpdateRes)
	if !ok {
		return nil, errors.New("unexpected res object")
	}

	return v, nil
}

func (account *Account) OrderList(fromTimestamp, toTimestamp int64) (*openapi.ProtoOAOrderListRes, error) {
	reqType := openapi.ProtoOAPayloadType_PROTO_OA_ORDER_LIST_REQ
	resType := []openapi.ProtoOAPayloadType{openapi.ProtoOAPayloadType_PROTO_OA_ORDER_LIST_RES}

	req := &openapi.ProtoOAOrderListReq{
		CtidTraderAccountId: &account.id,
		FromTimestamp:       &fromTimestamp,
		ToTimestamp:         &toTimestamp,
	}

	res, err := account.client.SendRequest(reqType, resType, DefaultErrResponseTypes(), req, nil)
	if err != nil {
		return nil, err
	}

	v, ok := res.(*openapi.ProtoOAOrderListRes)
	if !ok {
		return nil, errors.New("unexpected res object")
	}

	return v, nil
}

func (account *Account) GetDynamicLeverageByID(leverageId int64) (*openapi.ProtoOAGetDynamicLeverageByIDRes, error) {
	reqType := openapi.ProtoOAPayloadType_PROTO_OA_GET_DYNAMIC_LEVERAGE_REQ
	resType := []openapi.ProtoOAPayloadType{openapi.ProtoOAPayloadType_PROTO_OA_GET_DYNAMIC_LEVERAGE_RES}

	req := &openapi.ProtoOAGetDynamicLeverageByIDReq{
		PayloadType:         &reqType,
		CtidTraderAccountId: &account.id,
		LeverageId:          &leverageId,
	}

	res, err := account.client.SendRequest(reqType, resType, DefaultErrResponseTypes(), req, nil)
	if err != nil {
		return nil, err
	}

	v, ok := res.(*openapi.ProtoOAGetDynamicLeverageByIDRes)
	if !ok {
		return nil, errors.New("unexpected res object")
	}

	return v, nil
}

func (account *Account) DealListByPositionId(positionId int64, fromTimestamp, toTimestamp int64) (*openapi.ProtoOADealListByPositionIdRes, error) {
	reqType := openapi.ProtoOAPayloadType_PROTO_OA_DEAL_LIST_BY_POSITION_ID_REQ
	resType := []openapi.ProtoOAPayloadType{openapi.ProtoOAPayloadType_PROTO_OA_DEAL_LIST_BY_POSITION_ID_RES}

	req := &openapi.ProtoOADealListByPositionIdReq{
		PayloadType:         &reqType,
		CtidTraderAccountId: &account.id,
		PositionId:          &positionId,
		FromTimestamp:       &fromTimestamp,
		ToTimestamp:         &toTimestamp,
	}

	res, err := account.client.SendRequest(reqType, resType, DefaultErrResponseTypes(), req, nil)
	if err != nil {
		return nil, err
	}

	v, ok := res.(*openapi.ProtoOADealListByPositionIdRes)
	if !ok {
		return nil, errors.New("unexpected res object")
	}

	return v, nil
}

func (account *Account) createEventListener(payloadType openapi.ProtoOAPayloadType) error {
	responseHandler, err := account.client.eventBus.ListenFirehose(strconv.Itoa(int(payloadType)))
	if err != nil {
		return err
	}

	responseHandler.Handle(
		func(msg *model.Message) {
			if msg == nil {
				return
			}

			// get account id from msg payload
			var accountId *int64
			reflPayload := reflect.ValueOf(msg.Payload)
			if reflPayload.Kind() == reflect.Ptr && reflPayload.Elem().Kind() == reflect.Struct {
				// common account id field
				refAccountId := reflPayload.Elem().FieldByName("CtidTraderAccountId")
				// CtidTraderAccountIds field in ProtoOAAccountsTokenInvalidatedEvent
				refAccountIds := reflPayload.Elem().FieldByName("CtidTraderAccountIds")

				if refAccountId.Kind() == reflect.Ptr && refAccountId.Elem().Kind() == reflect.Int64 {
					id, ok := refAccountId.Elem().Interface().(int64)
					if !ok {
						panic("account id is not int64")
					}
					accountId = &id
				} else if refAccountIds.Kind() == reflect.Slice {
					ids, ok := refAccountIds.Interface().([]int64)
					if !ok {
						panic("account ids is not []int64")
					}

					for _, id := range ids {
						if id == account.id {
							accountId = &id
						}
						break
					}
				}
			}

			if accountId == nil {
				return
			}

			if accountId == &account.id {
				err := account.eventBus.SendBroadcastMessage(strconv.Itoa(int(payloadType)), msg)
				if err != nil {
					panic(err)
				}
			}
		},
		func(err error) {
			panic(err)
		})

	return nil
}

func (account *Account) On(payloadType openapi.ProtoOAPayloadType) (bus.MessageHandler, error) {
	return account.eventBus.ListenFirehose(strconv.Itoa(int(payloadType)))
}

func (account *Account) OnTrailingSLChange() (bus.MessageHandler, error) {
	return account.On(openapi.ProtoOAPayloadType_PROTO_OA_TRAILING_SL_CHANGED_EVENT)
}

func (account *Account) OnSymbolChange() (bus.MessageHandler, error) {
	return account.On(openapi.ProtoOAPayloadType_PROTO_OA_SYMBOL_CHANGED_EVENT)
}

func (account *Account) OnTraderUpdate() (bus.MessageHandler, error) {
	return account.On(openapi.ProtoOAPayloadType_PROTO_OA_TRADER_UPDATE_EVENT)
}

func (account *Account) OnExecution() (bus.MessageHandler, error) {
	return account.On(openapi.ProtoOAPayloadType_PROTO_OA_EXECUTION_EVENT)
}

func (account *Account) OnSpot() (bus.MessageHandler, error) {
	return account.On(openapi.ProtoOAPayloadType_PROTO_OA_SPOT_EVENT)
}

func (account *Account) OnOrderError() (bus.MessageHandler, error) {
	return account.On(openapi.ProtoOAPayloadType_PROTO_OA_ORDER_ERROR_EVENT)
}

func (account *Account) OnMarginChanged() (bus.MessageHandler, error) {
	return account.On(openapi.ProtoOAPayloadType_PROTO_OA_MARGIN_CHANGED_EVENT)
}

func (account *Account) OnAccountTokenInvalided() (bus.MessageHandler, error) {
	return account.On(openapi.ProtoOAPayloadType_PROTO_OA_ACCOUNTS_TOKEN_INVALIDATED_EVENT)
}

func (account *Account) OnDepth() (bus.MessageHandler, error) {
	return account.On(openapi.ProtoOAPayloadType_PROTO_OA_DEPTH_EVENT)
}

func (account *Account) OnAccountDisconnect() (bus.MessageHandler, error) {
	return account.On(openapi.ProtoOAPayloadType_PROTO_OA_ACCOUNT_DISCONNECT_EVENT)
}

func (account *Account) OnMarginCallUpdate() (bus.MessageHandler, error) {
	return account.On(openapi.ProtoOAPayloadType_PROTO_OA_MARGIN_CALL_UPDATE_EVENT)
}

func (account *Account) OnMarginCallTrigger() (bus.MessageHandler, error) {
	return account.On(openapi.ProtoOAPayloadType_PROTO_OA_MARGIN_CALL_TRIGGER_EVENT)
}
