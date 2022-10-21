package ctrader

import (
	. "github.com/smartystreets/goconvey/convey"
	"github.com/ty2/ctrader-go/proto/openapi"
	"testing"
	"time"
)

var (
	ClientID     = ""
	ClientSecret = ""
	Token        = ""
	AccountID    = int64(0)
	Host         = "demo.ctraderapi.com:5035"
)

var client *Client

func TestAccountUnitTest(t *testing.T) {
	Convey("setup", t, func() {
		conn := NewConn(Host)
		client = NewClient(conn, ClientID, ClientSecret, Token)
		err := client.Connect()
		So(err, ShouldEqual, nil)
		_, err = client.ApplicationAuth()
		So(err, ShouldEqual, nil)

		account, err := client.Account(AccountID)

		Convey("AssetsList", func(c C) {
			res, err := account.AssetsList()
			So(err, ShouldEqual, nil)
			So(res, ShouldNotEqual, nil)

		})
		Convey("SymbolList", func(c C) {
			res, err := account.SymbolList()
			So(err, ShouldEqual, nil)
			So(res, ShouldNotEqual, nil)
		})
		Convey("SymbolById", func(c C) {
			res, err := account.SymbolById([]int64{1})
			So(err, ShouldEqual, nil)
			So(res, ShouldNotEqual, nil)
		})
		Convey("SymbolsForConversion", func(c C) {
			res, err := account.SymbolsForConversion(1, 2)
			So(err, ShouldEqual, nil)
			So(res, ShouldNotEqual, nil)
		})

		Convey("AssetClassList", func(c C) {
			res, err := account.AssetClassList()
			So(err, ShouldEqual, nil)
			So(res, ShouldNotEqual, nil)
		})

		Convey("Trader", func(c C) {
			res, err := account.Trader()
			So(err, ShouldEqual, nil)
			So(res, ShouldNotEqual, nil)
		})

		Convey("DealList", func(c C) {
			res, err := account.DealList(time.Now().Add(-time.Hour*48).Unix()*1000, time.Now().Unix()*1000, nil)
			So(err, ShouldEqual, nil)
			So(res, ShouldNotEqual, nil)
		})

		Convey("ExpectedMargin", func(c C) {
			res, err := account.ExpectedMargin(1, []int64{1000})
			So(err, ShouldEqual, nil)
			So(res, ShouldNotEqual, nil)
		})

		Convey("CashFlowHistoryList", func(c C) {
			res, err := account.CashFlowHistoryList(time.Now().Add(-time.Hour*48).Unix()*1000, time.Now().Unix()*1000)
			So(err, ShouldEqual, nil)
			So(res, ShouldNotEqual, nil)
		})

		Convey("SubscribeSpots", func(c C) {
			res, err := account.SubscribeSpots([]int64{1})
			So(err, ShouldEqual, nil)
			So(res, ShouldNotEqual, nil)

			Convey("SubscribeLiveTrendbar", func(c C) {
				res, err := account.SubscribeLiveTrendbar(1, openapi.ProtoOATrendbarPeriod_M1)
				So(err, ShouldEqual, nil)
				So(res, ShouldNotEqual, nil)

				Convey("UnsubscribeLiveTrendbar", func(c C) {
					res, err := account.UnsubscribeLiveTrendbar(1, openapi.ProtoOATrendbarPeriod_M1)
					So(err, ShouldEqual, nil)
					So(res, ShouldNotEqual, nil)
				})
			})

			Convey("UnsubscribeSpots", func(c C) {
				res, err := account.UnsubscribeSpots([]int64{1})
				So(err, ShouldEqual, nil)
				So(res, ShouldNotEqual, nil)
			})
		})

		Convey("GetTrendbars", func(c C) {
			res, err := account.GetTrendbars(time.Now().Add(-time.Minute*40).Unix()*1000, time.Now().Unix()*1000, openapi.ProtoOATrendbarPeriod_M1, 1, 100)
			So(err, ShouldEqual, nil)
			So(res, ShouldNotEqual, nil)
		})

		Convey("GetTickData", func(c C) {
			res, err := account.GetTickData(1, openapi.ProtoOAQuoteType_ASK, time.Now().Add(-time.Hour*48).Unix()*1000, time.Now().Unix()*1000)
			So(err, ShouldEqual, nil)
			So(res, ShouldNotEqual, nil)
		})

		Convey("SubscribeDepthQuotes", func(c C) {
			res, err := account.SubscribeDepthQuotes([]int64{1})
			So(err, ShouldEqual, nil)
			So(res, ShouldNotEqual, nil)

			Convey("UnsubscribeDepthQuotes", func(c C) {
				res, err := account.UnsubscribeDepthQuotes([]int64{1})
				So(err, ShouldEqual, nil)
				So(res, ShouldNotEqual, nil)
			})
		})

		Convey("SymbolCategoryList", func(c C) {
			res, err := account.SymbolCategoryList()
			So(err, ShouldEqual, nil)
			So(res, ShouldNotEqual, nil)
		})

		Convey("AccountLogout", func(c C) {
			res, err := account.AccountLogout()
			So(err, ShouldEqual, nil)
			So(res, ShouldNotEqual, nil)
		})

		Convey("MarginCallList", func(c C) {
			res, err := account.MarginCallList()
			So(err, ShouldEqual, nil)
			So(res, ShouldNotEqual, nil)
		})

		Convey("GetDynamicLeverageByID", func(c C) {
			res, err := account.GetDynamicLeverageByID(2)
			So(err, ShouldNotEqual, nil)
			So(res, ShouldEqual, nil)
		})

		// TODO
		Convey("DealListByPositionId", func(c C) {
			res, err := account.DealListByPositionId(37557816, time.Now().Add(-time.Hour*48).Unix()*1000, time.Now().Unix()*1000)

			So(err, ShouldEqual, nil)
			So(res, ShouldNotEqual, nil)
		})

		Convey("MarginCallUpdate", func(c C) {
			res, err := account.MarginCallUpdate(openapi.ProtoOANotificationType_MARGIN_LEVEL_THRESHOLD_1, 1.5)

			So(err, ShouldEqual, nil)
			So(res, ShouldNotEqual, nil)
		})

		Convey("NewOrder", func(c C) {
			symbolId := int64(1)
			orderType := openapi.ProtoOAOrderType_LIMIT
			tradeSide := openapi.ProtoOATradeSide_BUY
			volume := int64(100000.00)
			limitPrice := 1.0
			timeInForce := openapi.ProtoOATimeInForce_GOOD_TILL_CANCEL
			orderRes, err := account.NewOrder(&openapi.ProtoOANewOrderReq{
				SymbolId:    &symbolId,
				OrderType:   &orderType,
				TradeSide:   &tradeSide,
				Volume:      &volume,
				LimitPrice:  &limitPrice,
				TimeInForce: &timeInForce,
			})

			So(err, ShouldEqual, nil)
			So(orderRes, ShouldNotEqual, nil)

			Convey("OrderList", func(c C) {
				res, err := account.OrderList(time.Now().Add(-time.Hour*48).Unix()*1000, time.Now().Unix()*1000)

				So(err, ShouldEqual, nil)
				So(res, ShouldNotEqual, nil)
			})

			Convey("Reconcile", func(c C) {
				res, err := account.Reconcile()

				So(err, ShouldEqual, nil)
				So(res, ShouldNotEqual, nil)
			})

			Convey("Cancel Order", func(c C) {
				res, err := account.CancelOrder(*orderRes.Order.OrderId)

				So(err, ShouldEqual, nil)
				So(res, ShouldNotEqual, nil)
			})

		})
	})
}
