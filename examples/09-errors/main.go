// Example 09: Structured error codes
//
// The errors package turns integer error codes into stable string codes
// (e.g. "AVS0022") with localized, human-readable messages. A registry holds
// the service prefix, code/message tables, and (optionally) Sentry config.
//
// Run:
//
//	go run ./examples/09-errors
package main

import (
	stderrors "errors"
	"fmt"
	"log"
	"net/http"

	"github.com/gofynd/fit-go/errors"
)

func main() {
	// Configure the package-level registry once at startup.
	//   serviceNameCode: prefix for every string code ("AVS" -> "AVS0022")
	//   errorCodes:      named code -> internal int code
	//   messages:        language -> (message id -> text)
	//   messageCodes:    internal int code -> message id
	err := errors.DefaultRegistry.Init(
		"AVS",
		map[string]int{
			"ORDER_NOT_FOUND":  22,
			"PAYMENT_DECLINED": 23,
		},
		map[string]map[int]string{
			"EN": {
				1: "Order not found. Please check the order ID.",
				2: "Payment was declined by the issuer.",
			},
		},
		map[int]int{
			22: 1,
			23: 2,
		},
	)
	if err != nil {
		log.Fatalf("error registry init: %v", err)
	}

	// Wrap a low-level error into a structured, client-safe FitError.
	cause := stderrors.New("mongo: no documents in result")
	fitErr := errors.New(cause, 22).
		SetStatus(http.StatusNotFound).
		SetMeta(map[string]interface{}{"order_id": "o-123"})

	fmt.Println("string code:", fitErr.GetStrCode()) // AVS0022
	fmt.Println("message    :", fitErr.GetMessage()) // AVS0022: Order not found...
	fmt.Println("as error   :", fitErr.Error())

	// Downstream code can recover the structured error from a plain `error`.
	var plain error = fitErr
	if fe, ok := errors.IsFitError(plain); ok {
		fmt.Printf("recovered FitError: code=%s http=%d meta=%v\n",
			fe.GetStrCode(), fe.HTTPStatusCode, fe.Meta)
	}
}
