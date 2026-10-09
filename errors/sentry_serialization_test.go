package errors

import (
	"compress/gzip"
	"encoding/json"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	sentrylib "github.com/getsentry/sentry-go"
)

func cachePrivateSentryFields(event *sentrylib.Event) {
	event.Extra = map[string]interface{}{"email": "private@example.com"}
	if event.Contexts == nil {
		event.Contexts = make(map[string]sentrylib.Context)
	}
	event.Contexts["nested"] = sentrylib.Context{"email": "private@example.com"}
	event.Breadcrumbs = []*sentrylib.Breadcrumb{{Message: "private@example.com"}}
	event.Exception = []sentrylib.Exception{{Value: "private@example.com"}}
	event.User = sentrylib.User{Email: "private@example.com"}
	event.MakeSerializationSafe()
}

func TestSentryCachedSerializationPrivacyWithActualHTTPTransport(t *testing.T) {
	for _, asynchronous := range []bool{false, true} {
		for _, transaction := range []bool{false, true} {
			for _, hook := range []bool{false, true} {
				for _, unsafeSampling := range []bool{false, true} {
					name := "sync/error/pre_capture/safe_sampling"
					if asynchronous {
						name = strings.Replace(name, "sync/", "async/", 1)
					}
					if transaction {
						name = strings.Replace(name, "/error/", "/transaction/", 1)
					}
					if hook {
						name = strings.Replace(name, "/pre_capture/", "/caller_hook/", 1)
					}
					if unsafeSampling {
						name = strings.Replace(name, "/safe_sampling", "/unsafe_sampling", 1)
					}
					t.Run(name, func(t *testing.T) {
						captures := make(chan []byte, 1)
						receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
							var input io.Reader = request.Body
							if request.Header.Get("Content-Encoding") == "gzip" {
								reader, err := gzip.NewReader(request.Body)
								if err != nil {
									t.Error(err)
									return
								}
								defer reader.Close()
								input = reader
							}
							body, err := io.ReadAll(input)
							if err != nil {
								t.Error(err)
								return
							}
							captures <- body
							w.WriteHeader(http.StatusOK)
						}))
						defer receiver.Close()
						previous := sentrylib.CurrentHub().Client()
						defer sentrylib.CurrentHub().BindClient(previous)
						config := SentryAdvancedConfig{SentryConfig: SentryConfig{
							DSN:        strings.Replace(receiver.URL, "://", "://public@", 1) + "/1",
							SampleRate: 1, TracesSampleRate: 1,
						}}
						if !asynchronous {
							config.Transport = sentrylib.NewHTTPSyncTransport()
						}
						calledHook := false
						if hook {
							callerHook := func(event *sentrylib.Event, _ *sentrylib.EventHint) *sentrylib.Event {
								calledHook = true
								cachePrivateSentryFields(event)
								return event
							}
							if transaction {
								config.BeforeSendTransaction = callerHook
							} else {
								config.BeforeSend = callerHook
							}
						}
						reporter := newTestSentry()
						if err := reporter.InitWithAdvancedConfig(config); err != nil {
							t.Fatal(err)
						}
						hub := sentrylib.NewHub(sentrylib.CurrentHub().Client(), sentrylib.NewScope())
						defer hub.Client().Close()
						trace := sentrylib.TraceID{}
						span := sentrylib.SpanID{}
						for i := range trace {
							trace[i] = 0x66
						}
						for i := range span {
							span[i] = 0x66
						}
						sampling := map[string]string{
							"trace_id": trace.String(), "transaction": "checkout",
							"sample_rate": "1", "sampled": "true", "public_key": "public",
						}
						if unsafeSampling {
							sampling["transaction"] = "private@example.com"
						}
						originalSampling := maps.Clone(sampling)
						hub.Scope().SetPropagationContext(sentrylib.PropagationContext{
							TraceID: trace, SpanID: span,
							DynamicSamplingContext: sentrylib.DynamicSamplingContext{Entries: sampling, Frozen: true},
						})
						event := sentrylib.NewEvent()
						event.Message = "safe"
						event.Contexts = map[string]sentrylib.Context{"trace": {"trace_id": trace, "span_id": span}}
						if transaction {
							event.Type = "transaction"
							event.Transaction = "checkout"
							event.StartTime = time.Now().Add(-time.Millisecond)
						}
						if !hook {
							cachePrivateSentryFields(event)
						}
						hub.CaptureEvent(event)
						if calledHook != hook {
							t.Fatal("expected caller hook was not reached")
						}
						if !hub.Flush(time.Second) {
							t.Fatal("transport did not flush")
						}
						select {
						case body := <-captures:
							if strings.Contains(string(body), "private@example.com") {
								t.Fatalf("cached private fields crossed the HTTP export boundary: %s", body)
							}
							parts := strings.SplitN(string(body), "\n", 3)
							if len(parts) != 3 {
								t.Fatal("incomplete Sentry envelope")
							}
							var envelope struct {
								Trace map[string]string `json:"trace"`
							}
							if err := json.Unmarshal([]byte(parts[0]), &envelope); err != nil {
								t.Fatal(err)
							}
							if unsafeSampling && len(envelope.Trace) != 0 {
								t.Fatal("unsafe sampling metadata survived")
							}
							if !unsafeSampling && !reflect.DeepEqual(envelope.Trace, sampling) {
								t.Fatalf("safe sampling metadata changed: %v", envelope.Trace)
							}
							var payload struct {
								Contexts map[string]map[string]interface{} `json:"contexts"`
								User     map[string]interface{}            `json:"user"`
								Sdk      sentrylib.SdkInfo                 `json:"sdk"`
							}
							if err := json.Unmarshal([]byte(parts[2]), &payload); err != nil {
								t.Fatal(err)
							}
							if len(payload.User) != 0 || payload.Contexts["trace"]["trace_id"] != trace.String() || payload.Contexts["trace"]["span_id"] != span.String() || payload.Sdk.Name == "" {
								t.Fatalf("sanitized user or SDK correlation fields changed: %+v", payload)
							}
						case <-time.After(time.Second):
							t.Fatal("event was not exported")
						}
						if !reflect.DeepEqual(sampling, originalSampling) {
							t.Fatal("shared sampling metadata was mutated")
						}
					})
				}
			}
		}
	}
}

func TestSentrySanitizerDiscardsAllClearedSerializationCaches(t *testing.T) {
	event := sentrylib.NewEvent()
	cachePrivateSentryFields(event)
	event.Extra = nil
	event.Contexts = nil
	event.Breadcrumbs = nil
	event.Exception = nil
	event.User = sentrylib.User{}
	event = sanitizeSentryEvent(event)
	encoded, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), "private@example.com") {
		t.Fatalf("cleared public fields retained private cached data: %s", encoded)
	}
}

func TestSentryCacheRebuildPreservesPublicFieldsAndSafeSampling(t *testing.T) {
	original := &sentrylib.Event{}
	fields := reflect.ValueOf(original).Elem()
	for i := 0; i < fields.NumField(); i++ {
		if fields.Field(i).CanSet() {
			seedSentryPublicField(fields.Field(i))
		}
	}
	scope := sentrylib.NewScope()
	sampling := map[string]string{"transaction": "checkout", "sample_rate": "1"}
	scope.SetPropagationContext(sentrylib.PropagationContext{
		DynamicSamplingContext: sentrylib.DynamicSamplingContext{Entries: sampling, Frozen: true},
	})
	contexts := original.Contexts
	original.Contexts = nil
	scope.ApplyToEvent(original, nil, nil)
	original.Contexts = contexts
	rebuilt := reflect.ValueOf(rebuildSanitizedSentryEvent(original)).Elem()
	for i := 0; i < fields.NumField(); i++ {
		if fields.Type().Field(i).IsExported() && !reflect.DeepEqual(fields.Field(i).Interface(), rebuilt.Field(i).Interface()) {
			t.Fatalf("cache rebuild changed SDK public field %s", fields.Type().Field(i).Name)
		}
	}
	if !reflect.DeepEqual(rebuilt.Addr().Interface().(*sentrylib.Event).GetDynamicSamplingContext(), sampling) {
		t.Fatal("safe sampling metadata was not restored")
	}
}
