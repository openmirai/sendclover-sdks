package sendclover

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestAuthRetryAndWebhook(t *testing.T) {
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.Header.Get("Authorization") != "Bearer test" {
			t.Error("missing auth")
		}
		if r.Header.Get("Idempotency-Key") != "stable" {
			t.Error("missing stable key")
		}
		if calls == 1 {
			w.WriteHeader(503)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"id":"e1","status":"queued"}`))
	}))
	defer srv.Close()
	c, _ := New(Config{APIKey: "test", BaseURL: srv.URL, MaxRetries: 1})
	got, err := c.Emails.Send(context.Background(), EmailInput{From: "a@example.com", To: "b@example.com", Subject: "Hi"}, &RequestOptions{IdempotencyKey: "stable"})
	if err != nil || got.ID != "e1" || calls != 2 {
		t.Fatalf("got=%+v err=%v calls=%d", got, err, calls)
	}
	secretBytes := []byte("secret")
	secret := "whsec_" + base64.StdEncoding.EncodeToString(secretBytes)
	now := time.Unix(1700000000, 0)
	ts := "1700000000"
	mac := hmac.New(sha256.New, secretBytes)
	mac.Write([]byte("evt." + ts + ".{}"))
	h := http.Header{"Webhook-Id": {"evt"}, "Webhook-Timestamp": {ts}, "Webhook-Signature": {"v1," + base64.StdEncoding.EncodeToString(mac.Sum(nil))}}
	if !VerifyWebhookAt([]byte("{}"), h, secret, now) {
		t.Fatal("valid signature rejected")
	}
}

func TestContractRoutes(t *testing.T) {
	type call struct{ method, path, body string }
	var calls []call
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		calls = append(calls, call{r.Method, r.URL.Path, string(body)})
		if r.URL.Path == "/v1/contacts/export" {
			w.Header().Set("Content-Type", "text/csv")
			w.Write([]byte("email\r\na@example.com"))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/v1/emails/batch":
			w.Write([]byte(`{"data":[]}`))
		case "/v1/contacts/import":
			w.Write([]byte(`{"imported":1,"data":[]}`))
		default:
			w.Write([]byte(`{"id":"test","status":"queued"}`))
		}
	}))
	defer srv.Close()
	c, _ := New(Config{APIKey: "test", BaseURL: srv.URL})
	c.Emails.Batch(context.Background(), []EmailInput{{From: "a@example.com", To: "b@example.com", Subject: "Hi", Text: "Body"}}, nil)
	c.Contacts.Import(context.Background(), ContactImport{Rows: []ContactInput{{Email: "a@example.com"}}}, nil)
	csv, err := c.Contacts.Export(context.Background())
	if err != nil || !strings.HasPrefix(csv, "email") {
		t.Fatalf("csv=%q err=%v", csv, err)
	}
	c.Campaigns.Test(context.Background(), "campaign", "b@example.com", nil)
	c.Domains.Update(context.Background(), "domain", DomainUpdate{MailFromDomain: "bounce.example.com", WarmupLimit: 250}, nil)
	topics := []string{"product"}
	properties := []CustomProperty{{Name: "plan", Type: CustomPropertyString}}
	defaultSender := "hello@example.test"
	c.Settings.Update(context.Background(), OrganizationSettingsUpdate{Name: "Demo organization", DefaultSender: &defaultSender, Topics: &topics, CustomProperties: &properties}, nil)
	paused := true
	c.Streams.Update(context.Background(), "notifications", StreamUpdate{Paused: &paused, WarmupLimit: 500}, nil)
	c.Streams.Create(context.Background(), StreamInput{Name: "Product", Kind: StreamNotifications}, nil)
	c.Projects.Create(context.Background(), ProjectInput{Name: "Product", Settings: map[string]any{"locale": "en"}}, nil)
	want := []call{{"POST", "/v1/emails/batch", `{"emails":[{"from":"a@example.com","to":"b@example.com","subject":"Hi","text":"Body"}]}`}, {"POST", "/v1/contacts/import", `{"rows":[{"email":"a@example.com"}]}`}, {"GET", "/v1/contacts/export", ""}, {"POST", "/v1/campaigns/campaign/test", `{"to":"b@example.com"}`}}
	want = append(want, call{"PATCH", "/v1/domains/domain", `{"mailFromDomain":"bounce.example.com","warmupLimit":250}`})
	want = append(want, call{"PATCH", "/v1/settings", `{"name":"Demo organization","defaultSender":"hello@example.test","topics":["product"],"customProperties":[{"name":"plan","type":"string"}]}`})
	want = append(want, call{"PATCH", "/v1/streams/notifications", `{"warmupLimit":500,"paused":true}`})
	want = append(want, call{"POST", "/v1/streams", `{"name":"Product","kind":"notifications"}`})
	want = append(want, call{"POST", "/v1/projects", `{"name":"Product","settings":{"locale":"en"}}`})
	for i := range want {
		if calls[i] != want[i] {
			t.Fatalf("call %d = %#v, want %#v", i, calls[i], want[i])
		}
	}
}

func TestSettingsUpdateCanClearAddressesWithoutSendingUnrelatedFields(t *testing.T) {
	var body string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		data, _ := io.ReadAll(r.Body)
		body = string(data)
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"id":"org","name":"Demo","sendRate":1,"dailyLimit":1,"authReserve":0,"settings":{},"branding":{},"defaultSender":null,"replyTo":null,"topics":[],"customProperties":[]}`))
	}))
	defer srv.Close()
	c, _ := New(Config{APIKey: "test", BaseURL: srv.URL})
	empty := ""
	_, err := c.Settings.Update(context.Background(), OrganizationSettingsUpdate{DefaultSender: &empty, ReplyTo: &empty}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if body != `{"defaultSender":"","replyTo":""}` {
		t.Fatalf("body = %s", body)
	}
}
func TestPaginationAndTypedError(t *testing.T) {
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/v1/templates/bad" {
			w.WriteHeader(404)
			w.Write([]byte(`{"error":{"code":"not_found","message":"missing","requestId":"req"}}`))
			return
		}
		if r.URL.Query().Get("cursor") == "next" {
			w.Write([]byte(`{"data":[{"id":"two","name":"Two","subject":"B"}],"nextCursor":null}`))
			return
		}
		w.Write([]byte(`{"data":[{"id":"one","name":"One","subject":"A"}],"nextCursor":"next"}`))
	}))
	defer srv.Close()
	c, _ := New(Config{APIKey: "test", BaseURL: srv.URL})
	it := c.Templates.Iterate(ListOptions{Limit: 1})
	for _, want := range []string{"one", "two"} {
		item, ok, err := it.Next(context.Background())
		if err != nil || !ok || item.ID != want {
			t.Fatalf("item=%+v ok=%v err=%v", item, ok, err)
		}
	}
	_, ok, err := it.Next(context.Background())
	if err != nil || ok {
		t.Fatalf("iterator did not finish")
	}
	_, err = c.Templates.Get(context.Background(), "bad")
	var apiErr *APIError
	if !errors.As(err, &apiErr) || apiErr.Code != "not_found" || apiErr.RequestID != "req" {
		t.Fatalf("unexpected error %#v", err)
	}
}

func TestConfigAndAbortErrors(t *testing.T) {
	for _, config := range []Config{{APIKey: "key"}, {APIKey: "key", BaseURL: "ftp://example.com"}, {APIKey: "key", BaseURL: "http://localhost", Timeout: -time.Second}, {APIKey: "key", BaseURL: "http://localhost", MaxRetries: 11}} {
		if _, err := New(config); err == nil {
			t.Fatalf("accepted invalid config %#v", config)
		}
	}
	c, _ := New(Config{APIKey: "key", BaseURL: "http://127.0.0.1:1"})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := c.Emails.Get(ctx, "id")
	var apiErr *APIError
	if !errors.As(err, &apiErr) || apiErr.Code != "aborted" {
		t.Fatalf("unexpected error %#v", err)
	}
}
