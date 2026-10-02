package sendclover

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	APIKey, BaseURL string
	Timeout         time.Duration
	MaxRetries      int
	HTTPClient      *http.Client
}
type Client struct {
	apiKey, baseURL string
	retries         int
	http            *http.Client
	Emails          EmailsService
	Templates       TemplatesService
	Domains         DomainsService
	Contacts        ContactsService
	Campaigns       CampaignsService
}
type RequestOptions struct{ IdempotencyKey string }
type ListOptions struct {
	Cursor string
	Limit  int
}
type Page[T any] struct {
	Data       []T     `json:"data"`
	NextCursor *string `json:"nextCursor"`
}
type BatchResult[T any] struct {
	Data []T `json:"data"`
}
type Iterator[T any] struct {
	fetch   func(context.Context, ListOptions) (Page[T], error)
	options ListOptions
	items   []T
	done    bool
}

func (i *Iterator[T]) Next(ctx context.Context) (T, bool, error) {
	var zero T
	for len(i.items) == 0 && !i.done {
		page, err := i.fetch(ctx, i.options)
		if err != nil {
			return zero, false, err
		}
		i.items = page.Data
		if page.NextCursor == nil {
			i.done = true
		} else {
			i.options.Cursor = *page.NextCursor
		}
	}
	if len(i.items) == 0 {
		return zero, false, nil
	}
	item := i.items[0]
	i.items = i.items[1:]
	return item, true, nil
}

type APIError struct {
	Code, Message, RequestID string
	Status                   int
}

func (e *APIError) Error() string { return fmt.Sprintf("sendclover: %s (%s)", e.Message, e.Code) }

type Attachment struct {
	Filename    string `json:"filename"`
	Content     string `json:"content"`
	ContentType string `json:"contentType"`
	ContentID   string `json:"contentId,omitempty"`
}
type EmailInput struct {
	From        string            `json:"from"`
	To          string            `json:"to"`
	Subject     string            `json:"subject"`
	HTML        string            `json:"html,omitempty"`
	Text        string            `json:"text,omitempty"`
	TemplateID  string            `json:"templateId,omitempty"`
	StreamID    string            `json:"streamId,omitempty"`
	Variables   map[string]any    `json:"variables,omitempty"`
	ReplyTo     string            `json:"replyTo,omitempty"`
	Attachments []Attachment      `json:"attachments,omitempty"`
	Headers     map[string]string `json:"headers,omitempty"`
	ScheduledAt string            `json:"scheduledAt,omitempty"`
	ExpiresAt   string            `json:"expiresAt,omitempty"`
	Topic       string            `json:"topic,omitempty"`
	Tags        map[string]string `json:"tags,omitempty"`
	Metadata    map[string]any    `json:"metadata,omitempty"`
}
type Email struct {
	ID                string `json:"id"`
	Status            string `json:"status"`
	FromEmail         string `json:"fromEmail,omitempty"`
	ToEmail           string `json:"toEmail,omitempty"`
	Subject           string `json:"subject,omitempty"`
	StreamID          string `json:"streamId,omitempty"`
	DomainID          string `json:"domainId,omitempty"`
	CampaignID        string `json:"campaignId,omitempty"`
	ContactID         string `json:"contactId,omitempty"`
	ScheduledAt       int64  `json:"scheduledAt,omitempty"`
	ExpiresAt         *int64 `json:"expiresAt,omitempty"`
	CreatedAt         int64  `json:"createdAt,omitempty"`
	UpdatedAt         int64  `json:"updatedAt,omitempty"`
	Attempts          int    `json:"attempts,omitempty"`
	ProviderMessageID string `json:"providerMessageId,omitempty"`
	LastError         string `json:"lastError,omitempty"`
	Timeline          []any  `json:"timeline,omitempty"`
}
type TemplateInput struct {
	Name    string `json:"name"`
	Subject string `json:"subject"`
	HTML    string `json:"html,omitempty"`
	Text    string `json:"text,omitempty"`
}
type Template struct {
	TemplateInput
	ID        string `json:"id"`
	TextBody  string `json:"textBody,omitempty"`
	CreatedAt int64  `json:"createdAt,omitempty"`
	UpdatedAt int64  `json:"updatedAt,omitempty"`
}
type DomainInput struct {
	Name           string `json:"name"`
	MailFromDomain string `json:"mailFromDomain,omitempty"`
}
type DomainUpdate struct {
	MailFromDomain string `json:"mailFromDomain,omitempty"`
	WarmupLimit    int    `json:"warmupLimit,omitempty"`
}
type Domain struct {
	DomainInput
	ID          string `json:"id"`
	Status      string `json:"status"`
	WarmupLimit int    `json:"warmupLimit,omitempty"`
	DNSRecords  []any  `json:"dnsRecords,omitempty"`
	CreatedAt   int64  `json:"createdAt,omitempty"`
	UpdatedAt   int64  `json:"updatedAt,omitempty"`
}
type ContactInput struct {
	Email         string          `json:"email"`
	FirstName     string          `json:"firstName,omitempty"`
	LastName      string          `json:"lastName,omitempty"`
	Properties    map[string]any  `json:"properties,omitempty"`
	Tags          []string        `json:"tags,omitempty"`
	Lists         []string        `json:"lists,omitempty"`
	Subscribed    *bool           `json:"subscribed,omitempty"`
	ConsentSource string          `json:"consentSource,omitempty"`
	ConsentAt     string          `json:"consentAt,omitempty"`
	Topics        map[string]bool `json:"topics,omitempty"`
	Preferences   map[string]any  `json:"preferences,omitempty"`
}
type Contact struct {
	ContactInput
	ID             string `json:"id"`
	ConsentAt      int64  `json:"consentAt,omitempty"`
	UnsubscribedAt *int64 `json:"unsubscribedAt,omitempty"`
	CreatedAt      int64  `json:"createdAt,omitempty"`
	UpdatedAt      int64  `json:"updatedAt,omitempty"`
}
type ContactImport struct {
	Rows []ContactInput `json:"rows,omitempty"`
	CSV  string         `json:"csv,omitempty"`
}
type ContactImportResult struct {
	Imported int       `json:"imported"`
	Data     []Contact `json:"data,omitempty"`
}
type CampaignInput struct {
	Name        string         `json:"name"`
	From        string         `json:"from"`
	StreamID    string         `json:"streamId"`
	TemplateID  string         `json:"templateId,omitempty"`
	Subject     string         `json:"subject,omitempty"`
	HTML        string         `json:"html,omitempty"`
	Text        string         `json:"text,omitempty"`
	Audience    map[string]any `json:"audience,omitempty"`
	ScheduledAt string         `json:"scheduledAt,omitempty"`
}
type Campaign struct {
	ID          string         `json:"id"`
	Name        string         `json:"name"`
	Status      string         `json:"status"`
	FromEmail   string         `json:"fromEmail,omitempty"`
	StreamID    string         `json:"streamId,omitempty"`
	TemplateID  string         `json:"templateId,omitempty"`
	Subject     string         `json:"subject,omitempty"`
	HTML        string         `json:"html,omitempty"`
	TextBody    string         `json:"textBody,omitempty"`
	Audience    map[string]any `json:"audience,omitempty"`
	ScheduledAt int64          `json:"scheduledAt,omitempty"`
	CreatedAt   int64          `json:"createdAt,omitempty"`
	UpdatedAt   int64          `json:"updatedAt,omitempty"`
}

type EmailsService struct{ c *Client }
type TemplatesService struct{ c *Client }
type DomainsService struct{ c *Client }
type ContactsService struct{ c *Client }
type CampaignsService struct{ c *Client }

func New(config Config) (*Client, error) {
	if config.APIKey == "" {
		return nil, errors.New("sendclover: APIKey is required")
	}
	if config.BaseURL == "" {
		return nil, errors.New("sendclover: BaseURL is required")
	}
	parsed, err := url.Parse(config.BaseURL)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
		return nil, errors.New("sendclover: BaseURL must be an absolute http(s) URL")
	}
	if config.Timeout < 0 {
		return nil, errors.New("sendclover: Timeout must be positive")
	}
	if config.MaxRetries < 0 || config.MaxRetries > 10 {
		return nil, errors.New("sendclover: MaxRetries must be from 0 to 10")
	}
	if config.Timeout == 0 {
		config.Timeout = 30 * time.Second
	}
	if config.HTTPClient == nil {
		config.HTTPClient = &http.Client{Timeout: config.Timeout}
	}
	c := &Client{apiKey: config.APIKey, baseURL: strings.TrimRight(config.BaseURL, "/"), retries: config.MaxRetries, http: config.HTTPClient}
	if c.retries == 0 {
		c.retries = 2
	}
	c.Emails.c = c
	c.Templates.c = c
	c.Domains.c = c
	c.Contacts.c = c
	c.Campaigns.c = c
	return c, nil
}

func (c *Client) do(ctx context.Context, method, path string, input, output any, opts *RequestOptions) error {
	var body []byte
	var err error
	if input != nil {
		body, err = json.Marshal(input)
		if err != nil {
			return transportError(err)
		}
	}
	retryable := method == http.MethodGet || opts != nil && opts.IdempotencyKey != ""
	for attempt := 0; ; attempt++ {
		req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, bytes.NewReader(body))
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+c.apiKey)
		req.Header.Set("Content-Type", "application/json")
		if opts != nil && opts.IdempotencyKey != "" {
			req.Header.Set("Idempotency-Key", opts.IdempotencyKey)
		}
		resp, err := c.http.Do(req)
		if err != nil {
			if retryable && attempt < c.retries {
				if err := wait(ctx, retryDelay("", attempt)); err != nil {
					return transportError(err)
				}
				continue
			}
			return err
		}
		data, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return transportError(readErr)
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			if output == nil || resp.StatusCode == 204 {
				return nil
			}
			if text, ok := output.(*string); ok {
				*text = string(data)
				return nil
			}
			return json.Unmarshal(data, output)
		}
		if retryable && attempt < c.retries && (resp.StatusCode == 429 || resp.StatusCode >= 500) {
			if err := wait(ctx, retryDelay(resp.Header.Get("Retry-After"), attempt)); err != nil {
				return transportError(err)
			}
			continue
		}
		var envelope struct {
			Error struct{ Code, Message, RequestID string } `json:"error"`
		}
		_ = json.Unmarshal(data, &envelope)
		if envelope.Error.Code == "" {
			envelope.Error.Code = "api_error"
		}
		if envelope.Error.Message == "" {
			envelope.Error.Message = resp.Status
		}
		return &APIError{Code: envelope.Error.Code, Message: envelope.Error.Message, RequestID: envelope.Error.RequestID, Status: resp.StatusCode}
	}
}
func transportError(err error) *APIError {
	code := "network_error"
	if errors.Is(err, context.DeadlineExceeded) {
		code = "timeout"
	} else if errors.Is(err, context.Canceled) {
		code = "aborted"
	}
	return &APIError{Code: code, Message: err.Error()}
}
func wait(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
func retryDelay(value string, attempt int) time.Duration {
	if n, err := strconv.Atoi(value); err == nil {
		if n < 0 {
			n = 0
		}
		return time.Duration(n) * time.Second
	}
	if t, err := http.ParseTime(value); err == nil {
		if d := time.Until(t); d > 0 {
			return d
		}
		return 0
	}
	return 250 * time.Millisecond * time.Duration(1<<attempt)
}
func list[T any](ctx context.Context, c *Client, path string, o ListOptions) (Page[T], error) {
	q := url.Values{}
	if o.Cursor != "" {
		q.Set("cursor", o.Cursor)
	}
	if o.Limit != 0 {
		q.Set("limit", strconv.Itoa(o.Limit))
	}
	if len(q) > 0 {
		path += "?" + q.Encode()
	}
	var page Page[T]
	err := c.do(ctx, http.MethodGet, path, nil, &page, nil)
	return page, err
}
func action[T any](ctx context.Context, c *Client, resource, id, name string, input any, opts *RequestOptions) (T, error) {
	var out T
	err := c.do(ctx, http.MethodPost, "/v1/"+resource+"/"+url.PathEscape(id)+"/"+name, input, &out, opts)
	return out, err
}

func (s EmailsService) Send(ctx context.Context, in EmailInput, o *RequestOptions) (Email, error) {
	var out Email
	err := s.c.do(ctx, "POST", "/v1/emails", in, &out, o)
	return out, err
}
func (s EmailsService) Batch(ctx context.Context, emails []EmailInput, o *RequestOptions) (BatchResult[Email], error) {
	var out BatchResult[Email]
	err := s.c.do(ctx, "POST", "/v1/emails/batch", map[string]any{"emails": emails}, &out, o)
	return out, err
}
func (s EmailsService) Get(ctx context.Context, id string) (Email, error) {
	var out Email
	err := s.c.do(ctx, "GET", "/v1/emails/"+url.PathEscape(id), nil, &out, nil)
	return out, err
}
func (s EmailsService) List(ctx context.Context, o ListOptions) (Page[Email], error) {
	return list[Email](ctx, s.c, "/v1/emails", o)
}
func (s EmailsService) Iterate(o ListOptions) *Iterator[Email] {
	return &Iterator[Email]{fetch: s.List, options: o}
}

func (s TemplatesService) Create(ctx context.Context, in TemplateInput, o *RequestOptions) (Template, error) {
	var out Template
	err := s.c.do(ctx, "POST", "/v1/templates", in, &out, o)
	return out, err
}
func (s TemplatesService) Get(ctx context.Context, id string) (Template, error) {
	var out Template
	err := s.c.do(ctx, "GET", "/v1/templates/"+url.PathEscape(id), nil, &out, nil)
	return out, err
}
func (s TemplatesService) Update(ctx context.Context, id string, in TemplateInput, o *RequestOptions) (Template, error) {
	var out Template
	err := s.c.do(ctx, "PATCH", "/v1/templates/"+url.PathEscape(id), in, &out, o)
	return out, err
}
func (s TemplatesService) Delete(ctx context.Context, id string, o *RequestOptions) error {
	return s.c.do(ctx, "DELETE", "/v1/templates/"+url.PathEscape(id), nil, nil, o)
}
func (s TemplatesService) List(ctx context.Context, o ListOptions) (Page[Template], error) {
	return list[Template](ctx, s.c, "/v1/templates", o)
}
func (s TemplatesService) Iterate(o ListOptions) *Iterator[Template] {
	return &Iterator[Template]{fetch: s.List, options: o}
}

func (s DomainsService) Create(ctx context.Context, in DomainInput, o *RequestOptions) (Domain, error) {
	var out Domain
	err := s.c.do(ctx, "POST", "/v1/domains", in, &out, o)
	return out, err
}
func (s DomainsService) Get(ctx context.Context, id string) (Domain, error) {
	var out Domain
	err := s.c.do(ctx, "GET", "/v1/domains/"+url.PathEscape(id), nil, &out, nil)
	return out, err
}
func (s DomainsService) Update(ctx context.Context, id string, in DomainUpdate, o *RequestOptions) (Domain, error) {
	var out Domain
	err := s.c.do(ctx, "PATCH", "/v1/domains/"+url.PathEscape(id), in, &out, o)
	return out, err
}
func (s DomainsService) Delete(ctx context.Context, id string, o *RequestOptions) error {
	return s.c.do(ctx, "DELETE", "/v1/domains/"+url.PathEscape(id), nil, nil, o)
}
func (s DomainsService) List(ctx context.Context, o ListOptions) (Page[Domain], error) {
	return list[Domain](ctx, s.c, "/v1/domains", o)
}
func (s DomainsService) Iterate(o ListOptions) *Iterator[Domain] {
	return &Iterator[Domain]{fetch: s.List, options: o}
}
func (s DomainsService) Verify(ctx context.Context, id string, o *RequestOptions) (Domain, error) {
	return action[Domain](ctx, s.c, "domains", id, "verify", map[string]any{}, o)
}

func (s ContactsService) Create(ctx context.Context, in ContactInput, o *RequestOptions) (Contact, error) {
	var out Contact
	err := s.c.do(ctx, "POST", "/v1/contacts", in, &out, o)
	return out, err
}
func (s ContactsService) Get(ctx context.Context, id string) (Contact, error) {
	var out Contact
	err := s.c.do(ctx, "GET", "/v1/contacts/"+url.PathEscape(id), nil, &out, nil)
	return out, err
}
func (s ContactsService) Update(ctx context.Context, id string, in ContactInput, o *RequestOptions) (Contact, error) {
	var out Contact
	err := s.c.do(ctx, "PATCH", "/v1/contacts/"+url.PathEscape(id), in, &out, o)
	return out, err
}
func (s ContactsService) Delete(ctx context.Context, id string, o *RequestOptions) error {
	return s.c.do(ctx, "DELETE", "/v1/contacts/"+url.PathEscape(id), nil, nil, o)
}
func (s ContactsService) List(ctx context.Context, o ListOptions) (Page[Contact], error) {
	return list[Contact](ctx, s.c, "/v1/contacts", o)
}
func (s ContactsService) Iterate(o ListOptions) *Iterator[Contact] {
	return &Iterator[Contact]{fetch: s.List, options: o}
}
func (s ContactsService) Import(ctx context.Context, input ContactImport, o *RequestOptions) (ContactImportResult, error) {
	var out ContactImportResult
	err := s.c.do(ctx, "POST", "/v1/contacts/import", input, &out, o)
	return out, err
}
func (s ContactsService) Export(ctx context.Context) (string, error) {
	var out string
	err := s.c.do(ctx, "GET", "/v1/contacts/export", nil, &out, nil)
	return out, err
}

func (s CampaignsService) Create(ctx context.Context, in CampaignInput, o *RequestOptions) (Campaign, error) {
	var out Campaign
	err := s.c.do(ctx, "POST", "/v1/campaigns", in, &out, o)
	return out, err
}
func (s CampaignsService) Get(ctx context.Context, id string) (Campaign, error) {
	var out Campaign
	err := s.c.do(ctx, "GET", "/v1/campaigns/"+url.PathEscape(id), nil, &out, nil)
	return out, err
}
func (s CampaignsService) Update(ctx context.Context, id string, in CampaignInput, o *RequestOptions) (Campaign, error) {
	var out Campaign
	err := s.c.do(ctx, "PATCH", "/v1/campaigns/"+url.PathEscape(id), in, &out, o)
	return out, err
}
func (s CampaignsService) Delete(ctx context.Context, id string, o *RequestOptions) error {
	return s.c.do(ctx, "DELETE", "/v1/campaigns/"+url.PathEscape(id), nil, nil, o)
}
func (s CampaignsService) List(ctx context.Context, o ListOptions) (Page[Campaign], error) {
	return list[Campaign](ctx, s.c, "/v1/campaigns", o)
}
func (s CampaignsService) Iterate(o ListOptions) *Iterator[Campaign] {
	return &Iterator[Campaign]{fetch: s.List, options: o}
}
func (s CampaignsService) Send(ctx context.Context, id string, o *RequestOptions) (Campaign, error) {
	return action[Campaign](ctx, s.c, "campaigns", id, "send", map[string]any{}, o)
}
func (s CampaignsService) Schedule(ctx context.Context, id string, at time.Time, o *RequestOptions) (Campaign, error) {
	return action[Campaign](ctx, s.c, "campaigns", id, "schedule", map[string]any{"scheduledAt": at.Format(time.RFC3339)}, o)
}
func (s CampaignsService) Pause(ctx context.Context, id string, o *RequestOptions) (Campaign, error) {
	return action[Campaign](ctx, s.c, "campaigns", id, "pause", map[string]any{}, o)
}
func (s CampaignsService) Resume(ctx context.Context, id string, o *RequestOptions) (Campaign, error) {
	return action[Campaign](ctx, s.c, "campaigns", id, "resume", map[string]any{}, o)
}
func (s CampaignsService) Cancel(ctx context.Context, id string, o *RequestOptions) (Campaign, error) {
	return action[Campaign](ctx, s.c, "campaigns", id, "cancel", map[string]any{}, o)
}
func (s CampaignsService) Duplicate(ctx context.Context, id string, o *RequestOptions) (Campaign, error) {
	return action[Campaign](ctx, s.c, "campaigns", id, "duplicate", map[string]any{}, o)
}
func (s CampaignsService) Test(ctx context.Context, id, to string, o *RequestOptions) (Email, error) {
	return action[Email](ctx, s.c, "campaigns", id, "test", map[string]any{"to": to}, o)
}

func VerifyWebhook(body []byte, headers http.Header, secret string) bool {
	return VerifyWebhookAt(body, headers, secret, time.Now())
}

// VerifyWebhookAt is VerifyWebhook with an injectable clock for deterministic tests.
func VerifyWebhookAt(body []byte, headers http.Header, secret string, now time.Time) bool {
	id, ts, signature := headers.Get("Webhook-Id"), headers.Get("Webhook-Timestamp"), headers.Get("Webhook-Signature")
	if id == "" || ts == "" || !strings.HasPrefix(signature, "v1,") || !strings.HasPrefix(secret, "whsec_") {
		return false
	}
	seconds, err := strconv.ParseInt(ts, 10, 64)
	if err != nil || now.Sub(time.Unix(seconds, 0)) > 5*time.Minute || time.Unix(seconds, 0).Sub(now) > 5*time.Minute {
		return false
	}
	key, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(secret, "whsec_"))
	if err != nil {
		return false
	}
	expected, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(signature, "v1,"))
	if err != nil {
		return false
	}
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(id + "." + ts + "."))
	mac.Write(body)
	return hmac.Equal(mac.Sum(nil), expected)
}
