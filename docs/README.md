# SendClover SDKs

Both SDKs call the SendClover REST API; applications never receive database, queue, storage, or email-provider credentials. The API contract is [`openapi.json`](../openapi.json). Use a restricted SendClover API key and keep it on your server. `baseUrl`/`BaseURL` is required and should point to your Worker (use `http://localhost:3000` for local development).

## JavaScript / TypeScript

The package name is `@sendclover/sdk` but version 0.1.0 is not published yet. Build a local package:

```sh
cd javascript
npm install
npm run build
npm pack
# From your application directory:
npm install /path/to/sendclover-sdks/javascript/sendclover-sdk-0.1.0.tgz
```

```ts
import { SendClover } from "@sendclover/sdk";
const client = new SendClover({ apiKey: process.env.SENDCLOVER_API_KEY!, baseUrl: "https://sendclover.example.com" });
await client.emails.send({ from: "hello@example.com", to: "user@example.net", subject: "Hello", text: "Welcome" }, { idempotencyKey: crypto.randomUUID() });
```

All list resources expose `list()` and async `pages()`. Requests accept an `AbortSignal`; client configuration accepts `timeoutMs` and `maxRetries`. Errors are `SendCloverError` instances with `code`, `status`, and `requestId`.

Domain updates accept `mailFromDomain` and a positive integer `warmupLimit`.

## Go

The module is hosted in the public SDK repository:

```go
go get github.com/openmirai/sendclover-sdks/go@main
```

```go
client, err := sendclover.New(sendclover.Config{APIKey: os.Getenv("SENDCLOVER_API_KEY"), BaseURL: "https://sendclover.example.com"})
if err != nil { log.Fatal(err) }
_, err = client.Emails.Send(context.Background(), sendclover.EmailInput{From: "hello@example.com", To: "user@example.net", Subject: "Hello", Text: "Welcome"}, &sendclover.RequestOptions{IdempotencyKey: uuidFromYourApplication})
```

Go methods accept `context.Context` for cancellation and timeouts. API failures are `*sendclover.APIError`. Each list service also has `Iterate`; call `Next(ctx)` until its boolean result is false.

## Retries and webhooks

GET requests retry transient 429 and 5xx responses. Mutations retry only when the caller provides one stable idempotency key; the SDK never creates a new key during a retry. Both SDKs honor `Retry-After`.

Webhook verification uses the raw request body and `webhook-id`, `webhook-timestamp`, and `webhook-signature` headers. Secrets have the form `whsec_<base64>` and signatures are `v1,<base64 HMAC-SHA256(id.timestamp.raw-body)>`; timestamps outside five minutes are rejected.
