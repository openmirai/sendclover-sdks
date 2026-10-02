import test from "node:test";
import assert from "node:assert/strict";
import { createHmac, randomBytes } from "node:crypto";
import { readFile } from "node:fs/promises";
import { SendClover, SendCloverError, verifyWebhook } from "../javascript/dist/index.js";

test("SDK authenticates, builds URLs, paginates, and preserves retry keys", async () => {
  const calls = [];
  const client = new SendClover({ apiKey: "test_key", baseUrl: "https://self-hosted.example/", maxRetries: 1, fetch: async (url, init) => {
    calls.push({ url: String(url), init });
    assert.equal(init.headers.authorization, "Bearer test_key");
    if (String(url).includes("/v1/emails?") && calls.filter(x => x.url.includes("/v1/emails?")).length === 1)
      return Response.json({ data: [{ id: "one" }], nextCursor: "next" });
    if (String(url).includes("cursor=next")) return Response.json({ data: [{ id: "two" }], nextCursor: null });
    if (calls.length === 3) return new Response(JSON.stringify({ error: { code: "busy", message: "retry", requestId: "req_1" } }), { status: 503, headers: { "retry-after": "0", "content-type": "application/json" } });
    return Response.json({ id: "sent", status: "queued" });
  }});
  const ids = [];
  for await (const email of client.emails.pages({ limit: 1 })) ids.push(email.id);
  assert.deepEqual(ids, ["one", "two"]);
  await client.emails.send({ from: "a@example.com", to: "b@example.com", subject: "Hi" }, { idempotencyKey: "stable-key" });
  assert.equal(calls[2].init.headers["idempotency-key"], "stable-key");
  assert.equal(calls[3].init.headers["idempotency-key"], "stable-key");
  assert.match(calls[0].url, /^https:\/\/self-hosted\.example\/v1\/emails\?limit=1$/);
});

test("mutations without an idempotency key are not retried and expose typed errors", async () => {
  let calls = 0;
  const client = new SendClover({ apiKey: "key", baseUrl: "http://localhost", fetch: async () => { calls++; return new Response(JSON.stringify({ error: { code: "invalid", message: "Nope", requestId: "req_2" } }), { status: 503, headers: { "content-type": "application/json" } }) } });
  await assert.rejects(client.emails.send({ from: "a", to: "b", subject: "x" }), error => error instanceof SendCloverError && error.code === "invalid" && error.requestId === "req_2");
  assert.equal(calls, 1);
});

test("batch, contact transfer, and campaign test use the API contract", async () => {
  const calls = [];
  const client = new SendClover({ apiKey: "key", baseUrl: "http://localhost:3000", fetch: async (url, init) => {
    calls.push({ path: new URL(url).pathname, method: init.method, body: init.body && JSON.parse(init.body) });
    if (String(url).endsWith("/contacts/export")) return new Response("email\r\na@example.com", { headers: { "content-type": "text/csv" } });
    if (String(url).endsWith("/emails/batch")) return Response.json({ data: [], nextCursor: null });
    if (String(url).endsWith("/contacts/import")) return Response.json({ imported: 1, data: [] });
    return Response.json({ id: "test", status: "queued" });
  }});
  await client.emails.batch([{ from: "a@example.com", to: "b@example.com", subject: "Hi", text: "Body" }]);
  await client.contacts.import({ rows: [{ email: "a@example.com", subscribed: true }] });
  assert.match(await client.contacts.export(), /^email/);
  await client.campaigns.test("campaign", "b@example.com");
  await client.domains.update("domain", { mailFromDomain: "bounce.example.com", warmupLimit: 250 });
  await client.settings.update({ name: "Demo organization", defaultSender: "hello@example.test", topics: ["product"], customProperties: [{ name: "plan", type: "string" }] });
  await client.streams.update("notifications", { paused: true, warmupLimit: 500 });
  await client.streams.create({ name: "Product", kind: "notifications" });
  await client.projects.create({ name: "Product", settings: { locale: "en" } });
  assert.deepEqual(calls, [
    { path: "/v1/emails/batch", method: "POST", body: { emails: [{ from: "a@example.com", to: "b@example.com", subject: "Hi", text: "Body" }] } },
    { path: "/v1/contacts/import", method: "POST", body: { rows: [{ email: "a@example.com", subscribed: true }] } },
    { path: "/v1/contacts/export", method: "GET", body: undefined },
    { path: "/v1/campaigns/campaign/test", method: "POST", body: { to: "b@example.com" } },
    { path: "/v1/domains/domain", method: "PATCH", body: { mailFromDomain: "bounce.example.com", warmupLimit: 250 } },
    { path: "/v1/settings", method: "PATCH", body: { name: "Demo organization", defaultSender: "hello@example.test", topics: ["product"], customProperties: [{ name: "plan", type: "string" }] } },
    { path: "/v1/streams/notifications", method: "PATCH", body: { paused: true, warmupLimit: 500 } },
    { path: "/v1/streams", method: "POST", body: { name: "Product", kind: "notifications" } },
    { path: "/v1/projects", method: "POST", body: { name: "Product", settings: { locale: "en" } } }
  ]);
});

test("constructor rejects unsafe transport settings", () => {
  assert.throws(() => new SendClover({ apiKey: "key", baseUrl: "" }), /baseUrl/);
  assert.throws(() => new SendClover({ apiKey: "key", baseUrl: "ftp://example.com" }), /http/);
  assert.throws(() => new SendClover({ apiKey: "key", baseUrl: "http://localhost", timeoutMs: 0 }), /timeoutMs/);
  assert.throws(() => new SendClover({ apiKey: "key", baseUrl: "http://localhost", maxRetries: 11 }), /maxRetries/);
});

test("timeouts abort requests", async () => {
  const client = new SendClover({ apiKey: "key", baseUrl: "http://localhost", timeoutMs: 5, fetch: (_url, init) => new Promise((_resolve, reject) => init.signal.addEventListener("abort", () => reject(init.signal.reason), { once: true })) });
  await assert.rejects(client.emails.get("never"), error => error instanceof SendCloverError && error.code === "timeout");
});

test("network failures are typed", async () => {
  const client = new SendClover({ apiKey: "key", baseUrl: "http://localhost", maxRetries: 0, fetch: async () => { throw new TypeError("offline") } });
  await assert.rejects(client.emails.get("never"), error => error instanceof SendCloverError && error.code === "network_error" && error.status === 0);
});

test("webhook verification checks HMAC, body, and timestamp", async () => {
  const key = randomBytes(32), secret = `whsec_${key.toString("base64")}`, now = 1_700_000_000_000, timestamp = String(now / 1000), body = '{"type":"delivered"}';
  const signature = createHmac("sha256", key).update(`evt_1.${timestamp}.${body}`).digest("base64");
  const headers = { "webhook-id": "evt_1", "webhook-timestamp": timestamp, "webhook-signature": `v1,${signature}` };
  assert.equal(await verifyWebhook(body, headers, secret, 300, now), true);
  assert.equal(await verifyWebhook(body + " ", headers, secret, 300, now), false);
  assert.equal(await verifyWebhook(body, headers, secret, 300, now + 301_000), false);
});

test("OpenAPI covers the SDK resources and actions", async () => {
  const api = JSON.parse(await readFile(new URL("../openapi.json", import.meta.url)));
  for (const path of ["/v1/emails", "/v1/emails/batch", "/v1/templates", "/v1/domains", "/v1/domains/{id}/verify", "/v1/contacts/import", "/v1/contacts/export", "/v1/campaigns/{id}/send", "/v1/campaigns/{id}/schedule", "/v1/campaigns/{id}/pause", "/v1/campaigns/{id}/resume", "/v1/campaigns/{id}/cancel", "/v1/campaigns/{id}/duplicate", "/v1/campaigns/{id}/test", "/v1/settings", "/v1/streams", "/v1/streams/{id}", "/v1/projects", "/v1/projects/{id}"]) assert.ok(api.paths[path], path);
  assert.equal(api.openapi, "3.1.0");
});
