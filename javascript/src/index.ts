export type EmailStatus = "scheduled" | "queued" | "submitting" | "accepted_by_ses" | "delivery_delayed" | "delivered" | "bounced" | "rejected" | "failed" | "suppressed" | "cancelled" | "expired" | "uncertain";
export interface Attachment { filename: string; content: string; contentType: string; contentId?: string }
export interface EmailInput { from: string; to: string; subject?: string; html?: string; text?: string; templateId?: string; streamId?: string; variables?: Record<string, unknown>; replyTo?: string; attachments?: Attachment[]; headers?: Record<string, string>; tags?: Record<string, string>; metadata?: Record<string, unknown>; scheduledAt?: string; expiresAt?: string; topic?: string }
export interface Email { id: string; status: EmailStatus; fromEmail?: string; toEmail?: string; subject?: string; streamId?: string; domainId?: string; campaignId?: string; contactId?: string; scheduledAt?: number; expiresAt?: number | null; createdAt?: number; updatedAt?: number; attempts?: number; providerMessageId?: string; lastError?: string; timeline?: unknown[] }
export interface TemplateInput { name: string; subject: string; html?: string; text?: string }
export interface Template { id: string; name: string; subject: string; html?: string; textBody?: string; createdAt?: number; updatedAt?: number; rendered?: { subject: string; html: string; text: string }; versions?: unknown[] }
export interface DomainInput { name: string; mailFromDomain?: string }
export interface DomainUpdate { mailFromDomain?: string; warmupLimit?: number }
export interface Domain { id: string; name: string; status: string; mailFromDomain?: string; warmupLimit?: number; dnsRecords?: unknown[]; createdAt?: number; updatedAt?: number }
export interface ContactInput { email: string; firstName?: string; lastName?: string; properties?: Record<string, unknown>; tags?: string[]; lists?: string[]; subscribed?: boolean; consentSource?: string; consentAt?: string; topics?: Record<string, boolean>; preferences?: Record<string, unknown> }
export interface Contact extends Omit<ContactInput, "consentAt"> { id: string; consentAt?: number; createdAt?: number; updatedAt?: number }
export interface CampaignInput { name: string; from: string; streamId: string; templateId?: string; subject?: string; html?: string; text?: string; audience?: Record<string, unknown>; scheduledAt?: string }
export interface Campaign { id: string; name: string; status: string; fromEmail: string; streamId: string; templateId?: string; subject?: string; html?: string; textBody?: string; audience?: Record<string, unknown>; scheduledAt?: number; createdAt?: number; updatedAt?: number }
export interface Page<T> { data: T[]; nextCursor: string | null }
export interface BatchResult<T> { data: T[] }
export interface ListOptions { cursor?: string; limit?: number; signal?: AbortSignal }
export interface RequestOptions { signal?: AbortSignal; idempotencyKey?: string }
export interface SendCloverOptions { apiKey: string; baseUrl: string; timeoutMs?: number; maxRetries?: number; fetch?: typeof fetch }

export class SendCloverError extends Error {
  constructor(public code: string, message: string, public status: number, public requestId?: string) { super(message); this.name = "SendCloverError" }
}

const sleep = (ms: number, signal?: AbortSignal) => new Promise<void>((resolve, reject) => {
  if (signal?.aborted) return reject(signal.reason);
  const abort = () => { clearTimeout(id); reject(signal?.reason) };
  const id = setTimeout(() => { signal?.removeEventListener("abort", abort); resolve() }, ms);
  signal?.addEventListener("abort", abort, { once: true });
});

export class SendClover {
  readonly emails;
  readonly templates;
  readonly domains;
  readonly contacts;
  readonly campaigns;
  readonly webhooks = { verify: verifyWebhook };
  private baseUrl: string;
  private timeoutMs: number;
  private maxRetries: number;
  private fetcher: typeof fetch;

  constructor(private options: SendCloverOptions) {
    if (!options.apiKey) throw new TypeError("apiKey is required");
    if (!options.baseUrl) throw new TypeError("baseUrl is required");
    const baseUrl = new URL(options.baseUrl);
    if (!/^https?:$/.test(baseUrl.protocol)) throw new TypeError("baseUrl must use http or https");
    if (options.timeoutMs !== undefined && (!Number.isFinite(options.timeoutMs) || options.timeoutMs <= 0)) throw new TypeError("timeoutMs must be positive");
    if (options.maxRetries !== undefined && (!Number.isInteger(options.maxRetries) || options.maxRetries < 0 || options.maxRetries > 10)) throw new TypeError("maxRetries must be an integer from 0 to 10");
    this.baseUrl = options.baseUrl.replace(/\/$/, "");
    this.timeoutMs = options.timeoutMs ?? 30_000;
    this.maxRetries = options.maxRetries ?? 2;
    this.fetcher = options.fetch ?? fetch;
    const listing = <T>(name: string) => ({ list: (opts?: ListOptions) => this.list<T>(`/v1/${name}`, opts), pages: (opts?: ListOptions) => this.pages<T>(`/v1/${name}`, opts) });
    this.emails = {
      ...listing<Email>("emails"),
      send: (input: EmailInput, opts?: RequestOptions) => this.request<Email>("POST", "/v1/emails", input, opts),
      batch: (emails: EmailInput[], opts?: RequestOptions) => this.request<BatchResult<Email>>("POST", "/v1/emails/batch", { emails }, opts),
      get: (id: string, opts?: RequestOptions) => this.request<Email>("GET", `/v1/emails/${encodeURIComponent(id)}`, undefined, opts)
    };
    this.templates = {
      ...listing<Template>("templates"),
      create: (input: TemplateInput, opts?: RequestOptions) => this.request<Template>("POST", "/v1/templates", input, opts),
      get: (id: string, opts?: RequestOptions) => this.request<Template>("GET", `/v1/templates/${encodeURIComponent(id)}`, undefined, opts),
      update: (id: string, input: Partial<TemplateInput>, opts?: RequestOptions) => this.request<Template>("PATCH", `/v1/templates/${encodeURIComponent(id)}`, input, opts),
      delete: (id: string, opts?: RequestOptions) => this.request<{ deleted: true; id: string }>("DELETE", `/v1/templates/${encodeURIComponent(id)}`, undefined, opts)
    };
    this.domains = {
      ...listing<Domain>("domains"),
      create: (input: DomainInput, opts?: RequestOptions) => this.request<Domain>("POST", "/v1/domains", input, opts),
      get: (id: string, opts?: RequestOptions) => this.request<Domain>("GET", `/v1/domains/${encodeURIComponent(id)}`, undefined, opts),
      update: (id: string, input: DomainUpdate, opts?: RequestOptions) => this.request<Domain>("PATCH", `/v1/domains/${encodeURIComponent(id)}`, input, opts),
      delete: (id: string, opts?: RequestOptions) => this.request<{ deleted: true; id: string }>("DELETE", `/v1/domains/${encodeURIComponent(id)}`, undefined, opts),
      verify: (id: string, opts?: RequestOptions) => this.action<Domain>("domains", id, "verify", {}, opts)
    };
    this.contacts = {
      ...listing<Contact>("contacts"),
      create: (input: ContactInput, opts?: RequestOptions) => this.request<Contact>("POST", "/v1/contacts", input, opts),
      get: (id: string, opts?: RequestOptions) => this.request<Contact>("GET", `/v1/contacts/${encodeURIComponent(id)}`, undefined, opts),
      update: (id: string, input: Partial<ContactInput>, opts?: RequestOptions) => this.request<Contact>("PATCH", `/v1/contacts/${encodeURIComponent(id)}`, input, opts),
      delete: (id: string, opts?: RequestOptions) => this.request<{ deleted: true; id: string }>("DELETE", `/v1/contacts/${encodeURIComponent(id)}`, undefined, opts),
      import: (input: { rows: ContactInput[] } | { csv: string }, opts?: RequestOptions) => this.request<{ imported: number; data?: Contact[] }>("POST", "/v1/contacts/import", input, opts),
      export: (opts?: RequestOptions) => this.request<string>("GET", "/v1/contacts/export", undefined, opts, "text")
    };
    const campaignAction = (action: string) => (id: string, opts?: RequestOptions) => this.action<Campaign>("campaigns", id, action, {}, opts);
    this.campaigns = {
      ...listing<Campaign>("campaigns"),
      create: (input: CampaignInput, opts?: RequestOptions) => this.request<Campaign>("POST", "/v1/campaigns", input, opts),
      get: (id: string, opts?: RequestOptions) => this.request<Campaign>("GET", `/v1/campaigns/${encodeURIComponent(id)}`, undefined, opts),
      update: (id: string, input: Partial<CampaignInput>, opts?: RequestOptions) => this.request<Campaign>("PATCH", `/v1/campaigns/${encodeURIComponent(id)}`, input, opts),
      delete: (id: string, opts?: RequestOptions) => this.request<{ deleted: true; id: string }>("DELETE", `/v1/campaigns/${encodeURIComponent(id)}`, undefined, opts),
      send: campaignAction("send"),
      schedule: (id: string, scheduledAt: string, opts?: RequestOptions) => this.action<Campaign>("campaigns", id, "schedule", { scheduledAt }, opts),
      pause: campaignAction("pause"), resume: campaignAction("resume"), cancel: campaignAction("cancel"), duplicate: campaignAction("duplicate"),
      test: (id: string, to: string, opts?: RequestOptions) => this.action<Email>("campaigns", id, "test", { to }, opts)
    };
  }

  private action<T>(resource: string, id: string, action: string, input: object = {}, opts?: RequestOptions) {
    return this.request<T>("POST", `/v1/${resource}/${encodeURIComponent(id)}/${action}`, input, opts);
  }

  private async request<T>(method: string, path: string, body?: object, opts: RequestOptions = {}, responseType: "json" | "text" = "json"): Promise<T> {
    const retryable = method === "GET" || Boolean(opts.idempotencyKey);
    for (let attempt = 0; ; attempt++) {
      const timeout = new AbortController();
      const timer = setTimeout(() => timeout.abort(new DOMException("Request timed out", "TimeoutError")), this.timeoutMs);
      const signal = opts.signal ? AbortSignal.any([opts.signal, timeout.signal]) : timeout.signal;
      try {
        const response = await this.fetcher(this.baseUrl + path, {
          method, signal,
          headers: { authorization: `Bearer ${this.options.apiKey}`, "content-type": "application/json", ...(opts.idempotencyKey ? { "idempotency-key": opts.idempotencyKey } : {}) },
          body: body === undefined ? undefined : JSON.stringify(body)
        });
        if (response.ok) return (response.status === 204 ? undefined : responseType === "text" ? await response.text() : await response.json()) as T;
        if (retryable && attempt < this.maxRetries && (response.status === 429 || response.status >= 500)) {
          await sleep(retryDelay(response.headers.get("retry-after"), attempt), opts.signal);
          continue;
        }
        const payload = await response.json().catch(() => ({})) as { error?: { code?: string; message?: string; requestId?: string } };
        throw new SendCloverError(payload.error?.code ?? "api_error", payload.error?.message ?? `HTTP ${response.status}`, response.status, payload.error?.requestId);
      } catch (error) {
        if (retryable && attempt < this.maxRetries && !(error instanceof SendCloverError) && !opts.signal?.aborted) {
          await sleep(retryDelay(null, attempt), opts.signal);
          continue;
        }
        if (error instanceof SendCloverError) throw error;
        if (opts.signal?.aborted) throw new SendCloverError("aborted", "Request aborted", 0);
        if (timeout.signal.aborted) throw new SendCloverError("timeout", `Request timed out after ${this.timeoutMs}ms`, 0);
        throw new SendCloverError("network_error", error instanceof Error ? error.message : "Network request failed", 0);
      } finally { clearTimeout(timer) }
    }
  }

  private list<T>(path: string, opts: ListOptions = {}) {
    const query = new URLSearchParams();
    if (opts.cursor) query.set("cursor", opts.cursor);
    if (opts.limit) query.set("limit", String(opts.limit));
    return this.request<Page<T>>("GET", `${path}${query.size ? `?${query}` : ""}`, undefined, opts);
  }

  private async *pages<T>(path: string, opts: ListOptions = {}): AsyncIterable<T> {
    let cursor = opts.cursor;
    do {
      const page = await this.list<T>(path, { ...opts, cursor });
      yield* page.data;
      cursor = page.nextCursor ?? undefined;
    } while (cursor);
  }
}

function retryDelay(value: string | null, attempt: number) {
  if (value) {
    const seconds = Number(value);
    if (Number.isFinite(seconds)) return Math.max(0, seconds * 1000);
    const date = Date.parse(value);
    if (Number.isFinite(date)) return Math.max(0, date - Date.now());
  }
  return 250 * 2 ** attempt;
}

export async function verifyWebhook(rawBody: string | Uint8Array, headers: Headers | Record<string, string>, secret: string, toleranceSeconds = 300, now = Date.now()): Promise<boolean> {
  const get = (name: string) => headers instanceof Headers ? headers.get(name) : Object.entries(headers).find(([key]) => key.toLowerCase() === name)?.[1] ?? null;
  const id = get("webhook-id"), timestamp = get("webhook-timestamp"), signature = get("webhook-signature");
  if (!id || !timestamp || !signature?.startsWith("v1,") || !secret.startsWith("whsec_")) return false;
  const seconds = Number(timestamp);
  if (!Number.isFinite(seconds) || Math.abs(now / 1000 - seconds) > toleranceSeconds) return false;
  try {
    const key = Uint8Array.from(atob(secret.slice(6)), c => c.charCodeAt(0));
    const body = typeof rawBody === "string" ? new TextEncoder().encode(rawBody) : rawBody;
    const prefix = new TextEncoder().encode(`${id}.${timestamp}.`);
    const message = new Uint8Array(prefix.length + body.length); message.set(prefix); message.set(body, prefix.length);
    const cryptoKey = await crypto.subtle.importKey("raw", key, { name: "HMAC", hash: "SHA-256" }, false, ["sign"]);
    const actual = new Uint8Array(await crypto.subtle.sign("HMAC", cryptoKey, message));
    const expected = Uint8Array.from(atob(signature.slice(3)), c => c.charCodeAt(0));
    let diff = actual.length ^ expected.length;
    for (let i = 0; i < Math.max(actual.length, expected.length); i++) diff |= (actual[i] ?? 0) ^ (expected[i] ?? 0);
    return diff === 0;
  } catch { return false }
}
