import { SendClover } from "../javascript/dist/index.js";
const baseUrl = process.env.SENDCLOVER_BASE_URL ?? "http://localhost:3000";
const client = new SendClover({ apiKey: process.env.SENDCLOVER_API_KEY!, baseUrl });
console.log(await client.emails.send({ from: "sender@example.com", to: "recipient@example.net", subject: "Hello", text: "Sent with SendClover" }, { idempotencyKey: crypto.randomUUID() }));
