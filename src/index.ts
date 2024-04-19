import { Kafka } from "@upstash/kafka";
import { Buffer } from 'node:buffer';

export interface Env {
	UPSTASH_KAFKA_REST_URL: string;
	UPSTASH_KAFKA_REST_USERNAME: string;
	UPSTASH_KAFKA_REST_PASSWORD: string;
}

export const CORS_HEADERS: HeadersInit = {
	"Access-Control-Allow-Origin": "*",
	"Access-Control-Allow-Methods": "GET,HEAD,POST,OPTIONS",
	"Access-Control-Max-Age": "86400",
};

export default {
	async fetch(
		request: Request,
		env: Env,
		ctx: ExecutionContext
	): Promise<Response> {
		const log = await request.json() as object
		const buf = Buffer.from(`${Date.now()}`, 'utf8');
		const key =buf.toString('hex');

		let message = {
			country: request.cf?.country,
			city: request.cf?.city,
			url: request.url,
			ip: request.headers.get("x-real-ip"),
			...log
		};

		const kafka = new Kafka({
			url: env.UPSTASH_KAFKA_REST_URL,
			username: env.UPSTASH_KAFKA_REST_USERNAME,
			password: env.UPSTASH_KAFKA_REST_PASSWORD,
		});

		const p = kafka.producer();

		const topic = "external.landing.command.push-log.1";

		ctx.waitUntil(p.produce(topic, JSON.stringify(message), {key: key}));

		const headers = getResponseHeaders(request)
		return new Response(null, {headers: {
			...headers
			}});
	},
};

export function getResponseHeaders(request: Request): HeadersInit {
	const accessControlValue = request.headers.get(
		"Access-Control-Request-Headers"
	) as string;
	return {
		...CORS_HEADERS,
		"Access-Control-Allow-Headers": accessControlValue,
	};
}
