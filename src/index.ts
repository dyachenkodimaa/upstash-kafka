import { Kafka } from "@upstash/kafka";
import { Buffer } from 'node:buffer';

export interface Env {
	UPSTASH_KAFKA_REST_URL: string;
	UPSTASH_KAFKA_REST_USERNAME: string;
	UPSTASH_KAFKA_REST_PASSWORD: string;
}

export default {
	async fetch(
		request: Request,
		env: Env,
		ctx: ExecutionContext
	): Promise<Response> {
		const buf = Buffer.from(`${Date.now()}`, 'utf8');

		const key =buf.toString('hex');

		let message = {
			country: request.cf?.country,
			city: request.cf?.city,
			region: request.cf?.region,
			url: request.url,
			ip: request.headers.get("x-real-ip"),
			mobile: request.headers.get("sec-ch-ua-mobile"),
			platform: request.headers.get("sec-ch-ua-platform"),
			useragent: request.headers.get("user-agent"),
		};

		const kafka = new Kafka({
			url: env.UPSTASH_KAFKA_REST_URL,
			username: env.UPSTASH_KAFKA_REST_USERNAME,
			password: env.UPSTASH_KAFKA_REST_PASSWORD,
		});

		const p = kafka.producer();

		const topic = "external.landing.command.push-log.1";

		ctx.waitUntil(p.produce(topic, JSON.stringify(message), {key: key}));

		return new Response(null);
	},
};
