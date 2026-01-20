import { Kafka } from "kafkajs";
import Parser from "rss-parser";

const FEEDS = [{ source: "vogue-runway", url: "https://www.voguerunway.com/rss" }];

const kafka = new Kafka({ clientId: "couturae-ingestor", brokers: ["localhost:9092"] });
const producer = kafka.producer();
const parser = new Parser();

async function main() {
  await producer.connect();

  for (const feed of FEEDS) {
    const parsed = await parser.parseURL(feed.url);

    for (const item of parsed.items) {
      const url = item.link ?? "";
      if (!url) continue;

      const payload = {
        source: feed.source,
        title: item.title ?? "",
        url,
        publishedAt: item.isoDate ?? item.pubDate ?? null,
        snippet: item.contentSnippet ?? item.content ?? "",
        fetchedAt: new Date().toISOString(),
      };

      await producer.send({
        topic: "couturae.raw_items",
        messages: [{ key: url, value: JSON.stringify(payload) }],
      });
    }
  }

  console.log("Got RSS → couturae.raw_items");
  await producer.disconnect();
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
