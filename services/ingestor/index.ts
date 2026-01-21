import { Kafka } from "kafkajs";
import Parser from "rss-parser";

const FEEDS = [
  { source: "wwd", url: "https://wwd.com/feed/" },
  { source: "fashionista", url: "https://fashionista.com/feed" },
  { source: "fashion-week-online", url: "https://fashionweekonline.com/feed" },
];

const kafka = new Kafka({ clientId: "couturae-ingestor", brokers: ["localhost:9092"] });
const producer = kafka.producer();
const parser = new Parser();

async function main() {
  await producer.connect();
  console.log("Ingestor connected to Kafka");

  let totalItems = 0;

  for (const feed of FEEDS) {
    try {
      console.log(`Fetching ${feed.source}...`);
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
        totalItems++;
      }

      console.log(`✓ ${feed.source}: ${parsed.items.length} items`);
    } catch (error) {
      console.error(`✗ Failed to fetch ${feed.source}:`, error instanceof Error ? error.message : error);
      // Continue with other feeds even if one fails
    }
  }

  console.log(`\nTotal: ${totalItems} items → couturae.raw_items`);
  await producer.disconnect();
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
