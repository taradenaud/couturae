import { Kafka } from "kafkajs";
import Parser from "rss-parser";

const FEEDS = [
  { source: "wwd", url: "https://wwd.com/feed/" },
  { source: "fashionista", url: "https://fashionista.com/feed" },
  { source: "fashion-week-online", url: "https://fashionweekonline.com/feed" },
  { source: "vogue", url: "https://www.vogue.com/feed/rss" },
  { source: "elle", url: "https://www.elle.com/rss/fashion.xml" },
  { source: "harpers-bazaar", url: "https://www.harpersbazaar.com/rss/fashion.xml" },
  { source: "business-of-fashion", url: "https://www.businessoffashion.com/feed" },
  { source: "highsnobiety", url: "https://www.highsnobiety.com/feed/" },
  { source: "hypebeast", url: "https://hypebeast.com/feed" },
  { source: "dazed", url: "https://www.dazeddigital.com/rss" },
  { source: "i-d", url: "https://i-d.co/feed/" },
  { source: "refinery29-fashion", url: "https://www.refinery29.com/fashion/rss.xml" },
  { source: "purseblog", url: "https://www.purseblog.com/feed/" },
  { source: "fashion-gone-rogue", url: "https://www.fashiongonerogue.com/feed/" },
  { source: "fashion-bomb-daily", url: "https://fashionbombdaily.com/feed/" },
  { source: "glamour", url: "https://www.glamour.com/feed/rss" },
  { source: "marie-claire", url: "https://www.marieclaire.com/rss/fashion.xml" },
  { source: "cosmopolitan", url: "https://www.cosmopolitan.com/rss/style.xml" },
  { source: "gq", url: "https://www.gq.com/feed/rss" },
  { source: "esquire", url: "https://www.esquire.com/rss/style.xml" },
  { source: "coveteur", url: "https://coveteur.com/feed" },
  { source: "whowhatwear", url: "https://www.whowhatwear.com/rss" },
  { source: "popsugar-fashion", url: "https://www.popsugar.com/fashion/feed" },
  { source: "luxuo", url: "https://www.luxuo.com/feed" },

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
