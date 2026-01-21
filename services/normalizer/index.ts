import { Kafka } from "kafkajs";
import crypto from "crypto";
import { Client } from "pg";

type RawItem = {
  source: string;
  title: string;
  url: string;
  publishedAt: string | null;
  snippet: string;
  fetchedAt: string;
};

const kafka = new Kafka({ clientId: "couturae-normalizer", brokers: ["localhost:9092"] });
const consumer = kafka.consumer({ groupId: "couturae-normalizer-group" });

const pgClient = new Client({
  host: "localhost",
  port: 5432,
  database: "couturae",
  user: "couturae_user",
  // no password needed 
});

const sha256 = (s: string) => crypto.createHash("sha256").update(s).digest("hex");

// Fashion-specific keywords to capture
const FASHION_KEYWORDS = new Set([
  // Fabrics & Materials
  "silk", "satin", "velvet", "lace", "leather", "suede", "denim", "cotton", "wool", "cashmere",
  "tweed", "chiffon", "organza", "tulle", "sequins", "mesh", "latex", "vinyl", "patent",
  "corduroy", "linen", "jersey", "knit", "crochet", "fur", "shearling", "nylon", "polyester",
  // Styles & Silhouettes
  "oversized", "tailored", "structured", "unstructured", "minimalist", "maximalist", "avant-garde",
  "bohemian", "grunge", "preppy", "androgynous", "feminine", "masculine", "neutral", "sporty",
  "romantic", "edgy", "vintage", "retro", "modern", "futuristic", "classic", "timeless",
  // Garments
  "blazer", "trench", "coat", "jacket", "dress", "skirt", "pants", "trousers", "blouse",
  "shirt", "sweater", "cardigan", "jumpsuit", "romper", "gown", "cape", "poncho",
  // Details & Features
  "pleated", "ruched", "draped", "gathered", "embroidered", "beaded", "fringed", "ruffled",
  "cutout", "sheer", "layered", "asymmetric", "cropped", "high-waisted", "low-rise",
  "sleeveless", "off-shoulder", "halter", "strapless", "backless", "plunging",
  // Colors & Patterns
  "monochrome", "neon", "pastel", "metallic", "neutral", "bold", "muted", "vibrant",
  "striped", "floral", "geometric", "animal-print", "leopard", "zebra", "snakeskin",
  "polka-dot", "checkered", "plaid", "houndstooth", "paisley", "tie-dye", "ombre",
  // Trends & Aesthetics
  "y2k", "90s", "80s", "70s", "60s", "cottagecore", "normcore", "gorpcore", "dopamine",
  "quiet-luxury", "clean-girl", "coastal-grandmother", "dark-academia", "light-academia"
]);

function extractBuzzwords(text: string): string[] {
  const buzzwords: string[] = [];
  const lowerText = text.toLowerCase();
  
  // Only extract exact fashion keywords - no bigrams, no synonyms, no adjectives
  for (const buzzword of FASHION_KEYWORDS) {
    if (lowerText.includes(buzzword)) {
      buzzwords.push(buzzword);
    }
  }

  return [...new Set(buzzwords)]; // Remove duplicates
}

function inferCity(text: string): string | null {
  const t = text.toLowerCase();
  if (t.includes("paris")) return "Paris";
  if (t.includes("milan")) return "Milan";
  if (t.includes("london")) return "London";
  if (t.includes("new york") || t.includes("nyfw")) return "New York";
  if (t.includes("berlin")) return "Berlin";
  return null;
}

function inferSeason(text: string): string | null {
  const t = text.toLowerCase();
  const m = t.match(/\b(fw|ss)\s?(\d{2})\b/i);
  if (m && m[1] && m[2]) return `${m[1].toUpperCase()}${m[2]}`;
  return null;
}

// List of major fashion designers/houses (expanded from Fashion Calendar schedules)
const DESIGNERS = [
  // Major fashion houses
  "Chanel", "Dior", "Gucci", "Prada", "Louis Vuitton", "Versace", "Valentino",
  "Balenciaga", "Saint Laurent", "Givenchy", "Hermès", "Fendi", "Celine",
  "Burberry", "Alexander McQueen", "Stella McCartney", "Marc Jacobs",
  "Dolce & Gabbana", "Bottega Veneta", "Loewe", "Balmain", "Jacquemus", 
  "Off-White", "Vetements", "Rick Owens", "Comme des Garçons", "Issey Miyake", 
  "Yohji Yamamoto",
  // From NYFW Official Schedule (fashioncalendar.com)
  "Coach", "Tory Burch", "Michael Kors", "Carolina Herrera", "Calvin Klein",
  "Ralph Lauren", "Tom Ford", "Tadashi Shoji", "Pamella Roland", "Altuzarra",
  "Proenza Schouler", "Derek Lam", "Eckhaus Latta", "Collina Strada",
  "Christian Cowan", "PatBo", "LoveShackFancy", "Theory", "Norma Kamali",
  "Markarian", "Zankov"
];

function extractDesigners(text: string): string[] {
  const found: string[] = [];
  const lowerText = text.toLowerCase();
  
  for (const designer of DESIGNERS) {
    if (lowerText.includes(designer.toLowerCase())) {
      found.push(designer);
    }
  }
  
  return [...new Set(found)]; // Remove duplicates
}

async function main() {
  await pgClient.connect();
  await consumer.connect();
  await consumer.subscribe({ topic: "couturae.raw_items", fromBeginning: true });

  console.log("Normalizer running: couturae.raw_items → Postgres(items)");

  await consumer.run({
    eachMessage: async ({ message }) => {
      if (!message.value) return;

      let item: RawItem;
      try {
        item = JSON.parse(message.value.toString());
      } catch {
        return;
      }

      if (!item.url) return;

      const id = sha256(item.url);
      const combined = `${item.title} ${item.snippet}`;
      const city = inferCity(combined);
      const season = inferSeason(combined);
      const buzzwords = extractBuzzwords(combined);
      const designers = extractDesigners(combined);

      await pgClient.query(
        `INSERT INTO items (id, source, title, url, published_at, snippet, city, season)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
         ON CONFLICT (url) DO NOTHING`,
        [
          id,
          item.source,
          item.title,
          item.url,
          item.publishedAt ? new Date(item.publishedAt) : null,
          item.snippet?.slice(0, 2000) ?? null,
          city,
          season,
        ]
      );

      // Store designers associated with this show/article
      for (const designer of designers) {
        await pgClient.query(
          `INSERT INTO designers (designer_name, item_id, city, season, detected_at)
           VALUES ($1, $2, $3, $4, NOW())
           ON CONFLICT (designer_name, item_id) DO NOTHING`,
          [designer, id, city, season]
        );
      }

      // Store buzzwords with frequency tracking
      for (const word of buzzwords) {
        await pgClient.query(
          `INSERT INTO buzzwords (word, item_id, city, season, detected_at)
           VALUES ($1, $2, $3, $4, NOW())
           ON CONFLICT (word, item_id) DO NOTHING`,
          [word, id, city, season]
        );
      }
    },
  });
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
