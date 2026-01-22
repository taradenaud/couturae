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
  "bohemian", "grunge", "preppy", "androgynous", "feminine", "masculine", "sporty",
  "romantic", "edgy", "vintage", "retro", "modern", "futuristic", "classic", "timeless",
  // Details & Features
  "pleated", "ruched", "draped", "gathered", "embroidered", "beaded", "fringed", "ruffled",
  "cutout", "sheer", "layered", "asymmetric", "cropped", "high-waisted", "low-rise",
  "sleeveless", "off-shoulder", "halter", "strapless", "backless", "plunging",
  // Colors & Patterns
  "monochrome", "neon", "pastel", "metallic", "bold", "muted", "vibrant",
  "striped", "floral", "geometric", "animal-print", "leopard", "zebra", "snakeskin",
  "polka-dot", "checkered", "plaid", "houndstooth", "paisley", "tie-dye", "ombre",
  // Specific Colors
  "burgundy", "emerald", "cobalt", "magenta", "fuchsia", "coral", "teal", "navy",
  "ivory", "champagne", "blush", "mauve", "lavender", "sage", "olive", "rust",
  "mustard", "terracotta", "ochre", "crimson", "scarlet", "vermillion", "tangerine",
  "cerulean", "indigo", "violet", "plum", "aubergine", "chartreuse", "mint",
  "rose-gold", "copper", "bronze", "pewter", "gunmetal", "silver", "gold",
  "nude", "camel", "taupe", "ecru", "bone", "cream", "off-white", "charcoal",
  // Common Colors
  "red", "blue", "green", "black", "white", "pink", "purple", "orange", "yellow",
  "brown", "grey", "gray", "beige", "turquoise", "maroon", "aqua", "peach",
  "lime", "cyan", "khaki", "tan", "salmon", "lilac", "periwinkle",
  // Trends & Aesthetics
  "y2k", "90s", "80s", "70s", "60s", "cottagecore", "normcore", "gorpcore", "dopamine",
  "quiet-luxury", "clean-girl", "coastal-grandmother", "dark-academia", "light-academia"
]);

// Generic garment words - only count if accompanied by fashion keywords
const GARMENT_WORDS = new Set([
  "blazer", "trench", "coat", "jacket", "dress", "skirt", "pants", "trousers", "blouse",
  "shirt", "sweater", "cardigan", "jumpsuit", "romper", "gown", "cape", "poncho",
  "top", "jeans", "shorts", "suit", "vest", "hoodie", "tee", "tank"
]);

function extractBuzzwords(text: string): string[] {
  const buzzwords: string[] = [];
  const lowerText = text.toLowerCase();
  
  // Extract fashion keywords (these are always valid on their own)
  // BUT exclude garment words - they should never appear alone
  for (const buzzword of FASHION_KEYWORDS) {
    // Skip if this keyword is also a garment word (shouldn't happen but safety check)
    if (GARMENT_WORDS.has(buzzword)) continue;
    
    // Use word boundary to avoid partial matches
    const wordPattern = new RegExp(`\\b${buzzword}\\b`, 'i');
    if (wordPattern.test(lowerText)) {
      buzzwords.push(buzzword);
    }
  }
  
  // Only include garment words when DIRECTLY paired with a modifier keyword
  // Look for patterns like "silk dress", "leather jacket", "velvet coat"
  for (const garment of GARMENT_WORDS) {
    for (const keyword of FASHION_KEYWORDS) {
      // Check for "keyword garment" pattern (e.g., "silk dress", "leather jacket")
      const pattern1 = new RegExp(`\\b${keyword}\\s+${garment}\\b`, 'i');
      // Check for "keyword-garment" hyphenated pattern (e.g., "leather-jacket")
      const pattern2 = new RegExp(`\\b${keyword}-${garment}\\b`, 'i');
      
      if (pattern1.test(lowerText) || pattern2.test(lowerText)) {
        buzzwords.push(`${keyword} ${garment}`);
        break; // Found a pairing for this garment, move on
      }
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

function inferSeason(text: string, publishedAt?: string | null): string | null {
  const t = text.toLowerCase();
  
  // Match FW24, SS25, AW24 format (any year)
  const shortMatch = t.match(/\b(fw|ss|aw)\s?(\d{2})\b/i);
  if (shortMatch && shortMatch[1] && shortMatch[2]) {
    const type = shortMatch[1].toUpperCase() === 'AW' ? 'FW' : shortMatch[1].toUpperCase();
    return `${type}${shortMatch[2]}`;
  }
  
  // Match "Fall/Winter 2024", "Fall Winter 2024", "Fall 2024", "Autumn/Winter 2024"
  const fallMatch = t.match(/\b(fall|autumn)[\s\/]*(winter)?\s*(20)?(\d{2})\b/i);
  if (fallMatch && fallMatch[4]) {
    return `FW${fallMatch[4]}`;
  }
  
  // Match "Spring/Summer 2025", "Spring Summer 2025", "Spring 2025"
  const springMatch = t.match(/\b(spring)[\s\/]*(summer)?\s*(20)?(\d{2})\b/i);
  if (springMatch && springMatch[4]) {
    return `SS${springMatch[4]}`;
  }
  
  // Match "Summer 2025"
  const summerMatch = t.match(/\bsummer\s*(20)?(\d{2})\b/i);
  if (summerMatch && summerMatch[2]) {
    return `SS${summerMatch[2]}`;
  }
  
  // Match "Winter 2024"
  const winterMatch = t.match(/\bwinter\s*(20)?(\d{2})\b/i);
  if (winterMatch && winterMatch[2]) {
    return `FW${winterMatch[2]}`;
  }
  
  // Match "Resort 2025", "Cruise 2025" (pre-spring collections)
  const resortMatch = t.match(/\b(resort|cruise)\s*(20)?(\d{2})\b/i);
  if (resortMatch && resortMatch[3]) {
    return `RS${resortMatch[3]}`;
  }
  
  // Match "Pre-Fall 2024"
  const preFallMatch = t.match(/\bpre-?fall\s*(20)?(\d{2})\b/i);
  if (preFallMatch && preFallMatch[2]) {
    return `PF${preFallMatch[2]}`;
  }
  
  // Match "Haute Couture Fall 2024" or "Couture Spring 2025"
  const coutureMatch = t.match(/\bcouture\s*(fall|spring|winter|summer)?\s*(20)?(\d{2})\b/i);
  if (coutureMatch && coutureMatch[3]) {
    const season = coutureMatch[1]?.toLowerCase();
    if (season === 'fall' || season === 'winter') return `HC-FW${coutureMatch[3]}`;
    if (season === 'spring' || season === 'summer') return `HC-SS${coutureMatch[3]}`;
    return `HC${coutureMatch[3]}`;
  }
  
  // Match "Menswear Fall 2024" or "Men's Spring 2025"
  const menswearMatch = t.match(/\b(men'?s?wear?|men'?s)\s*(fall|spring|winter|summer|fw|ss)[\s\/]*(winter|summer)?\s*(20)?(\d{2})\b/i);
  if (menswearMatch && menswearMatch[5]) {
    const seasonType = menswearMatch[2]?.toLowerCase();
    if (seasonType === 'fall' || seasonType === 'winter' || seasonType === 'fw') return `M-FW${menswearMatch[5]}`;
    return `M-SS${menswearMatch[5]}`;
  }
  
  // If fashion week is mentioned with just a year, infer from publication month
  const fashionWeekYear = t.match(/\bfashion\s*week\s*(20)?(\d{2})\b/i);
  if (fashionWeekYear && fashionWeekYear[2]) {
    return `FW${fashionWeekYear[2]}`; // Default to FW if just year mentioned
  }
  
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
      const season = inferSeason(combined, item.publishedAt);
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
