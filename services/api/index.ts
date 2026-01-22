import express from "express";
import cors from "cors";
import { Client } from "pg";

const app = express();
const port = 3000;

app.use(cors());
app.use(express.json());

const pgClient = new Client({
  host: "localhost",
  port: 5432,
  database: "couturae",
  user: "couturae_user",
});

await pgClient.connect();

// GET /api/seasons - List all available seasons
app.get("/api/seasons", async (req, res) => {
  try {
    const result = await pgClient.query(
      `SELECT season, COUNT(*) as article_count
       FROM items
       WHERE season IS NOT NULL
       GROUP BY season
       ORDER BY 
         SUBSTRING(season FROM '[0-9]+') DESC,
         CASE 
           WHEN season LIKE 'HC-%' THEN 1
           WHEN season LIKE 'M-%' THEN 2
           WHEN season LIKE 'FW%' THEN 3
           WHEN season LIKE 'PF%' THEN 4
           WHEN season LIKE 'RS%' THEN 5
           WHEN season LIKE 'SS%' THEN 6
           ELSE 7
         END`
    );
    res.json(result.rows);
  } catch (err) {
    console.error("Seasons query error:", err);
    res.status(500).json({ error: "Database query failed" });
  }
});

// GET /api/buzzwords/trending - Top trending buzzwords
app.get("/api/buzzwords/trending", async (req, res) => {
  try {
    const limit = req.query.limit ? parseInt(req.query.limit as string) : 20;
    const season = req.query.season as string | undefined;
    
    let query: string;
    let params: (string | number)[];
    
    if (season && season !== 'all') {
      query = `SELECT word, COUNT(*) as frequency, 
              COUNT(DISTINCT city) as cities,
              COUNT(DISTINCT season) as seasons
       FROM buzzwords 
       WHERE season = $1
       GROUP BY word 
       ORDER BY frequency DESC 
       LIMIT $2`;
      params = [season.toUpperCase(), limit];
    } else {
      query = `SELECT word, COUNT(*) as frequency, 
              COUNT(DISTINCT city) as cities,
              COUNT(DISTINCT season) as seasons
       FROM buzzwords 
       GROUP BY word 
       ORDER BY frequency DESC 
       LIMIT $1`;
      params = [limit];
    }
    
    const result = await pgClient.query(query, params);
    res.json(result.rows);
  } catch (err) {
    res.status(500).json({ error: "Database query failed" });
  }
});

// GET /api/designers - All designers with show counts
app.get("/api/designers", async (req, res) => {
  try {
    const season = req.query.season as string | undefined;
    
    let query: string;
    let params: string[] = [];
    
    if (season && season !== 'all') {
      query = `SELECT designer_name, 
              COUNT(*) as total_shows,
              COUNT(DISTINCT city) as cities,
              COUNT(DISTINCT season) as seasons,
              ARRAY_AGG(DISTINCT city) as cities_list,
              ARRAY_AGG(DISTINCT season) as seasons_list
       FROM designers 
       WHERE season = $1
       GROUP BY designer_name 
       ORDER BY total_shows DESC`;
      params = [season.toUpperCase()];
    } else {
      query = `SELECT designer_name, 
              COUNT(*) as total_shows,
              COUNT(DISTINCT city) as cities,
              COUNT(DISTINCT season) as seasons,
              ARRAY_AGG(DISTINCT city) as cities_list,
              ARRAY_AGG(DISTINCT season) as seasons_list
       FROM designers 
       GROUP BY designer_name 
       ORDER BY total_shows DESC`;
    }
    
    const result = await pgClient.query(query, params);
    res.json(result.rows);
  } catch (err) {
    res.status(500).json({ error: "Database query failed" });
  }
});

// GET /api/designers/:name/shows - Shows for specific designer
app.get("/api/designers/:name/shows", async (req, res) => {
  try {
    const { name } = req.params;
    const result = await pgClient.query(
      `SELECT i.title, i.url, i.published_at, d.city, d.season, i.snippet
       FROM designers d
       JOIN items i ON d.item_id = i.id
       WHERE d.designer_name ILIKE $1
       ORDER BY i.published_at DESC`,
      [name]
    );
    res.json(result.rows);
  } catch (err) {
    res.status(500).json({ error: "Database query failed" });
  }
});

// GET /api/stats - Overall platform stats
app.get("/api/stats", async (req, res) => {
  try {
    const season = req.query.season as string | undefined;
    const seasonFilter = season && season !== 'all' ? season.toUpperCase() : null;
    
    let totalItems, totalBuzzwords, totalDesigners, uniqueBuzzwords, uniqueDesigners;
    
    if (seasonFilter) {
      totalItems = await pgClient.query(`SELECT COUNT(*) FROM items WHERE season = $1`, [seasonFilter]);
      totalBuzzwords = await pgClient.query(`SELECT COUNT(*) FROM buzzwords WHERE season = $1`, [seasonFilter]);
      totalDesigners = await pgClient.query(`SELECT COUNT(*) FROM designers WHERE season = $1`, [seasonFilter]);
      uniqueBuzzwords = await pgClient.query(`SELECT COUNT(DISTINCT word) FROM buzzwords WHERE season = $1`, [seasonFilter]);
      uniqueDesigners = await pgClient.query(`SELECT COUNT(DISTINCT designer_name) FROM designers WHERE season = $1`, [seasonFilter]);
    } else {
      totalItems = await pgClient.query(`SELECT COUNT(*) FROM items`);
      totalBuzzwords = await pgClient.query(`SELECT COUNT(*) FROM buzzwords`);
      totalDesigners = await pgClient.query(`SELECT COUNT(*) FROM designers`);
      uniqueBuzzwords = await pgClient.query(`SELECT COUNT(DISTINCT word) FROM buzzwords`);
      uniqueDesigners = await pgClient.query(`SELECT COUNT(DISTINCT designer_name) FROM designers`);
    }
    
    res.json({
      total_articles: parseInt(totalItems.rows[0].count),
      total_buzzword_mentions: parseInt(totalBuzzwords.rows[0].count),
      total_designer_mentions: parseInt(totalDesigners.rows[0].count),
      unique_buzzwords: parseInt(uniqueBuzzwords.rows[0].count),
      unique_designers: parseInt(uniqueDesigners.rows[0].count),
      season: seasonFilter || 'all'
    });
  } catch (err) {
    res.status(500).json({ error: "Database query failed" });
  }
});

// GET / - API info
app.get("/", (req, res) => {
  res.json({
    name: "Couturae Fashion Intelligence API",
    version: "1.0.0",
    endpoints: {
      stats: "GET /api/stats",
      seasons: "GET /api/seasons",
      trending_buzzwords: "GET /api/buzzwords/trending?limit=20",
      all_designers: "GET /api/designers",
      designer_shows: "GET /api/designers/:name/shows"
    }
  });
});

app.listen(port, () => {
  console.log(`Couturae API running on http://localhost:${port}`);
  console.log(`Click: http://localhost:${port}/api/stats`);
});
