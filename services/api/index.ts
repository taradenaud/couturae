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

// GET /api/buzzwords/trending - Top trending buzzwords
app.get("/api/buzzwords/trending", async (req, res) => {
  try {
    const limit = req.query.limit ? parseInt(req.query.limit as string) : 20;
    const result = await pgClient.query(
      `SELECT word, COUNT(*) as frequency, 
              COUNT(DISTINCT city) as cities,
              COUNT(DISTINCT season) as seasons
       FROM buzzwords 
       GROUP BY word 
       ORDER BY frequency DESC 
       LIMIT $1`,
      [limit]
    );
    res.json(result.rows);
  } catch (err) {
    res.status(500).json({ error: "Database query failed" });
  }
});

// GET /api/buzzwords/by-season/:season - Buzzwords for specific season
app.get("/api/buzzwords/by-season/:season", async (req, res) => {
  try {
    const { season } = req.params;
    const result = await pgClient.query(
      `SELECT word, COUNT(*) as frequency, city
       FROM buzzwords 
       WHERE season = $1
       GROUP BY word, city
       ORDER BY frequency DESC`,
      [season.toUpperCase()]
    );
    res.json(result.rows);
  } catch (err) {
    res.status(500).json({ error: "Database query failed" });
  }
});

// GET /api/designers - All designers with show counts
app.get("/api/designers", async (req, res) => {
  try {
    const result = await pgClient.query(
      `SELECT designer_name, 
              COUNT(*) as total_shows,
              COUNT(DISTINCT city) as cities,
              COUNT(DISTINCT season) as seasons,
              ARRAY_AGG(DISTINCT city) as cities_list,
              ARRAY_AGG(DISTINCT season) as seasons_list
       FROM designers 
       GROUP BY designer_name 
       ORDER BY total_shows DESC`
    );
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

// GET /api/seasons/:season/stats - Stats for a specific season
app.get("/api/seasons/:season/stats", async (req, res) => {
  try {
    const { season } = req.params;
    
    const items = await pgClient.query(
      `SELECT COUNT(*) as total_articles FROM items WHERE season = $1`,
      [season.toUpperCase()]
    );
    
    const buzzwords = await pgClient.query(
      `SELECT COUNT(DISTINCT word) as unique_buzzwords FROM buzzwords WHERE season = $1`,
      [season.toUpperCase()]
    );
    
    const designers = await pgClient.query(
      `SELECT COUNT(DISTINCT designer_name) as unique_designers FROM designers WHERE season = $1`,
      [season.toUpperCase()]
    );
    
    const cities = await pgClient.query(
      `SELECT city, COUNT(*) as article_count 
       FROM items 
       WHERE season = $1 AND city IS NOT NULL
       GROUP BY city
       ORDER BY article_count DESC`,
      [season.toUpperCase()]
    );
    
    res.json({
      season: season.toUpperCase(),
      total_articles: parseInt(items.rows[0].total_articles),
      unique_buzzwords: parseInt(buzzwords.rows[0].unique_buzzwords),
      unique_designers: parseInt(designers.rows[0].unique_designers),
      by_city: cities.rows
    });
  } catch (err) {
    res.status(500).json({ error: "Database query failed" });
  }
});

// GET /api/cities/:city/trends - Trends for a specific city
app.get("/api/cities/:city/trends", async (req, res) => {
  try {
    const { city } = req.params;
    
    const topBuzzwords = await pgClient.query(
      `SELECT word, COUNT(*) as frequency
       FROM buzzwords
       WHERE city ILIKE $1
       GROUP BY word
       ORDER BY frequency DESC
       LIMIT 10`,
      [city]
    );
    
    const topDesigners = await pgClient.query(
      `SELECT designer_name, COUNT(*) as shows
       FROM designers
       WHERE city ILIKE $1
       GROUP BY designer_name
       ORDER BY shows DESC
       LIMIT 10`,
      [city]
    );
    
    const seasons = await pgClient.query(
      `SELECT season, COUNT(*) as article_count
       FROM items
       WHERE city ILIKE $1 AND season IS NOT NULL
       GROUP BY season
       ORDER BY season DESC`,
      [city]
    );
    
    res.json({
      city,
      top_buzzwords: topBuzzwords.rows,
      top_designers: topDesigners.rows,
      seasons: seasons.rows
    });
  } catch (err) {
    res.status(500).json({ error: "Database query failed" });
  }
});

// GET /api/stats - Overall platform stats
app.get("/api/stats", async (req, res) => {
  try {
    const totalItems = await pgClient.query(`SELECT COUNT(*) FROM items`);
    const totalBuzzwords = await pgClient.query(`SELECT COUNT(*) FROM buzzwords`);
    const totalDesigners = await pgClient.query(`SELECT COUNT(*) FROM designers`);
    const uniqueBuzzwords = await pgClient.query(`SELECT COUNT(DISTINCT word) FROM buzzwords`);
    const uniqueDesigners = await pgClient.query(`SELECT COUNT(DISTINCT designer_name) FROM designers`);
    
    res.json({
      total_articles: parseInt(totalItems.rows[0].count),
      total_buzzword_mentions: parseInt(totalBuzzwords.rows[0].count),
      total_designer_mentions: parseInt(totalDesigners.rows[0].count),
      unique_buzzwords: parseInt(uniqueBuzzwords.rows[0].count),
      unique_designers: parseInt(uniqueDesigners.rows[0].count)
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
      trending_buzzwords: "GET /api/buzzwords/trending?limit=20",
      season_buzzwords: "GET /api/buzzwords/by-season/:season",
      all_designers: "GET /api/designers",
      designer_shows: "GET /api/designers/:name/shows",
      season_stats: "GET /api/seasons/:season/stats",
      city_trends: "GET /api/cities/:city/trends"
    }
  });
});

app.listen(port, () => {
  console.log(`Couturae API running on http://localhost:${port}`);
  console.log(`Click: http://localhost:${port}/api/stats`);
});
