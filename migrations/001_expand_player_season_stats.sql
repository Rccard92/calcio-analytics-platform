-- Migrazione: espansione tabella player_season_stats
-- Aggiunge tutte le metriche disponibili da API-Football v3
-- Eseguire su PostgreSQL. Idempotente: usa IF NOT EXISTS / ADD COLUMN IF NOT EXISTS.
-- Data: 2026-02-16

-- =============================================
-- Rinomina colonne legacy per allineamento API
-- =============================================

-- "shots" → "shots_total" (allineamento nome API)
ALTER TABLE player_season_stats RENAME COLUMN shots TO shots_total;

-- "shots_on_target" → "shots_on" (allineamento nome API)
ALTER TABLE player_season_stats RENAME COLUMN shots_on_target TO shots_on;

-- =============================================
-- GAMES: nuovi campi
-- =============================================
ALTER TABLE player_season_stats ADD COLUMN IF NOT EXISTS lineups INTEGER;
ALTER TABLE player_season_stats ADD COLUMN IF NOT EXISTS captain BOOLEAN;

-- =============================================
-- GOALS: nuovi campi
-- =============================================
ALTER TABLE player_season_stats ADD COLUMN IF NOT EXISTS goals_conceded INTEGER;
ALTER TABLE player_season_stats ADD COLUMN IF NOT EXISTS saves INTEGER;

-- =============================================
-- PASSES: nuovi campi
-- =============================================
ALTER TABLE player_season_stats ADD COLUMN IF NOT EXISTS passes_total INTEGER;
ALTER TABLE player_season_stats ADD COLUMN IF NOT EXISTS key_passes INTEGER;

-- =============================================
-- TACKLES
-- =============================================
ALTER TABLE player_season_stats ADD COLUMN IF NOT EXISTS tackles_total INTEGER;
ALTER TABLE player_season_stats ADD COLUMN IF NOT EXISTS blocks INTEGER;
ALTER TABLE player_season_stats ADD COLUMN IF NOT EXISTS interceptions INTEGER;

-- =============================================
-- DUELS
-- =============================================
ALTER TABLE player_season_stats ADD COLUMN IF NOT EXISTS duels_total INTEGER;
ALTER TABLE player_season_stats ADD COLUMN IF NOT EXISTS duels_won INTEGER;

-- =============================================
-- DRIBBLES
-- =============================================
ALTER TABLE player_season_stats ADD COLUMN IF NOT EXISTS dribbles_attempts INTEGER;
ALTER TABLE player_season_stats ADD COLUMN IF NOT EXISTS dribbles_success INTEGER;
ALTER TABLE player_season_stats ADD COLUMN IF NOT EXISTS dribbled_past INTEGER;

-- =============================================
-- FOULS
-- =============================================
ALTER TABLE player_season_stats ADD COLUMN IF NOT EXISTS fouls_drawn INTEGER;
ALTER TABLE player_season_stats ADD COLUMN IF NOT EXISTS fouls_committed INTEGER;

-- =============================================
-- CARDS
-- =============================================
ALTER TABLE player_season_stats ADD COLUMN IF NOT EXISTS yellow_cards INTEGER;
ALTER TABLE player_season_stats ADD COLUMN IF NOT EXISTS red_cards INTEGER;

-- =============================================
-- PENALTY
-- =============================================
ALTER TABLE player_season_stats ADD COLUMN IF NOT EXISTS penalty_won INTEGER;
ALTER TABLE player_season_stats ADD COLUMN IF NOT EXISTS penalty_committed INTEGER;
ALTER TABLE player_season_stats ADD COLUMN IF NOT EXISTS penalty_scored INTEGER;
ALTER TABLE player_season_stats ADD COLUMN IF NOT EXISTS penalty_missed INTEGER;
ALTER TABLE player_season_stats ADD COLUMN IF NOT EXISTS penalty_saved INTEGER;

-- =============================================
-- INDICE COMPOSITO per query per squadra+stagione
-- =============================================
CREATE INDEX IF NOT EXISTS ix_player_season_stats_team_season
    ON player_season_stats (team_id, season);
