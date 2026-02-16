-- =========================================================================
-- Migrazione: espansione tabella player_season_stats
-- Aggiunge tutte le metriche disponibili da API-Football v3
-- COMPLETAMENTE IDEMPOTENTE: può essere eseguita più volte senza errori.
-- Target: PostgreSQL
-- Data: 2026-02-16
-- =========================================================================

-- =============================================
-- Rinomina colonne legacy (safe: controlla se esistono)
-- =============================================

DO $$
BEGIN
    -- shots → shots_total
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'player_season_stats' AND column_name = 'shots'
    ) AND NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'player_season_stats' AND column_name = 'shots_total'
    ) THEN
        ALTER TABLE player_season_stats RENAME COLUMN shots TO shots_total;
        RAISE NOTICE 'Rinominata: shots → shots_total';
    END IF;

    -- shots_on_target → shots_on
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'player_season_stats' AND column_name = 'shots_on_target'
    ) AND NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'player_season_stats' AND column_name = 'shots_on'
    ) THEN
        ALTER TABLE player_season_stats RENAME COLUMN shots_on_target TO shots_on;
        RAISE NOTICE 'Rinominata: shots_on_target → shots_on';
    END IF;
END
$$;

-- =============================================
-- Aggiunta nuove colonne (IF NOT EXISTS → idempotente)
-- =============================================

-- GAMES
ALTER TABLE player_season_stats ADD COLUMN IF NOT EXISTS lineups INTEGER;
ALTER TABLE player_season_stats ADD COLUMN IF NOT EXISTS captain BOOLEAN;

-- SHOTS (nel caso la tabella sia stata creata senza queste colonne)
ALTER TABLE player_season_stats ADD COLUMN IF NOT EXISTS shots_total INTEGER;
ALTER TABLE player_season_stats ADD COLUMN IF NOT EXISTS shots_on INTEGER;

-- GOALS
ALTER TABLE player_season_stats ADD COLUMN IF NOT EXISTS goals_conceded INTEGER;
ALTER TABLE player_season_stats ADD COLUMN IF NOT EXISTS saves INTEGER;

-- PASSES
ALTER TABLE player_season_stats ADD COLUMN IF NOT EXISTS passes_total INTEGER;
ALTER TABLE player_season_stats ADD COLUMN IF NOT EXISTS key_passes INTEGER;

-- TACKLES
ALTER TABLE player_season_stats ADD COLUMN IF NOT EXISTS tackles_total INTEGER;
ALTER TABLE player_season_stats ADD COLUMN IF NOT EXISTS blocks INTEGER;
ALTER TABLE player_season_stats ADD COLUMN IF NOT EXISTS interceptions INTEGER;

-- DUELS
ALTER TABLE player_season_stats ADD COLUMN IF NOT EXISTS duels_total INTEGER;
ALTER TABLE player_season_stats ADD COLUMN IF NOT EXISTS duels_won INTEGER;

-- DRIBBLES
ALTER TABLE player_season_stats ADD COLUMN IF NOT EXISTS dribbles_attempts INTEGER;
ALTER TABLE player_season_stats ADD COLUMN IF NOT EXISTS dribbles_success INTEGER;
ALTER TABLE player_season_stats ADD COLUMN IF NOT EXISTS dribbled_past INTEGER;

-- FOULS
ALTER TABLE player_season_stats ADD COLUMN IF NOT EXISTS fouls_drawn INTEGER;
ALTER TABLE player_season_stats ADD COLUMN IF NOT EXISTS fouls_committed INTEGER;

-- CARDS
ALTER TABLE player_season_stats ADD COLUMN IF NOT EXISTS yellow_cards INTEGER;
ALTER TABLE player_season_stats ADD COLUMN IF NOT EXISTS red_cards INTEGER;

-- PENALTY
ALTER TABLE player_season_stats ADD COLUMN IF NOT EXISTS penalty_won INTEGER;
ALTER TABLE player_season_stats ADD COLUMN IF NOT EXISTS penalty_committed INTEGER;
ALTER TABLE player_season_stats ADD COLUMN IF NOT EXISTS penalty_scored INTEGER;
ALTER TABLE player_season_stats ADD COLUMN IF NOT EXISTS penalty_missed INTEGER;
ALTER TABLE player_season_stats ADD COLUMN IF NOT EXISTS penalty_saved INTEGER;

-- =============================================
-- INDICE COMPOSITO
-- =============================================
CREATE INDEX IF NOT EXISTS ix_player_season_stats_team_season
    ON player_season_stats (team_id, season);
