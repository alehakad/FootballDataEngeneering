// player_similarity.js - A dynamic script to generate similarity tables for any position

/**
 * Create a player similarity table for a specific position
 * @param {string} positionName - Name of the position (e.g., 'centerbacks', 'fullbacks')
 * @param {Array<string>} metrics - List of percentile metrics to use for similarity calculation
 * @param {int} n_similar - Number of similar players to find for each player
 */
function createSimilarityTable(positionName, metrics, n_similar) {
  const tableName = `${positionName}_similarity`;
  const sourceTable = `radar_metrics_${positionName}_career`;
  const position = positionName.endsWith('s') ? positionName.slice(0, -1) : positionName;
  
  // Build the dot product expression dynamically
  const dotProductExpr = metrics.map(metric => `p1.${metric} * p2.${metric}`).join(' + ');
  
  // Build the magnitude expressions dynamically
  const magnitude1Expr = metrics.map(metric => `POW(p1.${metric}, 2)`).join(' + ');
  const magnitude2Expr = metrics.map(metric => `POW(p2.${metric}, 2)`).join(' + ');
  
  publish(tableName).config({
    type: "table",
    schema: "analytics",
    description: `Top 3 most similar ${position}s for each player based on cosine similarity of radar metrics`,
    tags: ["radar", "similarity", position],
  }).query( ctx => `
    WITH 
    -- Select the metrics we want to use for similarity calculation
    radar_metrics AS (
      SELECT 
        rm.player_id,
        ${metrics.map(m => `rm.${m}`).join(',\n        ')}
      FROM 
        ${ctx.ref(sourceTable)} rm
    ),
    
    -- Calculate cosine similarity between all players
    similarity_calc AS (
      SELECT
        p1.player_id,
        p2.player_id AS similar_player_id,
        
        -- Cosine similarity calculation
        SAFE_DIVIDE(
          (${dotProductExpr}),
          (SQRT(${magnitude1Expr}) * SQRT(${magnitude2Expr}))
        ) AS similarity_score,
        
        -- Rank similar players
        ROW_NUMBER() OVER (
          PARTITION BY p1.player_id
          ORDER BY SAFE_DIVIDE(
            (${dotProductExpr}),
            (SQRT(${magnitude1Expr}) * SQRT(${magnitude2Expr}))
          ) DESC
        ) AS similarity_rank
      FROM 
        radar_metrics p1
      CROSS JOIN 
        radar_metrics p2
      WHERE 
        p1.player_id <> p2.player_id  -- Don't compare players to themselves
    )
    
    -- Select the top 3 most similar players for each player
    SELECT
      player_id,
      similar_player_id,
      similarity_score,
      similarity_rank,
      '${position}' AS position
    FROM 
      similarity_calc
    WHERE 
      similarity_rank <= ${n_similar}
    AND 
      similarity_score IS NOT NULL
    ORDER BY 
      player_id, similarity_rank
  `);
}

module.exports = { createSimilarityTable };