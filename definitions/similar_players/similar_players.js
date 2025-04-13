find_similar_players.createSimilarityTable('centerbacks', [
  'aerial_duels_won_percentile',
  'clearences_percentile',
  'interceptions_percentile',
  'blocks_percentile',
  'progressive_passes_percentile',
  'long_pass_completion_percentile',
  'defensive_aggression_index_percentile'
], 3);

find_similar_players.createSimilarityTable('fullbacks', [
  'crosses_percentile',
  'progressive_carries_percentile',
  'successful_take_ons_percentile',
  'defensive_work_percentile',
  'progressive_passes_percentile',
  'final_third_passes_percentile',
  'attacking_contribution_percentile'
], 3);

find_similar_players.createSimilarityTable('goalkeepers', [
  'save_percentage_percentile',
  'delta_saves_percentile',
  'crosses_stop_rate_percentile',
  'sweeper_actions_percentile',
  'sweeper_distance_percentile',
  'final_third_passes_percentile',
  'goal_kick_launch_rate_percentile'
], 3);


find_similar_players.createSimilarityTable('midfielders', [
  'progressive_passes_percentile',
  'progressive_carries_percentile',
  'shot_creating_actions_percentile',
  'defensive_work_percentile',
  'final_third_passes_percentile',
  'goal_creating_actions_percentile',
  'pass_completion_rate_percentile'
], 3);

find_similar_players.createSimilarityTable('strikers', [
  'non_penalty_xg_percentile',
  'shots_percentile',
  'shots_accuracy_percentile',
  'pressing_effectiveness_percentile',
  'aerial_duels_percentile',
  'box_presence_percentile',
  'goal_creating_actions_percentile'
], 3);

find_similar_players.createSimilarityTable('wingers', [
  'successful_take_ons_percentile',
  'crosses_percentile',
  'shots_percentile',
  'progressive_carries_percentile',
  'aerial_duels_won_rate_percentile',
  'goal_creating_actions_percentile',
  'non_penalty_xg_percentile'
], 3);

