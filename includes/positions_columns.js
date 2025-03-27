// Define required columns for each soccer position

const positionColumns = {
  Goalkeeper: [
    'shot_stopping_saves',
    'shot_stopping_sota',
    'shot_stopping_psxg',
    'shot_stopping_ga',
    'crosses_stp',
    'crosses_opp',
    'sweeper_#opa',
    'sweeper_avgdist',
    'total_cmp%',
    'goal_kicks_launch%',
    'minutes'
  ],

  Centerback: [
    'aerial_duels_won%',
    'clr_',
    'int_',
    'blocks_blocks',
    'tackles_tkl',
    'tackles_tklw',
    'minutes'
  ],

  Fullback: [
    'crosses_crs',
    'tackles_tkl',
    'tackles_tklw',
    'tackles_def_3rd',
    'tackles_mid_3rd',
    'tackles_att_3rd',
    'int_',
    'minutes'
  ],

  Midfielder: [
    'total_cmp%',
    'tackles_tkl',
    'int_',
    'tackles_mid_3rd',
    'xag_',
    '1_3_',
    'minutes'
  ],

  Winger: [
    'crosses_crs',
    'xag_',
    'xa_',
    'performance_og',
    'performance_recov',
    'minutes'
  ],

  Striker: [
    'performance_pkwon',
    'performance_pkcon',
    'aerial_duels_won%',
    'aerial_duels_won',
    'challenges_att_3rd',
    'ppa_',
    'minutes'
  ]
};

// Function to get columns for a specific position
function getColumnsForPosition(position) {
  return positionColumns[position] || [];
}

// Function to generate a SELECT statement for a specific position
function generatePositionSelect(position) {
  const columns = getColumnsForPosition(position);
  return columns.map(col => `${col}`).join(', ');
}

module.exports = {
  positionColumns,
  getColumnsForPosition,
  generatePositionSelect
};