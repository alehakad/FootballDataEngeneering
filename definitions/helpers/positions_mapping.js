// positions mapping array
const positionMappings = [
  { source_position: 'Right-Back', position_id: 'RB' },
  { source_position: 'Attacking Midfield', position_id: 'AM' },
  { source_position: 'Left-Back', position_id: 'LB' },
  { source_position: 'Centre-Forward', position_id: 'FW' },
  { source_position: 'Centre-Back', position_id: 'CB' },
  { source_position: 'Central Midfield', position_id: 'CM' },
  { source_position: 'Goalkeeper', position_id: 'GK' },
  { source_position: 'Defensive Midfield', position_id: 'DM' },
  { source_position: 'Right Winger', position_id: 'RW' },
  { source_position: 'Left Winger', position_id: 'LW' },
  { source_position: 'Right Midfield', position_id: 'RM' },
  { source_position: 'Second Striker', position_id: 'FW' },
  { source_position: 'Left Midfield', position_id: 'LM' }
];
 
const structStatements = positionMappings
  .map(mapping => `STRUCT('${mapping.source_position}' AS source_position, '${mapping.position_id}' AS position_id)`)
  .join(',\n  '); 

 publish("positions_mapping", {
    type: "table",
    schema:"helpers",
    description: "Table for mapping between TF and FBref positions"
  }).query(ctx => `SELECT source_position, position_id
  FROM UNNEST([${structStatements}])`);