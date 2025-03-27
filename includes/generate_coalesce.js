function generateCoalesceColumns(tables) {
    const columns = retrieve({
        query: `
            SELECT column_name
            FROM \`${ref("new_stats_rows")}.INFORMATION_SCHEMA.COLUMNS\`
            WHERE table_name = 'new_stats_rows'
            AND column_name NOT IN ('player_', 'source_file')
        `
    });
    const coalesceStatements = columns.map(column => 
        `COALESCE(${tables.map(table => `${table}.${column.column_name}`).join(', ')}) AS ${column.column_name}`
    );

    return coalesceStatements.join(',\n');
}

module.exports = { generateCoalesceColumns };