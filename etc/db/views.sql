CREATE OR REPLACE VIEW quality AS
SELECT node,
DATEDIFF(MAX(as_of), MIN(as_of)) / (COUNT(as_of) - 1) * 86400 AS frequency
FROM reading
JOIN node ON reading.node = node.id
WHERE node.segment IS NOT NULL
GROUP BY node;
