-- =====================================================
-- Target: Verify total_influential_citations field is correct
-- =====================================================

-- SQL: Calculate total_influential_citations from enriched_papers  
SELECT
    SUM(influentialCitationCount) AS influentialCitationCount
FROM
    authorships a
LEFT JOIN
    enriched_papers e 
    ON a.paper_id = e.id
WHERE
    a.s2_author_id IS NOT NULL
    AND a.s2_author_id <> ''
    AND a.dblp_author_name = {dblp_author_name};