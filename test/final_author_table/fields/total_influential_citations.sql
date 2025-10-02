-- =====================================================
-- Target: Verify total_influential_citations field is correct
-- =====================================================

-- SQL: Calculate total_influential_citations from enriched_papers  
   SELECT
    SUM(influentialCitationCount) AS influentialCitationCount
FROM (
    SELECT DISTINCT ON (semantic_paper_id)
        semantic_paper_id,
        influentialCitationCount
    FROM
        enriched_papers
    WHERE
        semantic_paper_id IN (
            SELECT
                semantic_paper_id
            FROM
                authorships
            WHERE
                dblp_author_name = {dblp_author_name}
                AND s2_author_id <> ''
                AND s2_author_id IS NOT NULL
        )
) AS distinct_papers;