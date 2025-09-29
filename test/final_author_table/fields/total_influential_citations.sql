-- =====================================================
-- Target: Verify total_influential_citations field is correct
-- =====================================================

-- SQL: Calculate total_influential_citations from enriched_papers  
SELECT
    SUM(influentialCitationCount) as total_influential_citations
FROM
    enriched_papers
WHERE
    semantic_paper_id IN (
        SELECT
            semantic_paper_id
        FROM
            authorships
        WHERE
            dblp_author_name ={dblp_author_name}
        AND s2_author_id <> '' AND s2_author_id is not null 

    );
