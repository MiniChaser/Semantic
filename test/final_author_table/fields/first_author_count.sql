-- =====================================================
-- Target: Verify first_author_count field is correct
-- =====================================================

-- SQL: Calculate first_author_count from enriched_papers and authorships

SELECT
    COUNT(semantic_paper_id) AS first_author_count
FROM (
    SELECT
        a.semantic_paper_id
    FROM
        authorships a
    LEFT JOIN enriched_papers e 
        ON a.semantic_paper_id = e.semantic_paper_id
    WHERE
        a.dblp_author_name =  {dblp_author_name}
        AND a.s2_author_id IS NOT NULL
        AND a.s2_author_id <> ''
        AND a.s2_author_id = e.first_author_semantic_id
    GROUP BY
        a.semantic_paper_id
) AS first_author_papers;