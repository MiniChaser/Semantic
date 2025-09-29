-- =====================================================
-- Target: Verify semantic_scholar_paper_count field is correct
-- =====================================================

-- SQL: Calculate semantic_scholar_paper_count from authorships and s2_author_profiles
SELECT
    sum(paper_count) as semantic_scholar_paper_count
FROM
    (
        SELECT
            dblp_author_name,
            s2_author_id
        FROM
            authorships
        WHERE
            dblp_author_name = {dblp_author_name}
            AND s2_author_id <> '' AND s2_author_id is not null 
        GROUP BY
            dblp_author_name,
            s2_author_id
    ) A
    LEFT JOIN s2_author_profiles P ON A.s2_author_id = P.s2_author_id;