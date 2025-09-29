-- =====================================================
-- Target: Verify semantic_scholar_h_index field is correct
-- =====================================================

-- SQL: Calculate semantic_scholar_h_index from authorships and s2_author_profiles
SELECT
       h_index as semantic_scholar_h_index
FROM
    s2_author_profiles
WHERE
    s2_author_id IN (
        SELECT
            s2_author_id
        FROM
            authorships
        WHERE
            dblp_author_name =  {dblp_author_name}
            AND s2_author_id <> '' AND s2_author_id is not null 
    )
ORDER BY
    h_index DESC
LIMIT 1;