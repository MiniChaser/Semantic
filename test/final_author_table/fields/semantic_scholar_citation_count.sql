-- =====================================================
-- Target: Verify semantic_scholar_citation_count field is correct
-- =====================================================

-- SQL: Calculate semantic_scholar_citation_count from s2_author_profiles and s2_author_profiles

SELECT
   sum(citation_count) as semantic_scholar_citation_count
    
FROM
    s2_author_profiles
WHERE
    s2_author_id IN (
        SELECT
            s2_author_id
        FROM
            authorships
        WHERE
            dblp_author_name = {dblp_author_name}
            AND s2_author_id <> '' AND s2_author_id is not null 
    );

      