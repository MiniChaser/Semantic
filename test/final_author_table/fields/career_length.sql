-- =====================================================
-- Target: Verify career_length field is correct
-- =====================================================

-- SQL: Calculate career_length from authorships and s2_author_profiles
SELECT 
    COALESCE(MAX(semantic_year) - MIN(semantic_year) + 1, 0) AS career_length
FROM enriched_papers 
WHERE semantic_paper_id IN (
    SELECT semantic_paper_id 
    FROM authorships   
    WHERE dblp_author_name = {dblp_author_name}
      AND s2_author_id <> ''  AND s2_author_id is not null 
);
