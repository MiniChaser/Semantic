-- =====================================================
-- Target: Verify first_author_count field is correct
-- =====================================================

-- SQL: Calculate first_author_count from authorships and s2_author_profiles
  SELECT
      count(t1.key) as first_author_count
  FROM
      (
          SELECT
              key,
              author,
              ordinality AS position
          FROM
              dblp_papers,
              jsonb_array_elements_text(authors) WITH ORDINALITY AS t(author, ordinality)
      ) AS t1
      LEFT JOIN dblp_papers t2 ON t2.key = t1.key
  WHERE
      t1.author = {dblp_author_name}
      AND t1.position = 1;
      