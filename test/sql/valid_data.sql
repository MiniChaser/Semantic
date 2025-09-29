------------------------------step1---------------------------------
--1.1 统计dblp paper total  
--期望输出：统计dblp xml 解析出来的 dblp paper总数
SELECT COUNT
	( KEY ) 
FROM
	dblp_papers --1.2 统计dblp author total
--期望输出：将dblp xml中 authors集合进行拆解，以dblp author name 作为唯一值罗列出来，并统计作者总数，和final总行数对应
SELECT
	author 
FROM
	( SELECT KEY, jsonb_array_elements_text ( authors ) AS author FROM dblp_papers ) 
GROUP BY
	author;
--1.3 查询作者名字是否获取异常，比如因为特殊符号，只获取到1-3个字母的场景，通过作者名字长度来筛选
--期望输出：未查询到异常数据
SELECT DISTINCT P
	.* 
FROM
	dblp_papers
	P CROSS JOIN jsonb_array_elements_text ( P.authors ) AS author_name 
WHERE
	LENGTH ( author_name ) < 4;
------------------------------step2---------------------------------
--2.1 dblp总数核对，enriched_papers total 37420 
--期望输出：查询paper总数 = dblp paper总数（对比1.2 文章总数是否一致）
SELECT COUNT
	( dblp_key ) 
FROM
	enriched_papers --2.2 查询dblp paper title 在s2找不到的数据，
--输出说明：查询出数据属于正常现场，可人工抽查，根据dblp_title 在s2网站搜索
SELECT
	* 
FROM
	enriched_papers 
WHERE
	semantic_paper_id IS NULL --2.3 统计dblp author 在第二步未匹配到 s2 author 的数据
--输出说明：
--1)当semantic_paper_id为空时，说明根据dblp title 在s2 未找到文章，不为空时说明author 名字在s2 authors 匹配不到作者信息  
--2)当s2_author_name 不为空 s2_author_id为空时，说明 s2返回的作者集中，author_id 为null
SELECT A
	.dblp_author_name,
	A.s2_author_id,
	A.s2_author_name,
	A.semantic_paper_id,
	P.dblp_authors,
	P.semantic_authors 
FROM
	authorships
	A LEFT JOIN enriched_papers P ON A.semantic_paper_id = P.semantic_paper_id 
WHERE
	s2_author_id = '';
------------------------------step3---------------------------------
--3.1  验证h_index 数据，
-- 验证方式： dblp author 的h_index 等于 s2 author api中 h_index的最大值
-- 数据关系： dblp author会对应多个s2author
-- 期望输出： 未查询出不一致数据
SELECT
	f.* 
FROM
	final_author_table f
	LEFT JOIN (
	SELECT A
		.dblp_author_name,
		MAX ( P.h_index ) AS dblp_h_index 
	FROM
		authorships
		A LEFT JOIN s2_author_profiles P ON A.s2_author_id = P.s2_author_id 
	GROUP BY
		A.dblp_author_name 
	) AS T ON f.dblp_author = T.dblp_author_name 
WHERE
	f.semantic_scholar_h_index <> T.dblp_h_index;
	
--3.2 验证 semantic_citation_count 
--验证方式：比对final表中 semantic_citation_count 和 s2_author_profiles citation_count 求合是否一致
--数据关系：dblp author 对应的s2 author id 通过s2 author api 获取 citation_count
--期望输出：未查询出不一致数据
SELECT
	f.dblp_author,
	f.semantic_scholar_citation_count,
	T.semantic_citation_count 
FROM
	final_author_table f
	LEFT JOIN (
	SELECT A
		.dblp_author_name,
		SUM ( P.citation_count ) AS semantic_citation_count 
	FROM
		-- 先对 authorships 表去重，同一个作者会出现在多个文章里面，去重后就得到dblp author 对应（1-n）个s2 author
		-- 然后再根据 s2 author Id 聚合统计得出 dblp author 的semantic_citation_count
		(SELECT dblp_author_name, s2_author_id FROM authorships WHERE s2_author_id <> '' GROUP BY dblp_author_name, s2_author_id ) A 
		LEFT JOIN s2_author_profiles P ON A.s2_author_id = P.s2_author_id 
	WHERE
		A.s2_author_id <> '' 
	GROUP BY A.dblp_author_name 
	) AS T ON f.dblp_author = T.dblp_author_name 
	WHERE-- 这里比对 最终数据和过程数据是否一致， <> 改成 = 则查询相等的数据
	f.semantic_scholar_citation_count <> T.semantic_citation_count;
	
--3.3 验证 total_influential_citations 的准确性
--验证方式：比对final表中 total_influential_citations 和 enriched_papers 中influentialCitationCount 的求和是否一致
--数据关系：dblp author 对应的paper title  = s2 title 
--期望输出：未查询出不一致数据
SELECT
	f.dblp_author,
	f.total_influential_citations,
	T.influentialCitationCount 
FROM
	final_author_table f
	LEFT JOIN (
	SELECT
		dblp_author,
		SUM ( influentialCitationCount ) AS influentialCitationCount 
	FROM
		( SELECT dblp_key, jsonb_array_elements_text ( dblp_authors ) AS dblp_author, influentialCitationCount FROM enriched_papers ) AS p1 
	GROUP BY
		dblp_author 
	) AS T ON f.dblp_author = T.dblp_author 
	WHERE-- 这里比对 最终数据和过程数据是否一致， <> 改成 = 则查询相等的数据
	f.total_influential_citations <> T.influentialCitationCount;
	
--dblp作者influentialCitationCount 明细抽查，根据实际情况修改where 后面的查询条件
SELECT
	dblp_key,
	dblp_author,
	influentialCitationCount 
FROM
	( --将 enriched_papers（step2） 的数据根据dblp_author 拆解成每个author一行数据
--这样就可以统计每个author 对应的paper在s2 中的influentialCitationCount 值
	SELECT dblp_key, jsonb_array_elements_text ( dblp_authors ) AS dblp_author, influentialCitationCount FROM enriched_papers ) AS p1 
WHERE
	dblp_author = 'Richard Sproat';
	