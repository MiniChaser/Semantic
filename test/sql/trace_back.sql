--输入dblp author 
-- 执行sql
select  * from final_author_table where dblp_author='Min Zhang 0005'

--作者结果集
dblp_author: Min Zhang 0005
first_author_count: 6   
semantic_scholar_paper_count: 8185
career_length: 22
total_influential_citations: 815
semantic_scholar_citation_count: 149045
semantic_scholar_h_index: 125
s2_author_id: 5432151,1390813134,2258690233,2315161412,2269805934,39767557,2267153486,48985192,2156053331,2273887691,2157502742,2263413056,1700777,2265392804,2157502208,2258690219,2372215151,2258690229,2329947801,2157501686,2258690227,2157501621,2263413125,2266375134,2346352158,2279744105,2156053262,2284915404,2259709647,2306158069,50495870,2364061603,2324304769,2272195992,2316793350,2157502438,40093418,2309443056,2273887680,2288523215,2265810714,2306158062,2331796452,2220937823,2279744107,2157503110,2157501829,66094431

--开始作者属性校验
--1. first_author_count=6   统计在dblp_papers 中排名第一的作者
--执行sql
SELECT
    t1.*,
    t2.authors
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
    t1.author = 'Min Zhang 0005'
    AND t1.position = 1;

--描述：作者在dblp 作为第一作者 总数为 6 
--结果：一致
--明细：    

key   	author	  position（位置）	    authors
conf/acl/ZhangZL10	Min Zhang 0005	1	["Min Zhang 0005", "Hui Zhang 0066", "Haizhou Li 0001"]
conf/acl/ZhangJALTL08	Min Zhang 0005	1	["Min Zhang 0005", "Hongfei Jiang", "AiTi Aw", "Haizhou Li 0001", "Chew Lim Tan", "Sheng Li 0003"]
conf/acl/ZhangCATZLL07	Min Zhang 0005	1	["Min Zhang 0005", "Wanxiang Che", "AiTi Aw", "Chew Lim Tan", "Guodong Zhou", "Ting Liu 0001", "Sheng Li 0003"]
conf/acl/ZhangZSZ06	Min Zhang 0005	1	["Min Zhang 0005", "Jie Zhang 0011", "Jian Su 0002", "Guodong Zhou"]
conf/naacl/ZhangZS06	Min Zhang 0005	1	["Min Zhang 0005", "Jie Zhang 0011", "Jian Su 0002"]
conf/emnlp/ZhangL09	Min Zhang 0005	1	["Min Zhang 0005", "Haizhou Li 0001"]



--2.semantic_scholar_paper_count =8185 ，dblp 作者对应s2_author_id Publications 的总和
--执行sql
SELECT
    A.*,
    p.url,
    paper_count
FROM
    (
        SELECT
            dblp_author_name,
            s2_author_id
        FROM
            authorships
        WHERE
            dblp_author_name = 'Min Zhang 0005'
            AND s2_author_id <> ''
        GROUP BY
            dblp_author_name,
            s2_author_id
    ) A
    LEFT JOIN s2_author_profiles P ON A.s2_author_id = P.s2_author_id;
--描述：作者对应 48个 s2_author_id, paper_count 总数为 8185 
--结果：一致
--明细：
dblp_author_name	s2_author_id	url	paper_count
Min Zhang 0005	40093418	https://www.semanticscholar.org/author/40093418	34
Min Zhang 0005	2157502208	https://www.semanticscholar.org/author/2157502208	8
Min Zhang 0005	2263413125	https://www.semanticscholar.org/author/2263413125	5
Min Zhang 0005	2266375134	https://www.semanticscholar.org/author/2266375134	4
Min Zhang 0005	2284915404	https://www.semanticscholar.org/author/2284915404	11
Min Zhang 0005	2258690233	https://www.semanticscholar.org/author/2258690233	19
Min Zhang 0005	2279744107	https://www.semanticscholar.org/author/2279744107	8
Min Zhang 0005	2157502438	https://www.semanticscholar.org/author/2157502438	14
Min Zhang 0005	2258690229	https://www.semanticscholar.org/author/2258690229	18
Min Zhang 0005	2272195992	https://www.semanticscholar.org/author/2272195992	6
Min Zhang 0005	2309443056	https://www.semanticscholar.org/author/2309443056	1
Min Zhang 0005	2265810714	https://www.semanticscholar.org/author/2265810714	4
Min Zhang 0005	2346352158	https://www.semanticscholar.org/author/2346352158	4
Min Zhang 0005	1700777	https://www.semanticscholar.org/author/1700777	784
Min Zhang 0005	2315161412	https://www.semanticscholar.org/author/2315161412	8
Min Zhang 0005	2267153486	https://www.semanticscholar.org/author/2267153486	10
Min Zhang 0005	2220937823	https://www.semanticscholar.org/author/2220937823	10
Min Zhang 0005	50495870	https://www.semanticscholar.org/author/50495870	686
Min Zhang 0005	2258690219	https://www.semanticscholar.org/author/2258690219	5
Min Zhang 0005	2265392804	https://www.semanticscholar.org/author/2265392804	4
Min Zhang 0005	2279744105	https://www.semanticscholar.org/author/2279744105	5
Min Zhang 0005	2258690227	https://www.semanticscholar.org/author/2258690227	26
Min Zhang 0005	2306158062	https://www.semanticscholar.org/author/2306158062	4
Min Zhang 0005	66094431	https://www.semanticscholar.org/author/66094431	18
Min Zhang 0005	2157501621	https://www.semanticscholar.org/author/2157501621	8
Min Zhang 0005	2329947801	https://www.semanticscholar.org/author/2329947801	2
Min Zhang 0005	2306158069	https://www.semanticscholar.org/author/2306158069	6
Min Zhang 0005	2269805934	https://www.semanticscholar.org/author/2269805934	19
Min Zhang 0005	2364061603	https://www.semanticscholar.org/author/2364061603	3
Min Zhang 0005	5432151	https://www.semanticscholar.org/author/5432151	107
Min Zhang 0005	2156053331	https://www.semanticscholar.org/author/2156053331	26
Min Zhang 0005	2157503110	https://www.semanticscholar.org/author/2157503110	9
Min Zhang 0005	2259709647	https://www.semanticscholar.org/author/2259709647	31
Min Zhang 0005	48985192	https://www.semanticscholar.org/author/48985192	60
Min Zhang 0005	2288523215	https://www.semanticscholar.org/author/2288523215	1
Min Zhang 0005	39767557	https://www.semanticscholar.org/author/39767557	5980
Min Zhang 0005	2273887691	https://www.semanticscholar.org/author/2273887691	39
Min Zhang 0005	2263413056	https://www.semanticscholar.org/author/2263413056	18
Min Zhang 0005	2316793350	https://www.semanticscholar.org/author/2316793350	2
Min Zhang 0005	2157502742	https://www.semanticscholar.org/author/2157502742	11
Min Zhang 0005	1390813134	https://www.semanticscholar.org/author/1390813134	112
Min Zhang 0005	2157501686	https://www.semanticscholar.org/author/2157501686	3
Min Zhang 0005	2324304769	https://www.semanticscholar.org/author/2324304769	8
Min Zhang 0005	2156053262	https://www.semanticscholar.org/author/2156053262	11
Min Zhang 0005	2157501829	https://www.semanticscholar.org/author/2157501829	18
Min Zhang 0005	2273887680	https://www.semanticscholar.org/author/2273887680	2
Min Zhang 0005	2331796452	https://www.semanticscholar.org/author/2331796452	9
Min Zhang 0005	2372215151	https://www.semanticscholar.org/author/2372215151	4
    
    

--3.career_length =22， 作者在Semantic论文数据中，发布文章的年限(最晚发布的论文的年份 - 作者最早发布的论文年份 + 1)
--执行sql1 获取最早发布年份2004年

select  semantic_paper_id,semantic_year,semantic_url from enriched_papers where semantic_paper_id in (
select semantic_paper_id from authorships   WHERE  dblp_author_name = 'Min Zhang 0005'  and s2_author_id <>''
) order by semantic_year limit  1 ;

--执行sql2 获取最新发布年份 2025年
select  semantic_paper_id,semantic_year,semantic_url from enriched_papers where semantic_paper_id in (
select semantic_paper_id from authorships   WHERE  dblp_author_name = 'Min Zhang 0005'  and s2_author_id <>''
) order by semantic_year desc limit  1;

--描述：作者career_length的计算结果 2025-2004+1 = 22 
--结果：一致
--明细：
semantic_paper_id											semantic_year						semantic_url
24bc03a0cb6e1a08276a4e433dd09a4018793b2c	2004	https://www.semanticscholar.org/paper/24bc03a0cb6e1a08276a4e433dd09a4018793b2c
4edcccfbf1d43a2dd3037608ab495bb5439fe959	2025	https://www.semanticscholar.org/paper/4edcccfbf1d43a2dd3037608ab495bb5439fe959

 

select * from enriched_papers limit 1 
--4.total_influential_citations =815  作者 s2 paper中发布的文章总引用次数
--执行sql
SELECT
	dblp_author,
	semantic_url,
	influentialCitationCount
FROM
	( --将 enriched_papers（step2） 的数据根据dblp_author 拆解成每个author一行数据
--这样就可以统计每个author 对应的paper在s2 中的influentialCitationCount 值
	SELECT dblp_key, jsonb_array_elements_text ( dblp_authors ) AS dblp_author, influentialCitationCount,semantic_url FROM enriched_papers ) AS p1 
WHERE
	dblp_author ='Min Zhang 0005'

--描述：明细数据influentialCitationCount  列进行求和，得出815
--结果：一致
--明细：
dblp_author	semantic_url		influentialCitationCount
Min Zhang 0005	https://www.semanticscholar.org/paper/e6b7bb81c863235ffe22f25b316b6270080d28e4	0
Min Zhang 0005	https://www.semanticscholar.org/paper/5a5af9f5aa6703c5bde0492ccdaff926c60ca975	2
Min Zhang 0005	https://www.semanticscholar.org/paper/fa7322f88c0a206cb8b7f24fefe2cd3b10f6a7d4	2
Min Zhang 0005	https://www.semanticscholar.org/paper/80d742c5293db96f0b7b7eb6e7f4b76ce966f52c	0
...
--5.semantic_scholar_查询结果=149045
--执行sql
	SELECT s2_author_id,name s2_author_name,url,citation_count  from  s2_author_profiles where s2_author_id in 
	(SELECT  s2_author_id FROM authorships WHERE dblp_author_name ='Min Zhang 0005'  and s2_author_id <> '')

	
--描述：将查询结果citation_count求合，得出149045
--结果：一致
--明细：
s2_author_id  s2_author_name 	url 										citation_count
40093418	Min Zhang	https://www.semanticscholar.org/author/40093418	706
2157502208	Min Zhang	https://www.semanticscholar.org/author/2157502208	130
2263413125	Min Zhang	https://www.semanticscholar.org/author/2263413125	22
2266375134	Min Zhang	https://www.semanticscholar.org/author/2266375134	63
...

--6.semantic_scholar_h_index=125
--执行sql
SELECT s2_author_id,name s2_author_name,url, h_index from s2_author_profiles where s2_author_id in 
(SELECT  s2_author_id FROM authorships WHERE dblp_author_name ='Min Zhang 0005'  and s2_author_id <> '')
order by h_index desc 


--描述：取第一条h_index 的值125
--结果：一致
--明细：
s2_author_id	s2_author_name	url		 h_index
39767557	M. Zhang	https://www.semanticscholar.org/author/39767557	125
1700777	Min Zhang	https://www.semanticscholar.org/author/1700777	43
50495870	M. Zhang	https://www.semanticscholar.org/author/50495870	39
5432151	Min Zhang	https://www.semanticscholar.org/author/5432151	29
1390813134	Min Zhang	https://www.semanticscholar.org/author/1390813134	25
...






