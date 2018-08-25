"""
MATCH g=(p1:Paper)-[:HAS]-(pa1:PaperAuthor)-[:IS]-(a:Author)-[:IS]-(pa2:PaperAuthor)-[:HAS]-(p2:Paper)
where p1 <> p2
RETURN g
"""


"""
MATCH g=(a1:Author)-[:IS]-(pa1:PaperAuthor)-[:HAS]-(p1:Paper)-[:CITED]->(p2:Paper)-[:HAS]-(pa2:PaperAuthor)-[:IS]-(a2:Author)
where a1 <> a2
Merge (a1)-[:CITED]->(a2)
"""

"""
MATCH g=(a1:Affiliation)-[:AFFILIATE]-(pa1:PaperAuthor)-[:HAS]-(p1:Paper)-[pc:CITED]->(p2:Paper)-[:HAS]-(pa2:PaperAuthor)-[:AFFILIATE]-(a2:Affiliation)
where a1 <> a2
Merge (a1)-[:CITED {CitationCount: count(pc)}]->(a2)
"""

"""
MATCH g=(a1:Affiliation)-[:AFFILIATE]-(pa1:PaperAuthor)-[:HAS]-(p1:Paper {})-[pc:CITED]->(p2:Paper)-[:HAS]-(pa2:PaperAuthor)-[:AFFILIATE]-(a2:Affiliation)
where a1 <> a2
and exists(p1.level)
and exists(p2.level)
and p1.CitationCount > 5
WITH a1, a2, count(pc) AS count
merge (a1)-[:STRONG_CITED {citationCount: count}]->(a2)
"""


MATCH path = (:Affiliation)-[:STRONG_CITED]->(:Affiliation)
CALL apoc.gephi.add('','workspace1',path,'citiationCount') yield nodes
return *