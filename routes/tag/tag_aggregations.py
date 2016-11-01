from flask_restful import Resource, reqparse
from neo4j.v1 import ResultError
from connector import neo4j
from routes.utils import addargs, makeResponse

parser = reqparse.RequestParser()


class CountAllTag(Resource):
    def get(self):
        req = "MATCH (:tag) RETURN count(*) AS nb_tags"
        result = neo4j.query_neo4j(req)
        try:
            return makeResponse(result.single()['nb_tags'], 200)
        except ResultError:
            return makeResponse("ERROR", 500)
            
class CountTagsByParent(Resource):
    def get(self, parent_tag_id):
        req = "MATCH (t:tag {tag_id : %d})<-[:IS_CHILD]-(:tag) RETURN count(*) AS nb_child" % parent_tag_id
        result = neo4j.query_neo4j(req)
        try:
            return makeResponse(result.single()['nb_child'], 200)
        except ResultError:
            return makeResponse("ERROR", 500)

class TagsByDate(Resource):
    def get(self, timestamp1, timestamp2, limit):
        req = "match (n)<-[:ANNOTATES]-(a:annotation)-[:REFERS_TO]->(t:tag) where n.timestamp > %d and n.timestamp < %d and ('post' in labels(n) or 'comment' in labels(n)) return t.tag_id as id, t.label as label, count(t) as count order by count desc limit %d" % (timestamp1, timestamp2, limit)
        result = neo4j.query_neo4j(req)
        tags=[]
        for record in result:
            tags.append({'id': record['id'], "label": record['label'], "count": record['count']})

        try:
            return makeResponse(tags, 200)
        except ResultError:
            return makeResponse("ERROR", 500)

