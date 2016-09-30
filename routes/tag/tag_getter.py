from flask_restful import Resource, reqparse
from neo4j.v1 import ResultError
from connector import neo4j
from routes.utils import addargs, addTimeFilter, makeResponse

parser = reqparse.RequestParser()

class GetTag(Resource):
    def get(self, tag_id):
        result = neo4j.query_neo4j("MATCH (find:tag {tag_id: %d}) RETURN find" % tag_id)
        try:
            return makeResponse(result.single()['find'].properties, 200)
        except ResultError:
            return makeResponse("ERROR : Cannot find tag with tid: %d" % tag_id, 204)

class GetTags(Resource):
    def get(self):
        req = "MATCH (t:tag) RETURN t.tag_id AS tid, t.label AS label"
        req += addargs()
        result = neo4j.query_neo4j(req)
        tags = []
        for record in result:
            tags.append({'tid': record['tid'], "label": record['label']})
        return makeResponse(tags, 200)
        
class GetTagsByParent(Resource):
    def get(self, parent_tag_id):
        req = "MATCH (parent:tag {tag_id : %d})<-[:IS_CHILD]-(child:tag) RETURN child" % parent_tag_id
        result = neo4j.query_neo4j(req)
        tags = []
        for record in result:
            tags.append(record['child'].properties)
        return makeResponse(tags, 200)


