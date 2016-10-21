import psutil
from flask_restful import Resource, reqparse
from importer.importFromJson import ImportFromJson
from routes.utils import makeResponse
from neo4j.v1 import ResultError
from connector import neo4j


class Info(Resource):
    def get(self):
        # todo change status
        response = {"status": "ok", "version": "0000000000000", "percentRamUsage": psutil.virtual_memory()[2], "percentDiskUsage": psutil.disk_usage('/')[3]}
        req = "MATCH (n) RETURN max(n.timestamp) AS version"
        result = neo4j.query_neo4j(req)
        try:
            response['version'] = result.single()['version']
        except ResultError:
            return makeResponse("ERROR : Cannot load latest timestamp", 204)

        return makeResponse(response, 200)


class Update(Resource):
    def get(self):
        importer = ImportFromJson(False)
        importer.create_users()
        importer.create_posts()
        importer.create_comments()
#        importer.create_tags()
#        importer.create_annotations()
        return makeResponse(importer.end_import(), 200)


class HardUpdate(Resource):
    def get(self):
        importer = ImportFromJson(True)
        importer.create_users()
        importer.create_posts()
        importer.create_comments()
        importer.create_tags()
        importer.create_annotations()
        return makeResponse(importer.end_import(), 200)


class UpdateUsers(Resource):
    def get(self):
        importer = ImportFromJson(False)
        importer.create_users()
        return makeResponse(importer.end_import(), 200)


class UpdatePosts(Resource):
    def get(self):
        importer = ImportFromJson(False)
        importer.create_posts()
        return makeResponse(importer.end_import(), 200)


class UpdateComments(Resource):
    def get(self):
        importer = ImportFromJson(False)
        importer.create_comments()
        return makeResponse(importer.end_import(), 200)


class UpdateTags(Resource):
    def get(self):
        importer = ImportFromJson(False)
        importer.create_tags()
        return makeResponse(importer.end_import(), 200)


class UpdateAnnotations(Resource):
    def get(self):
        importer = ImportFromJson(False)
        importer.create_annotations()
        return makeResponse(importer.end_import(), 200)
