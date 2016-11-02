import psutil
import requests
import time
from time import strftime
from flask_restful import Resource, reqparse
from importer.importFromJson import ImportFromJson
from routes.utils import makeResponse
from neo4j.v1 import ResultError
from connector import neo4j
import configparser
config = configparser.ConfigParser()
config.read("config.ini")


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
        json_file = json.load(open(config['importer']['json_users_path']))
        importer.create_users(json_file)
        json_file = json.load(open(config['importer']['json_posts_path']))
        importer.create_posts(json_file)
        json_file = json.load(open(config['importer']['json_comments_path']))
        importer.create_comments(json_file)
        json_file = json.load(open(config['importer']['json_tags_path']))
        importer.create_tags(json_file)
        json_file = json.load(open(config['importer']['json_annotations_path']))
        importer.create_annotations(json_file)
        return makeResponse(importer.end_import(), 200)


class HardUpdate(Resource):
    def get(self):
        importer = ImportFromJson(True)
        json_file = json.load(open(config['importer']['json_users_path']))
        importer.create_users(json_file)
        json_file = json.load(open(config['importer']['json_posts_path']))
        importer.create_posts(json_file)
        json_file = json.load(open(config['importer']['json_comments_path']))
        importer.create_comments(json_file)
        json_file = json.load(open(config['importer']['json_tags_path']))
        importer.create_tags(json_file)
        json_file = json.load(open(config['importer']['json_annotations_path']))
        importer.create_annotations(json_file)
        return makeResponse(importer.end_import(), 200)


class UpdateFromEdgeRyders(Resource):
    def get(self):
        importer = ImportFromJson(False)
        # no user update
        # first update tag
        req= requests.get(config['importer_edgeryders']['json_tags_path'])
        json_file = req.json()
        importer.create_tags(json_file)
        # then the rest
        updateList = ['post', 'comment', 'annotation']
        for elem in updateList:
            req = "MATCH (n:"+elem+") RETURN max(n.timestamp) AS max"
            result = neo4j.query_neo4j(req)
            try:
                most_recent = time.gmtime(int(result.single()['max'])/1000)
            except ResultError:
                print("Problem from neo4j request.")
            since_str = time.strftime('%Y%m%d', most_recent)
            req= requests.get(config['importer_edgeryders']['json_'+elem+'s_path']+"?since="+since_str)
            json_file = req.json()
            if elem == 'post':
                importer.create_posts(json_file)
            if elem == 'comment':
                importer.create_comments(json_file)
            if elem == 'annotation':
                importer.create_annotations(json_file)
        return makeResponse(importer.end_import(), 200)


class HardUpdateFromEdgeRyders(Resource):
    def get(self):
        importer = ImportFromJson(True)
        req= requests.get(config['importer_edgeryders']['json_users_path'])
        json_file = req.json()
        req= requests.get(open(config['importer_edgeryders']['json_users_path']))
        json_file = req.json()
        importer.create_users(json_file)
        req= requests.get(open(config['importer_edgeryders']['json_posts_path']))
        json_file = req.json()
        importer.create_posts(json_file)
        req= requests.get(open(config['importer_edgeryders']['json_comments_path']))
        json_file = req.json()
        importer.create_comments(json_file)
        req= requests.get(open(config['importer_edgeryders']['json_tags_path']))
        json_file = req.json()
        importer.create_tags(json_file)
        req= requests.get(open(config['importer_edgeryders']['json_annotations_path']))
        json_file = req.json()
        importer.create_annotations(json_file)
        return makeResponse(importer.end_import(), 200)


class UpdateUsers(Resource):
    def get(self):
        importer = ImportFromJson(False)
        json_file = json.load(open(config['importer']['json_users_path']))
        importer.create_users(json_file)
        return makeResponse(importer.end_import(), 200)


class UpdatePosts(Resource):
    def get(self):
        importer = ImportFromJson(False)
        json_file = json.load(open(config['importer']['json_posts_path']))
        importer.create_posts(json_file)
        return makeResponse(importer.end_import(), 200)


class UpdateComments(Resource):
    def get(self):
        importer = ImportFromJson(False)
        json_file = json.load(open(config['importer']['json_comments_path']))
        importer.create_comments(json_file)
        return makeResponse(importer.end_import(), 200)


class UpdateTags(Resource):
    def get(self):
        importer = ImportFromJson(False)
        json_file = json.load(open(config['importer']['json_tags_path']))
        importer.create_tags(json_file)
        return makeResponse(importer.end_import(), 200)


class UpdateAnnotations(Resource):
    def get(self):
        importer = ImportFromJson(False)
        json_file = json.load(open(config['importer']['json_annotations_path']))
        importer.create_annotations(json_file)
        return makeResponse(importer.end_import(), 200)
