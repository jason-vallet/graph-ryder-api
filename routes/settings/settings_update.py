import psutil
import requests
import time
import json
from time import strftime
from flask_restful import Resource, reqparse
from importer.importFromJson import ImportFromJson
from importer.importFromDiscourse import ImportFromDiscourse
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


class Status(Resource):
    def get(self):
        elementType = ['user', 'post', 'comment', 'annotation', 'tag']
        labels = ['Users', 'Posts', 'Comments', 'Annotations', 'Tags']
        data = []
        for t in elementType:
            req = "MATCH (e: "+t+")--() RETURN count(distinct e) as nb"
            result = neo4j.query_neo4j(req)
            for record in result:
                data.append(record['nb'])
        return makeResponse({'labels': labels, 'data': [data]}, 200)


class GetContentNotTagged(Resource):
    def get(self):
        req = "MATCH (p:post) WHERE NOT (p)<-[:ANNOTATES]- (: annotation) RETURN p.post_id AS post_id,p.label AS label, p.timestamp AS timestamp ORDER BY timestamp DESC"
        result = neo4j.query_neo4j(req)
        posts = []
        for record in result:
            posts.append({'post_id': record['post_id'], "label": record['label'], "timestamp": record['timestamp']})
        
        req = "MATCH (c:comment) WHERE NOT (c)<-[:ANNOTATES]- (: annotation) RETURN c.comment_id AS comment_id, c.label AS label, c.timestamp AS timestamp ORDER BY timestamp DESC"
        result = neo4j.query_neo4j(req)
        comments = []
        for record in result:
            comments.append({'comment_id': record['comment_id'], "label": record['label'], "timestamp": record['timestamp']})

        return makeResponse({'posts': posts, "comments": comments}, 200)


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
        #json_file = json.load(open(config['importer']['json_users_path']))
        #importer.create_users(json_file)
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
        #req= requests.get(config['importer_edgeryders']['json_users_path'])
        #json_file = req.json()
        #importer.create_users(json_file)
        req= requests.get(config['importer_edgeryders']['json_posts_path'])
        if req.status_code != 200:
            print("Error req posts: "+str(req.status_code))
        else:
            json_file = req.json()
            importer.create_posts(json_file)
        req= requests.get(config['importer_edgeryders']['json_comments_path'])
        if req.status_code != 200:
            print("Error req comments: "+str(req.status_code))
        else:
            json_file = req.json()
            importer.create_comments(json_file)
        req= requests.get(config['importer_edgeryders']['json_tags_path'])
        if req.status_code != 200:
            print("Error req tags: "+str(req.status_code))
        else:
            json_file = req.json()
            importer.create_tags(json_file)
        req= requests.get(config['importer_edgeryders']['json_annotations_path'])
        if req.status_code != 200:
            print("Error req annotations: "+str(req.status_code))
        else:
            json_file = req.json()
            importer.create_annotations(json_file)
        return makeResponse(importer.end_import(), 200)


class HardUpdateFromEdgeRydersDiscourse(Resource):
    def get(self):
        importer = ImportFromDiscourse(True)
        Continue = True
        page_val = 0

        importer.create_users()

        while Continue:
            print(page_val)
            cat_url = config['importer_discourse']['abs_path']+config['importer_discourse']['tag_rel_path']+config['importer_discourse']['tag_focus']+".json?api_key="+config['importer_discourse']['admin_api_key']+"&api_username="+config['importer_discourse']['admin_api_username']+"&page="+str(page_val)
            not_ok = True
            while not_ok:
                try:
                    cat_req = requests.get(cat_url)
                except:
                    print('request problem on topics page='+str(page_val))
                    time.sleep(2)
                    continue
                try:
                    cat_json = cat_req.json()
                except:
                    print("failed read on topic page "+str(page_val))
                    time.sleep(2)
                    continue
                try:
                    topics = cat_json['topic_list']['topics']
                except:
                    print("failed read on topic list page "+str(page_val))
                    time.sleep(2)
                    continue
                not_ok = False

            for post in cat_json['topic_list']['topics']:
                try:
                    comment_n = importer.create_posts(post['id'], post['title'])
                except Exception as exc:
                    print("fail creating post "+str(post['id'])+" ("+config['importer_discourse']['abs_path']+config['importer_discourse']['topic_rel_path']+str(post['id'])+".json):"+str(exc))
                    raise exc

            if len(cat_json['topic_list']['topics']) == 30:
                page_val += 1
            else:
                Continue = False   
            #if page_val > 1:
                break

        importer.create_tags()
        importer.create_annotations()

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
