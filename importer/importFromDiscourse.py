import configparser
import json
import time
import string
from datetime import datetime
from py2neo import *
from connector.neo4j import query_neo4j
from neo4j.v1 import ResultError
import requests
import sys
import re
from tulip import *

config = configparser.ConfigParser()
config.read("config.ini")

def cleanString(s):
#    s=s.replace("\n", "<br>")
    try:
        s=s.replace("\r", "")
    except Exception as inst:
        print(inst)
#    return s.replace("\\","")
    return s

def clean_html(raw_html):
    cleanr = re.compile('<.*?>')
    cleantext = re.sub(cleanr, '', raw_html)
    return cleantext

class ImportFromDiscourse(object):
    verbose = False
    unmatch_post_user = 0
    unmatch_comment_user = 0
    unmatch_comment_post = 0
    unmatch_comment_parent = 0
    unmatch_tag_parent = 0
    unmatch_annotation_user = 0
    unmatch_annotation_tag = 0
    unmatch_annotation_entity = 0
    
    def __init__(self, erase=False, debug=False):
        super(ImportFromDiscourse, self).__init__()
        print('Initializing')
        self.neo4j_graph = Graph(host=config['neo4j']['url'], user=config['neo4j']['user'], password=config['neo4j']['password'])
        if erase:
            self.neo4j_graph.delete_all()
        ImportFromDiscourse.verbose=debug
        self.tags = {}
        self.node_tulip = {'users': {}, 'posts': {}, 'comments': {}, 'annotations': {}, 'tags': {}}
        self.graph = tlp.newGraph()
        self.max_tag_id = 0
        self.unavailable_users_id = []
        self.unavailable_posts_id = []
        self.unavailable_comments_id = []
        self.unavailable_tags_id = []


    def createUser(self, id, label, avatar):
        user_node = Node('user')
        user_node['user_id'] = id
        user_node['label'] = cleanString(label)
        user_node['avatar'] = avatar
        self.neo4j_graph.merge(user_node)

        idp = self.graph.getIntegerProperty('id')
        labelp = self.graph.getStringProperty('label')
        typep = self.graph.getStringProperty('type')
        avatarp = self.graph.getStringProperty('avatar')
        n = self.graph.addNode()
        idp[n] = id
        labelp[n] = label
        typep[n] = 'user'
        avatarp[n] = avatar
        return n


    def create_users(self, json_users):
        query_neo4j("CREATE CONSTRAINT ON (n:user) ASSERT n.user_id IS UNIQUE")


    def createContent(self, id, type, label, content, timestamp):
        content_node = Node(type)
        content_node[type+'_id'] = id
        content_node['label'] = cleanString(label)
        content_node['title'] = cleanString(label)
        content_node['content'] = cleanString(content)
        timestamp = (timestamp[0:23]+'000')
        content_node['timestamp'] = int(time.mktime(datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%f").timetuple()) * 1000)
        try:
            self.neo4j_graph.merge(content_node)
        except ConstraintError:
            if ImportFromDiscourse.verbose:
                print("WARNING: "+type+" id "+str(content_node[type+'_id'])+" already exists")
        idp = self.graph.getIntegerProperty('id')
        labelp = self.graph.getStringProperty('label')
        contentp = self.graph.getStringProperty('content')
        typep = self.graph.getStringProperty('type')
        timestampp = self.graph.getIntegerProperty('timestamp')
        n = self.graph.addNode()
        idp[n] = id
        labelp[n] = label
        contentp[n] = content
        typep[n] = type
        timestamp = (timestamp[0:23]+'000')
        timestampp[n] = int(time.mktime(datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%f").timetuple()) * 1000)
        return n


    def create_posts(self, id, title):
        query_neo4j("CREATE CONSTRAINT ON (p:post) ASSERT p.post_id IS UNIQUE")
        ImportFromDiscourse.unmatch_post_user = 0
        ImportFromDiscourse.unmatch_comment_post += 1
        idp = self.graph.getIntegerProperty('id')
        labelp = self.graph.getStringProperty('label')
        typep = self.graph.getStringProperty('type')

        # get list of posts from topic
        post_url = config['importer_discourse']['abs_path']+config['importer_discourse']['topic_rel_path']+str(id)+".json?api_key="+config['importer_discourse']['admin_api_key']+"&api_username="+config['importer_discourse']['admin_api_username']
        not_ok = True
        while not_ok:
            try:
                post_req = requests.get(post_url)
            except:
                print('request problem on topic '+str(id))
                time.sleep(2)
                continue
            try:
                post_json = post_req.json()
            except:
                print("failed read on topic "+str(id))
                post_json = []
                time.sleep(2)
                continue
            not_ok = False
        edgeToCreate = []
        commentList = {}

        i = 0
        # create all elements
        for comment_id in post_json['post_stream']['stream']:
            #print(str(len(post_json['post_stream']['stream'])) +' : '+str(i)+' '+str(comment_id))
            if i >= len(post_json['post_stream']['posts']):
            # if comment resume is unavailable (not one of the first 20 posts)
                comment_url = config['importer_discourse']['abs_path']+config['importer_discourse']['posts_rel_path']+str(comment_id)+".json?api_key="+config['importer_discourse']['admin_api_key']+"&api_username="+config['importer_discourse']['admin_api_username']
                not_ok = True
                while not_ok:    
                    try:
                        comment_req = requests.get(comment_url)
                    except:
                        print('request problem on post '+str(comment_id))
                        time.sleep(2)
                        continue
                    try:
                        comment = comment_req.json()
                    except:
                        print("failed read on post "+str(comment_id))
                        time.sleep(2)
                        continue
                    not_ok = False
    #            time.sleep(1)
            else:
            # else get available resume
                comment = post_json['post_stream']['posts'][i]

            commentList[comment['post_number']] = comment['id']
            if i == 0:
            # first 'comment' of the topic is the main post
                type = 'post'
                post_n = self.createContent(comment['id'], type, title, comment['cooked'], comment['created_at'])
                self.node_tulip['posts'][comment['id']] = post_n
                self.node_tulip['comments'][comment['id']] = post_n
                comment_n = post_n
                id = comment['id']

            else:
            # check as a comment
                type = 'comment'
                # extract the title
                tmp = comment['cooked'].split('</b></p>\n\n')
                if len(tmp) > 1:
                    label = tmp[0][6:]
                    content = "<p>"+tmp[1]
                else:
                    tmp = clean_html(comment['cooked']).split(" ")
                    label = ""
                    for j in range(min(8, len(tmp))):
                        label += tmp[j] + " "
                    content = comment['cooked']
                content = content.replace('href=\\"//', 'href=\\"https://')
                content = content.replace('href=\\"/', 'href=\\"'+config['importer_discourse']['abs_path'])
                content = content.replace('src=\\"//', 'href=\\"https://')
                content = content.replace('src=\\"/', 'href=\\"'+config['importer_discourse']['abs_path'])

                comment_n = self.createContent(comment['id'], type, label, content, comment['created_at'])
                self.node_tulip['comments'][comment['id']] = comment_n
            
                # response to a comment
                if not(comment['reply_to_post_number'] is None):
                    #reply_id = comment['reply_to_post_number']
                    #print("post " + str(id) + " reply from " + str(comment['id']) + " to "+ str( post_json['post_stream']['stream'][reply_id]))
                    edgeToCreate.append([comment['id'], comment['reply_to_post_number']])
                else:
                # direct response to post
                    edgeToCreate.append([comment['id'], 1])

            # link with author
            if not(comment['user_id'] in self.node_tulip['users']):
                user_n = self.createUser(comment['user_id'], comment['username'], comment['avatar_template'])
                self.node_tulip['users'][comment['user_id']] = user_n
            self.graph.addEdge(self.node_tulip['users'][comment['user_id']], comment_n)
            try :
                req = "MATCH (e:%s { %s_id : %d })" % (type, type, comment['id'])
                req += " MATCH (u:user { user_id : %s })" % comment['user_id']
                req += " CREATE UNIQUE (u)-[:AUTHORSHIP]->(e) RETURN u"
                query_neo4j(req).single()
            except ResultError:
                if ImportFromDiscourse.verbose:
                    print("WARNING : %s id %d has no author user_id %s" % (type, comment['id'], comment['user_id']))
                ImportFromDiscourse.unmatch_post_user+=1
                query_neo4j("MATCH (p:%s {%s_id : %s}) DETACH DELETE p" % (type, type, comment['id']))
                self.unavailable_users_id.append(comment['user_id'])

            # build timetree
            if 'timestamp' in comment:
            # TimeTree
                timestamp = (comment['timestamp'][0:23]+'000')
                timestamp = int(time.mktime(datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%f").timetuple()) * 1000)
                req = "MATCH (p:%s { %s_id : %d }) WITH p " % (type, type, post_node['post_id'])
                req += "CALL ga.timetree.events.attach({node: p, time: %s, relationshipType: 'POST_ON'}) " % timestamp
                req += "YIELD node RETURN p"
                query_neo4j(req)

            # link betwixt comment and post
            if i > 0:
                self.graph.addEdge(self.node_tulip['comments'][comment['id']], self.node_tulip['comments'][commentList[1]])
                try:
                    req = "MATCH (c:comment { comment_id : %d }) " % comment['id']
                    req += "MATCH (p:post { post_id : %s }) " % commentList[1]
                    req += "CREATE UNIQUE (c)-[:COMMENTS]->(p) RETURN p"
                    query_neo4j(req).single()
                except ResultError:
                    if ImportFromDiscourse.verbose:
                        print("WARNING : comment %d has no post parent %s" % (comment['id'], commentList[1]))
                    ImportFromDiscourse.unmatch_comment_post+=1
                    query_neo4j("MATCH (c:comment {comment_id : %s}) DETACH DELETE c" % comment['id'])

            i+=1

        # add edges between comments
        for e in edgeToCreate:
            if not(e[1] in commentList):
            # ignore bad mapping and link back to root post instead
                print("bad mapping: from "+str(e[0])+" to "+str(e[1])+" for thread "+str(id)+" ("+title+") and post "+str(comment_id))
                e[1] = 1
            if e[1] == 1:
                continue

            try:
                req = "MATCH (c1:comment { comment_id : %d }) " % e[0]
                req += "MATCH (c2:comment { comment_id : %d }) " % commentList[e[1]]
                req += "CREATE UNIQUE (c1)-[:COMMENTS]->(c2) RETURN c2"
                query_neo4j(req).single()
            except ResultError:
                if ImportFromDiscourse.verbose:
                    print("WARNING : comment %d has no parent %d" % (e[0], commentList[e[1]]))
                query_neo4j("MATCH (c:comment {comment_id : %s}) DETACH DELETE c" % commentList[e[1]])
                ImportFromDiscourse.unmatch_comment_post+=1
                if e[1] not in self.unavailable_posts_id:
                    self.unavailable_posts_id.append(e[1])


    def createTag(self, id, label):
        tag_node = Node('tag')
        tag_node['tag_id'] = id
        tag_node['label'] = cleanString(label)
        tag_node['name'] = cleanString(label)
        try:
            self.neo4j_graph.merge(tag_node)
        except ConstraintError:
            if ImportFromDiscourse.verbose:
                print("WARNING: tag id "+str(tag_node['tag_id'])+" already exists")
        idp = self.graph.getIntegerProperty('id')
        labelp = self.graph.getStringProperty('label')
        typep = self.graph.getStringProperty('type')
        n = self.graph.addNode()
        idp[n] = id
        labelp[n] = label
        typep[n] = 'tag'
        return n


    def create_tags(self):
        query_neo4j("CREATE CONSTRAINT ON (t:tag) ASSERT t.tag_id IS UNIQUE")
        print('Import tags')
        Continue = True
        page_val = 0
        self.max_tag_id = 0
        while Continue:
            tag_url = config['importer_discourse']['abs_path']+config['importer_discourse']['codes_rel_path']+".json?api_key="+config['importer_discourse']['admin_api_key']+"&api_username="+config['importer_discourse']['admin_api_username']+"&per_page=1000&page="+str(page_val)
            not_ok = True
            while not_ok:
                try:
                    tag_req = requests.get(tag_url)
                except:
                    print('request problem on tag page '+str(page_val))
                    time.sleep(2)
                    continue
                try:
                    tag_json = tag_req.json()
                except:
                    print("failed read tag on page "+str(page_val))
                    time.sleep(2)
                    continue
                not_ok = False

            # get all tags
            for tag in tag_json:
                # create tag if not existing
                if not(tag['id'] in self.node_tulip['tags']):
                    if not(tag['name'].lower() in self.tags):
                        tag_n = self.createTag(tag['id'], tag['name'].lower())
                    else:
                        tag_n = self.node_tulip['tags'][self.tags[tag['name'].lower()]]
                    self.node_tulip['tags'][tag['id']] = tag_n
                    self.tags[tag['name'].lower()] = tag['id']
                    self.max_tag_id = max(self.max_tag_id, tag['id']+1)
                # no need to create tag hierarchy as the route does not give ancestry info
            
            if len(tag_json) == 1000:
                page_val += 1
            else:
                Continue = False
                break


    def createAnnotation(self, id, quote, timestamp):
        annotation_node = Node('annotation')
        annotation_node['annotation_id'] = id
        annotation_node['quote'] = cleanString(quote)
        timestamp = (timestamp[0:23]+'000')
        annotation_node['timestamp'] = int(time.mktime(datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%f").timetuple()) * 1000)
        self.neo4j_graph.merge(annotation_node)

        idp = self.graph.getIntegerProperty('id')
        quotep = self.graph.getStringProperty('quote')
        typep = self.graph.getStringProperty('type')
        timestampp = self.graph.getIntegerProperty('timestamp')
        n = self.graph.addNode()
        idp[n] = id
        quotep[n] = quote
        typep[n] = 'annotation'
        timestamp = (timestamp[0:23]+'000')
        timestampp[n] = int(time.mktime(datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%f").timetuple()) * 1000)
        return n


    def create_annotations(self):
        query_neo4j("CREATE CONSTRAINT ON (a:annotation) ASSERT a.annotation_id IS UNIQUE")
        print('Import annotations')
        #json_annotations = json.load(open(config['importer']['json_annotations_path']))
        ImportFromDiscourse.unmatch_annotation_user = 0
        ImportFromDiscourse.unmatch_annotation_tag = 0 
        ImportFromDiscourse.unmatch_annotation_entity = 0
        Continue = True
        page_val = 0
        while Continue:
        # get pages of 1000 annotations
            ann_url = config['importer_discourse']['abs_path']+config['importer_discourse']['annotations_rel_path']+".json?api_key="+config['importer_discourse']['admin_api_key']+"&api_username="+config['importer_discourse']['admin_api_username']+"&per_page=1000&page="+str(page_val)
            not_ok = True
            while not_ok:
                try:
                    ann_req = requests.get(ann_url)
                except:
                    print('request problem on annotation page '+str(page_val))
                    time.sleep(2)
                    continue
                try:
                    ann_json = ann_req.json()
                except:
                    print("failed read annotation on page "+str(page_val))
                    time.sleep(2)
                    continue
                not_ok = False

            # get all annotations
            for annotation in ann_json:
                # only select annotations which link to existing posts and tags
                if annotation['post_id'] in self.node_tulip['comments']:
                    if not(annotation['tag_id'] in self.node_tulip['tags']):
                        if annotation['tag_id'] != None:
                            self.unavailable_tags_id.append(str(annotation['tag_id']))
                            #print("Missing tag "+str(annotation['tag_id'])+" for annotation "+str(annotation['id'])+" on registered post "+str(annotation['post_id']))
                            continue
                        else:
                            print("Missing tag "+str(annotation['quote'])+" for annotation "+str(annotation['id'])+" on registered post "+str(annotation['post_id']))
                        if not(annotation['quote'].lower() in self.tags):
                            tag_n = self.createTag(max_tag_id, annotation['quote'].lower())
                            node_tulip['tags'][max_tag_id] = tag_n
                            tags[annotation['quote'].lower()] = max_tag_id
                            max_tag_id+=1
                        annotation['tag_id']=self.tags[annotation['quote'].lower()]


                    annotation_n = self.createAnnotation(annotation['id'], annotation['quote'], annotation['created_at'])
                    # link annotation to tag
                    self.graph.addEdge(annotation_n, self.node_tulip['tags'][annotation['tag_id']])
                    try:
                        req = "MATCH (a:annotation { annotation_id : %d }) " % annotation['id']
                        req += "MATCH (t:tag { tag_id : %s }) " % annotation['tag_id']
                        req += "CREATE UNIQUE (a)-[:REFERS_TO]->(t) RETURN t"
                        query_neo4j(req).single()
                    except ResultError:
                        if ImportFromDiscourse.verbose:
                            print("WARNING : annotation %d has no corresponding tag %s" % (annotation['id'], annotation['tag_id']))
                        ImportFromDiscourse.unmatch_annotation_tag +=1
                        query_neo4j("MATCH (a:annotation {annotation_id : %s}) DETACH DELETE a" % annotation['id'])
                    # link to content
                    type = 'comment'
                    if  annotation['post_id'] in self.node_tulip['posts']:
                        type = 'post'
                    self.graph.addEdge(annotation_n, self.node_tulip['comments'][annotation['post_id']])
                    try:
                        req = "MATCH (a:annotation { annotation_id : %d }) " % annotation['id']
                        req += "MATCH (e:%s { %s_id : %s }) " % (type, type, annotation['post_id'])
                        req += "CREATE UNIQUE (a)-[:ANNOTATES]->(e) RETURN e"
                        query_neo4j(req).single()
                    except ResultError:
                        if ImportFromDiscourse.verbose:
                            print("WARNING : annotation %d has no corresponding %s id %s" % (annotation['id'], type, annotation['post_id']))
                        ImportFromDiscourse.unmatch_annotation_entity +=1
                        query_neo4j("MATCH (a:annotation {annotation_id : %s}) DETACH DELETE a" % annotation['id'])
                    # link to creator
                    if annotation['creator_id'] in self.node_tulip['users']:
                        self.graph.addEdge(annotation_n, self.node_tulip['users'][annotation['creator_id']])
                        try:
                            req = "MATCH (a:annotation { annotation_id : %d }) " % annotation['id']
                            req += "MATCH (u:user { user_id : %s }) " % annotation['creator_id']
                            req += "CREATE UNIQUE (u)-[:AUTHORSHIP]->(a) RETURN u"
                            query_neo4j(req).single()
                        except ResultError:
                            if ImportFromDiscourse.verbose:
                                print("WARNING : annotation id %d has no author id %s" % (annotation['id'], annotation['creator_id']))
                            ImportFromDiscourse.unmatch_annotation_user+=1
                            query_neo4j("MATCH (a:annotation {annotation_id : %s}) DETACH DELETE a" % annotation['id'])
                            if annotation['creator_id'] not in self.unavailable_users_id:
                                self.unavailable_users_id.append(annotation['creator_id'])
                    else:
                        self.unavailable_users_id.append(str(annotation['creator_id']))
                        #print("Unknown creator "+str(annotation['creator_id'])+" for annotation "+str(annotation['id'])+" on registered post "+str(annotation['post_id']))

            if len(ann_json) == 1000:
                page_val += 1
            else:
                Continue = False
                break


    def end_import(self):

        #tlp.saveGraph(self.graph, "/usr/src/myapp/discourse.tlpb")
        response = {'users': self.unavailable_users_id, "posts": self.unavailable_posts_id, 'comments': self.unavailable_comments_id, "tags":  self.unavailable_tags_id}
        print(response)
        print(" unmatch post -> (user): ", ImportFromDiscourse.unmatch_post_user,"\n",
        "unmatch comment -> (user): ", ImportFromDiscourse.unmatch_comment_user,"\n",
        "unmatch comment -> (post): ", ImportFromDiscourse.unmatch_comment_post,"\n",
        "unmatch comment -> (parent): ", ImportFromDiscourse.unmatch_comment_parent,"\n",
        "unmatch tag -> (parent): ", ImportFromDiscourse.unmatch_tag_parent,"\n",
        "unmatch annotation -> (user): ", ImportFromDiscourse.unmatch_annotation_user,"\n",
        "unmatch annotation -> (tag): ", ImportFromDiscourse.unmatch_annotation_tag,"\n", 
        "unmatch annotation -> (entity): ", ImportFromDiscourse.unmatch_annotation_entity,"\n")
        return response

