import configparser
import json
import time
import string
from datetime import datetime
from py2neo import *
from connector.neo4j import query_neo4j
from neo4j.v1 import ResultError

config = configparser.ConfigParser()
config.read("config.ini")

def cleanString(s):
#    s=s.replace("\n", "<br>")
    s=s.replace("\r", "")
    #s=s.replace("\u", "u")
#    return s.replace("\\","")
    return s

class ImportFromJson(object):
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
        super(ImportFromJson, self).__init__()
        print('Initializing')
        self.neo4j_graph = Graph(host=config['neo4j']['url'], user=config['neo4j']['user'], password=config['neo4j']['password'])
        if erase:
            self.neo4j_graph.delete_all()
        # else:
            # todo ask neo4j for is data version (last_uid last_pid last_cid)
        ImportFromJson.verbose=debug
        self.unavailable_users_id = []
        self.unavailable_posts_id = []
        self.unavailable_comments_id = []
        self.unavailable_tags_id = []
        self.unavailable_annotations_id = []

    def create_users(self, json_users):
        query_neo4j("CREATE CONSTRAINT ON (n:user) ASSERT n.user_id IS UNIQUE")
        query_neo4j("CREATE CONSTRAINT ON (l:language) ASSERT l.name IS UNIQUE")
        query_neo4j("CREATE CONSTRAINT ON (r:role) ASSERT r.name IS UNIQUE")
        print('Import users')
        #json_users = json.load(open(config['importer']['json_users_path']))
        for user_entry in json_users['nodes']:
            user_node = Node('user')
            user_fields = user_entry['node']
            user_node['user_id'] = int(user_fields['user_id'])
            if user_fields['label']:
                user_node['label'] = cleanString(user_fields['label'])
            if user_fields['name']:
                user_node['name'] = cleanString(user_fields['name'])
            if user_fields['first_name']:
                user_node['first_name'] = cleanString(user_fields['first_name'])
            if user_fields['last_name']:
                user_node['last_name'] = cleanString(user_fields['last_name'])
            if user_fields['age']:
                user_node['age'] = cleanString(user_fields['age'])
            if user_fields['location']:
                user_node['location'] = cleanString(user_fields['location'])
            if user_fields['biography']:
                user_node['biography'] = cleanString(user_fields['biography'])
            if user_fields['active']:
                user_node['active'] = cleanString(user_fields['active'])
            if user_fields['creation_date']:
                user_node['timestamp'] = int(time.mktime(datetime.strptime(user_fields['creation_date'], "%A, %B %d, %Y - %H:%M").timetuple())* 1000)
            if user_fields['email']:
                # user_node['email'] = user_fields['Email']
                user_node['email'] = "nomail@nomail.com"
            if user_fields['group_membership']:
                user_node['group_membership'] = cleanString(user_fields['group_membership']) # not well structure to be relation
            if user_fields['url_website']:
                user_node['url_website'] = cleanString(user_fields['url_website'])
            if user_fields['url_facebook']:
                user_node['url_facebook'] = cleanString(user_fields['url_facebook'])
            if user_fields['url_linkedin']:
                user_node['url_linkedin'] = cleanString(user_fields['url_linkedin'])
            if user_fields['url_twitter']:
                user_node['url_twitter'] = cleanString(user_fields['url_twitter'])

            try:
                self.neo4j_graph.merge(user_node)
            except ConstraintError:
                if ImportFromJson.verbose:
                    print("WARNING: User uid %s already exists" % user_node['user_id'] )

            # Add relation
            # Language
            if user_fields['language']:
                req = "MATCH (u:user { user_id : %d }) " % user_node['user_id']
                req += "MERGE (l:language { name : '%s'}) " % user_fields['language']
                req += "CREATE UNIQUE (u)-[:LANGUAGE_IS]->(l)"
                query_neo4j(req)

            # Role
            if user_fields['role']:
                for role in user_fields['role'].split(','):
                    req = "MATCH (u:user { user_id : %d }) " % user_node['user_id']
                    req += "MERGE (r:role { name : '%s'}) " %role
                    req += "CREATE UNIQUE (u)-[:ROLE_IS]->(r)"
                    query_neo4j(req)

            # link to TimeTree
            if user_fields['creation_date']:
                timestamp = int(time.mktime(datetime.strptime(user_fields['creation_date'], "%A, %B %d, %Y - %H:%M").timetuple())* 1000)
                req = "MATCH (u:user { user_id : %d }) WITH u " % user_node['user_id']
                req += "CALL ga.timetree.events.attach({node: u, time: %s, relationshipType: 'CREATED_ON'}) " % timestamp
                req += "YIELD node RETURN u"
                query_neo4j(req)


    def create_posts(self, json_posts):
        query_neo4j("CREATE CONSTRAINT ON (p:post) ASSERT p.post_id IS UNIQUE")
        print('Import posts')
        #json_posts = json.load(open(config['importer']['json_posts_path']))
        ImportFromJson.unmatch_post_user = 0
        for post_entry in json_posts['nodes']:
            post_node = Node('post')
            post_fields = post_entry['node']
            post_node['post_id'] = int(post_fields['post_id'])
            if post_fields['label']:
                post_node['label'] = cleanString(post_fields['label'])
            if post_fields['title']:
                post_node['title'] = cleanString(post_fields['title'])
            if post_fields['content']:
                post_node['content'] = cleanString(post_fields['content'])
            if post_fields['creation_date']:
                tmp_date = post_fields['creation_date'].split(" ")
                post_node['timestamp'] = int(time.mktime(datetime.strptime(tmp_date[0]+' '+tmp_date[1]+' '+tmp_date[2], "%a, %Y-%m-%d %H:%M").timetuple()) * 1000)
                #print("Date ("+post_fields['creation_date']+ ") is uncompatbile when split in: "+post_fields['creation_date'][:21])
                #post_node['timestamp'] = int(time.mktime(datetime.strptime(post_fields['creation_date'], "%A, %B %d, %Y - %H:%M").timetuple()) * 1000)
            try:
                self.neo4j_graph.merge(post_node)
            except ConstraintError:
                if ImportFromJson.verbose:
                    print("WARNING: Post pid %s already exists" % post_node['post_id'] )

            # Add relation
            # Type
            if post_fields['type']:
                req = "MATCH (p:post { post_id : %d })" % post_node['post_id']
                req += "MERGE (pt:post_type { name : '%s'})" % post_fields['type']
                req += "CREATE UNIQUE (p)-[:TYPE_IS]->(pt)"
                query_neo4j(req)

            # Type
            if post_fields['group_id']:
                req = "MATCH (p:post { post_id : %d })" % post_node['post_id']
                req += " MERGE (n:group { group_id : '%s'})" % post_fields['group_id']
                req += " CREATE UNIQUE (p)-[:GROUP_IS]->(n)"
                query_neo4j(req)

            # Author
            if post_fields['user_id']:
                try:
                    req = "MATCH (u:user { user_id : %s }) RETURN u" % post_fields['user_id']
                    query_neo4j(req).single()
                except ResultError:
                    if ImportFromJson.verbose:
                        print("WARNING : post post_id : %d has no author user_id : %s. Creating one." % (post_node['post_id'], post_fields['user_id']))

                    user_node = Node('user')
                    user_node['user_id'] = int(post_fields['user_id'])
                    if post_fields['user_name']:
                        user_node['label'] = cleanString(post_fields['user_name'])
                        user_node['name'] = cleanString(post_fields['user_name'])
                    self.neo4j_graph.merge(user_node)

                try :
                    req = "MATCH (p:post { post_id : %d })" % post_node['post_id']
                    req += " MATCH (u:user { user_id : %s })" % post_fields['user_id']
                    req += " CREATE UNIQUE (u)-[:AUTHORSHIP]->(p) RETURN u"
                    query_neo4j(req).single()
                except ResultError:
                    if ImportFromJson.verbose:
                        print("WARNING : post post_id : %d has no author user_id : %s" % (post_node['post_id'], post_fields['user_id']))
                    ImportFromJson.unmatch_post_user+=1
                    query_neo4j("MATCH (p:post {post_id : %s}) DETACH DELETE p" % post_node['post_id'])
                    self.unavailable_users_id.append(post_fields['user_id'])

            # TimeTree
            if post_fields['creation_date']:
                timestamp = int(time.mktime(datetime.strptime(tmp_date[0]+' '+tmp_date[1]+' '+tmp_date[2], "%a, %Y-%m-%d %H:%M").timetuple()) * 1000)
                req = "MATCH (p:post { post_id : %d }) WITH p " % post_node['post_id']
                req += "CALL ga.timetree.events.attach({node: p, time: %s, relationshipType: 'POST_ON'}) " % timestamp
                req += "YIELD node RETURN p"
                query_neo4j(req)

    def create_comments(self, json_comments):
        query_neo4j("CREATE CONSTRAINT ON (c:comment) ASSERT c.comment_id IS UNIQUE")
        print('Import comments')
        #json_comments = json.load(open(config['importer']['json_comments_path']))
        ImportFromJson.unmatch_comment_user = 0
        ImportFromJson.unmatch_comment_post = 0
        ImportFromJson.unmatch_comment_parent = 0
        for comment_entry in json_comments['nodes']:
            comment_node = Node('comment')
            comment_fields = comment_entry['node']
            comment_node['comment_id'] = int(comment_fields['comment_id'])
            if comment_fields['label']:
                comment_node['label'] = cleanString(comment_fields['label'])
            if comment_fields['title']:
                comment_node['title'] = cleanString(comment_fields['title'])
            if comment_fields['content']:
                comment_node['content'] = cleanString(comment_fields['content'])
            if comment_fields['creation_date']:
                comment_node['timestamp'] = int(time.mktime(datetime.strptime(comment_fields['creation_date'], "%A, %B %d, %Y - %H:%M").timetuple()) * 1000)
            try:
                self.neo4j_graph.merge(comment_node)
            except ConstraintError:
                if ImportFromJson.verbose:
                    print("WARNING: Comment cid %s already exists" % comment_node['comment_id'] )

            # Add relation
            # Language
            #if comment_fields['language']: # todo repare
            #    req = "MATCH (c:comment { comment_id : %d })" % comment_node['comment_id']
            #    req += " MERGE (l:language { name : '%s'})" % comment_fields['language']
            #    req += " CREATE UNIQUE (u)-[:WRITE_IN]->(l)"
            #    query_neo4j(req)

            # ParentAuthor
            if comment_fields['user_id']:
                try:
                    req = "MATCH (u:user { user_id : %s }) RETURN u" % comment_fields['user_id']
                    query_neo4j(req).single()
                except ResultError:
                    if ImportFromJson.verbose:
                        print("WARNING : comment comment_id : %d has no author user_id : %s. Creating one." % (comment_node['comment_id'], comment_fields['user_id']))

                    user_node = Node('user')
                    user_node['user_id'] = int(comment_fields['user_id'])
                    if comment_fields['user_name']:
                        user_node['label'] = cleanString(comment_fields['user_name'])
                        user_node['name'] = cleanString(comment_fields['user_name'])
                    self.neo4j_graph.merge(user_node)

                try:
                    req = "MATCH (c:comment { comment_id : %d }) " % comment_node['comment_id']
                    req += "MATCH (u:user { user_id : %s }) " % comment_fields['user_id']
                    req += "CREATE UNIQUE (u)-[:AUTHORSHIP]->(c) RETURN u"
                    query_neo4j(req).single()
                except ResultError:
                    if ImportFromJson.verbose:
                        print("WARNING : comment cid : %d has no author uid : %s" % (comment_node['comment_id'], comment_fields['user_id']))
                    ImportFromJson.unmatch_comment_user+=1
                    query_neo4j("MATCH (c:comment {comment_id : %s}) DETACH DELETE c" % comment_node['comment_id'])
                    if comment_fields['user_id'] not in self.unavailable_users_id:
                        self.unavailable_users_id.append(comment_fields['user_id'])
            # ParentPost
            if comment_fields['post_id']:
                try:
                    req = "MATCH (c:comment { comment_id : %d }) " % comment_node['comment_id']
                    req += "MATCH (p:post { post_id : %s }) " % comment_fields['post_id'].replace(",", "")
                    req += "CREATE UNIQUE (c)-[:COMMENTS]->(p) RETURN p"
                    query_neo4j(req).single()
                except ResultError:
                    if ImportFromJson.verbose:
                        print("WARNING : comment cid : %d has no post parent pid : %s" % (comment_node['comment_id'], comment_fields['post_id'].replace(",", "")))
                    ImportFromJson.unmatch_comment_post+=1
                    query_neo4j("MATCH (c:comment {comment_id : %s}) DETACH DELETE c" % comment_node['comment_id'])
                    if comment_fields['post_id'] not in self.unavailable_posts_id:
                        self.unavailable_posts_id.append(comment_fields['post_id'])

            # TimeTree
            if comment_fields['creation_date']:
                timestamp = int(time.mktime(datetime.strptime(comment_fields['creation_date'], "%A, %B %d, %Y - %H:%M").timetuple()) * 1000)
                req = "MATCH (c:comment { comment_id : %d }) WITH c " % comment_node['comment_id']
                req += "CALL ga.timetree.events.attach({node: c, time: %s, relationshipType: 'POST_ON'}) " % timestamp
                req += "YIELD node RETURN c"
                query_neo4j(req)

        # ParentComment
        for comment_entry in json_comments['nodes']:
            comment_node = Node('comment')
            comment_fields = comment_entry['node']
            comment_node['comment_id'] = int(comment_fields['comment_id'])
            if comment_fields['parent_comment_id'] and comment_fields['parent_comment_id'] != str(0):
                try:
                    req = "MATCH (c:comment { comment_id : %d }) " % comment_node['comment_id']
                    req += "MATCH (parent:comment { comment_id : %d }) " % int(comment_fields['parent_comment_id'])
                    req += "CREATE UNIQUE (c)-[:COMMENTS]->(parent) RETURN parent"
                    query_neo4j(req).single()
                except ResultError:
                    if ImportFromJson.verbose:
                        print("WARNING : comment cid : %d has no comment parent cid : %s. Left attached to post." % (comment_node['comment_id'], comment_fields['parent_comment_id']))
                    #ImportFromJson.unmatch_comment_parent +=1
                    #query_neo4j("MATCH (c:comment {comment_id : %s}) DETACH DELETE c" % comment_node['comment_id'])
                    #if comment_fields['parent_comment_id'] not in self.unavailable_comments_id:
                    #    self.unavailable_comments_id.append(comment_fields['parent_comment_id'])

    def create_tags(self, json_tags):
        query_neo4j("CREATE CONSTRAINT ON (t:tag) ASSERT t.tag_id IS UNIQUE")
        print('Import tags')
        #json_tags = json.load(open(config['importer']['json_tags_path']))
        ImportFromJson.unmatch_tag_parent = 0
        for tag_entry in json_tags['nodes']:
            tag_node = Node('tag')
            tag_fields = tag_entry['node']
            tag_node['tag_id'] = int(tag_fields['tag_id'])
            if tag_fields['label']:
                tag_node['label'] = cleanString(tag_fields['label'])
            if tag_fields['name']:
                tag_node['name'] = cleanString(tag_fields['name'])
            try:
                self.neo4j_graph.merge(tag_node)
            except ConstraintError:
                if ImportFromJson.verbose:
                    print("WARNING: Tag tid %s already exists" % tag_node['tag_id'] )

        # ParentTags
        for tag_entry in json_tags['nodes']:
            tag_node = Node('tag')
            tag_fields = tag_entry['node']
            tag_node['tag_id'] = int(tag_fields['tag_id'])
            if tag_fields['parent_tag_id'] and tag_fields['parent_tag_id'] != str(0):
                try:
                    req = "MATCH (t:tag { tag_id : %d }) " % tag_node['tag_id']
                    req += "MATCH (parent:tag { tag_id : %d }) " % int(tag_fields['parent_tag_id'])
                    req += "CREATE UNIQUE (t)-[:IS_CHILD]->(parent) RETURN parent"
                    query_neo4j(req).single()
                except ResultError:
                    if ImportFromJson.verbose:
                        print("WARNING : tag tid : %d has no tag parent tid : %s" % (tag_node['tag_id'], tag_fields['parent_tag_id']))
                    ImportFromJson.unmatch_tag_parent +=1
                    query_neo4j("MATCH (t:tag {tag_id : %s}) DETACH DELETE t" % tag_node['tag_id'])
                    if tag_fields['parent_tag_id'] not in self.unavailable_tags_id:
                        self.unavailable_tags_id.append(tag_fields['parent_tag_id'])

    def create_annotations(self, json_annotations):
        query_neo4j("CREATE CONSTRAINT ON (a:annotation) ASSERT a.annotation_id IS UNIQUE")
        print('Import annotations')
        #json_annotations = json.load(open(config['importer']['json_annotations_path']))
        ImportFromJson.unmatch_annotation_user = 0
        ImportFromJson.unmatch_annotation_tag = 0
        ImportFromJson.unmatch_annotation_entity = 0
        for annotation_entry in json_annotations['nodes']:
            annotation_node = Node('annotation')
            annotation_fields = annotation_entry['node']
            annotation_node['annotation_id'] = int(annotation_fields['annotation_id'])
            #if annotation_fields['label']:
            #    annotation_node['label'] = annotation_fields['label']
            if annotation_fields['quote']:
                annotation_node['quote'] = cleanString(annotation_fields['quote'])
            if annotation_fields['creation_date']:
                annotation_node['timestamp'] = int(time.mktime(datetime.strptime(annotation_fields['creation_date'], "%A, %B %d, %Y - %H:%M").timetuple()) * 1000)
            try:
                self.neo4j_graph.merge(annotation_node)
            except ConstraintError:
                if ImportFromJson.verbose:
                    print("WARNING: Annotation aid %s already exists" % annotation_node['annotation_id'] )

            to_pass = False
            # Add relation
            # Author
            if annotation_fields['user_id']:
                try:
                    req = "MATCH (u:user { user_id : %s }) RETURN u" % annotation_fields['user_id']
                    query_neo4j(req).single()
                except ResultError:
                    if ImportFromJson.verbose:
                        print("WARNING : Resolved about annotation aid : %d had no author uid : %s" % (annotation_node['annotation_id'], annotation_fields['user_id']))

                    user_node = Node('user')
                    user_node['user_id'] = int(annotation_fields['user_id'])
                    if annotation_fields['user_name']:
                        user_node['label'] = cleanString(annotation_fields['user_name'])
                    if annotation_fields['user_name']:
                        user_node['name'] = cleanString(annotation_fields['user_name'])
                    self.neo4j_graph.merge(user_node)

                try:
                    req = "MATCH (a:annotation { annotation_id : %d }) " % annotation_node['annotation_id']
                    req += "MATCH (u:user { user_id : %s }) " % annotation_fields['user_id']
                    req += "CREATE UNIQUE (u)-[:AUTHORSHIP]->(a) RETURN u"
                    query_neo4j(req).single()
                except ResultError:
                    if ImportFromJson.verbose:
                        print("WARNING : annotation aid : %d has no author uid : %s" % (annotation_node['annotation_id'], annotation_fields['user_id']))
                    ImportFromJson.unmatch_annotation_user+=1
                    query_neo4j("MATCH (a:annotation {annotation_id : %s}) DETACH DELETE a" % annotation_node['annotation_id'])
                    if annotation_fields['user_id'] not in self.unavailable_users_id:
                        self.unavailable_users_id.append(annotation_fields['user_id'])
                    to_pass = True
            # Tag
            if annotation_fields['tag_id'] and not to_pass:
                try:
                    req = "MATCH (a:annotation { annotation_id : %d }) " % annotation_node['annotation_id']
                    req += "MATCH (t:tag { tag_id : %s }) " % annotation_fields['tag_id'].replace(",", "")
                    req += "CREATE UNIQUE (a)-[:REFERS_TO]->(t) RETURN t"
                    query_neo4j(req).single()
                except ResultError:
                    if ImportFromJson.verbose:
                        print("WARNING : annotation aid : %d has no corresponding tag tid : %s" % (annotation_node['annotation_id'], annotation_fields['tag_id'].replace(",", "")))
                    ImportFromJson.unmatch_annotation_tag +=1
                    query_neo4j("MATCH (a:annotation {annotation_id : %s}) DETACH DELETE a" % annotation_node['annotation_id'])
                    if annotation_fields['tag_id'] not in self.unavailable_tags_id:
                        self.unavailable_tags_id.append(annotation_fields['tag_id'])
                    to_pass = True
            # Post/comment
            if annotation_fields['entity_id'] and not to_pass:
                try:
                    req = "MATCH (a:annotation { annotation_id : %d }) " % annotation_node['annotation_id']
                    if str(annotation_fields['entity_type'])==str('comment') :
                        req += "MATCH (e:comment { comment_id : %s }) " % annotation_fields['entity_id'].replace(",", "")
                    else:
                        req += "MATCH (e:post { post_id : %s }) " % annotation_fields['entity_id'].replace(",", "")
                    req += "CREATE UNIQUE (a)-[:ANNOTATES]->(e) RETURN e"
                    query_neo4j(req).single()
                except ResultError:
                    if ImportFromJson.verbose:
                        print("WARNING : annotation aid : %d has no corresponding %s id : %s" % (annotation_node['annotation_id'], annotation_fields['entity_type'], annotation_fields['entity_id'].replace(",", "")))
                    ImportFromJson.unmatch_annotation_entity +=1
                    query_neo4j("MATCH (a:annotation {annotation_id : %s}) DETACH DELETE a" % annotation_node['annotation_id'])
                    if annotation_fields['entity_type'] == "comment":
                        if annotation_fields['entity_id'] not in self.unavailable_comments_id:
                            self.unavailable_comments_id.append(annotation_fields['entity_id'])
                    else:
                        if annotation_fields['entity_id'] not in self.unavailable_posts_id:
                            self.unavailable_posts_id.append(annotation_fields['entity_id'])


    def end_import(self):
        response = {'users': self.unavailable_users_id, "posts": self.unavailable_posts_id, 'comments': self.unavailable_comments_id, "tags":  self.unavailable_tags_id}
        print(response)
        print(" unmatch post -> (user): ", ImportFromJson.unmatch_post_user,"\n",
        "unmatch comment -> (user): ", ImportFromJson.unmatch_comment_user,"\n",
        "unmatch comment -> (post): ", ImportFromJson.unmatch_comment_post,"\n",
        "unmatch comment -> (parent): ", ImportFromJson.unmatch_comment_parent,"\n",
        "unmatch tag -> (parent): ", ImportFromJson.unmatch_tag_parent,"\n",
        "unmatch annotation -> (user): ", ImportFromJson.unmatch_annotation_user,"\n",
        "unmatch annotation -> (tag): ", ImportFromJson.unmatch_annotation_tag,"\n",
        "unmatch annotation -> (entity): ", ImportFromJson.unmatch_annotation_entity,"\n")
        return response
