import uuid
import configparser
import os
import time
from flask_restful import Resource, reqparse
from routes.utils import makeResponse
from graphtulip.createtlp import CreateTlp
from graphtulip.createfulltlp import CreateFullTlp
from graphtulip.createusertlp import CreateUserTlp
from graphtulip.createtagtlp import CreateTagTlp
from graphtulip.createtagdatetlp import CreateTagDateTlp
from graphtulip.createtagfocustlp import CreateTagFocusTlp
from graphtulip.createtagfulltlp import CreateTagFullTlp

config = configparser.ConfigParser()
config.read("config.ini")

parser = reqparse.RequestParser()

# Graph generate once


class GenerateFullGraph(Resource):
    def __init__(self, **kwargs):
        self.gid_stack = kwargs['gid_stack']

    def get(self):
        if 'complete' in self.gid_stack.keys():
            os.remove('%s%s.tlp' % (config['exporter']['tlp_path'], self.gid_stack.pop("complete")))
        private_gid = uuid.uuid4().urn[9:]
        creator = CreateFullTlp()
        creator.create(private_gid)
        self.gid_stack.update({"complete": private_gid})
        return makeResponse(True)


class GenerateUserGraph(Resource):
    def __init__(self, **kwargs):
        self.gid_stack = kwargs['gid_stack']

    def get(self):
        if 'usersToUsers' in self.gid_stack.keys():
            os.remove('%s%s.tlp' % (config['exporter']['tlp_path'], self.gid_stack.pop("usersToUsers")))
        private_gid = uuid.uuid4().urn[9:]
        creator = CreateUserTlp()
        creator.create(private_gid)
        self.gid_stack.update({"usersToUsers": private_gid})
        return makeResponse(True)


class GenerateTagGraph(Resource):
    def __init__(self, **kwargs):
        self.gid_stack = kwargs['gid_stack']

    def get(self, value):
        if 'tagToTags' in self.gid_stack.keys():
            os.remove('%s%s.tlp' % (config['exporter']['tlp_path'], self.gid_stack.pop("tagToTags")))
        private_gid = uuid.uuid4().urn[9:]
        creator = CreateTagTlp(value)
        creator.create(private_gid)
        self.gid_stack.update({"tagToTags": private_gid})
        return makeResponse(True)


class GenerateTagDateGraph(Resource):
    def __init__(self, **kwargs):
        self.gid_stack = kwargs['gid_stack']

    def get(self, value, start, end):
        if 'tagToTags' in self.gid_stack.keys():
            os.remove('%s%s.tlp' % (config['exporter']['tlp_path'], self.gid_stack.pop("tagToTags")))
        private_gid = uuid.uuid4().urn[9:]
        creator = CreateTagDateTlp(value, start, end)
        creator.create(private_gid)
        self.gid_stack.update({"tagToTags": private_gid})
        return makeResponse(True)


class GenerateTagFocusGraph(Resource):
    def __init__(self, **kwargs):
        self.gid_stack = kwargs['gid_stack']

    def get(self, value, start, end):
        if 'tagToTags' in self.gid_stack.keys():
            os.remove('%s%s.tlp' % (config['exporter']['tlp_path'], self.gid_stack.pop("tagToTags")))
        private_gid = uuid.uuid4().urn[9:]
        creator = CreateTagFocusTlp(value, start, end)
        creator.create(private_gid)
        self.gid_stack.update({"tagToTags": private_gid})
        return makeResponse(True)


class GenerateTagFullGraph(Resource):
    def __init__(self, **kwargs):
        self.gid_stack = kwargs['gid_stack']

    def get(self, value, start, end, force_fresh):
        if 'tagToTags' in self.gid_stack.keys():
            os.remove('%s%s.tlp' % (config['exporter']['tlp_path'], self.gid_stack.pop("tagToTags")))
        private_gid = uuid.uuid4().urn[9:]
        creator = CreateTagFullTlp(value, start, end, force_fresh)
        creator.create(private_gid)
        self.gid_stack.update({"tagToTags": private_gid})
        return makeResponse(True)


class GenerateGraphWithoutUser(Resource):
    def __init__(self, **kwargs):
        self.gid_stack = kwargs['gid_stack']

    def get(self):
        if 'commentAndPost' in self.gid_stack.keys():
            os.remove('%s%s.tlp' % (config['exporter']['tlp_path'], self.gid_stack.pop("commentAndPost")))
        private_gid = uuid.uuid4().urn[9:]
        creator = CreateTlp()
        creator.createWithout(["user"], private_gid)
        self.gid_stack.update({"commentAndPost": private_gid})
        return makeResponse(True)


class GenerateGraphs(Resource):
    def __init__(self, **kwargs):
        self.gid_stack = kwargs['gid_stack']

    def get(self, returnValue=True):
        # Full graph
        if 'complete' in self.gid_stack.keys():
            os.remove('%s%s.tlp' % (config['exporter']['tlp_path'], self.gid_stack.pop("complete")))
        creator = CreateFullTlp()
        complete_gid = uuid.uuid4().urn[9:]
        creator.create(complete_gid)
        self.gid_stack.update({"complete": complete_gid})
        # User Graph
        if 'usersToUsers' in self.gid_stack.keys():
            os.remove('%s%s.tlp' % (config['exporter']['tlp_path'], self.gid_stack.pop("usersToUsers")))
        creator = CreateUserTlp()
        users_gid = uuid.uuid4().urn[9:]
        creator.create(users_gid)
        self.gid_stack.update({"usersToUsers": users_gid})
        # Comment And Post Graph
        if 'commentAndPost' in self.gid_stack.keys():
            os.remove('%s%s.tlp' % (config['exporter']['tlp_path'], self.gid_stack.pop("commentAndPost")))
        creator = CreateTlp()
        commentPost_gid = uuid.uuid4().urn[9:]
        creator.createWithout(["user"], commentPost_gid)
        self.gid_stack.update({"commentAndPost": commentPost_gid})
        if returnValue:
            return makeResponse(True)


# Create new graph


class CreateGraph(Resource):
    def __init__(self, **kwargs):
        self.gid_stack = kwargs['gid_stack']

    def get(self, field, value):
        public_gid = int(time.time()) + uuid.uuid4().urn[19:]
        print(public_gid)
        private_gid = uuid.uuid4().urn[9:]
        creator = CreateTlp()
        params = [(field, value)]
        creator.createWithParams(params, private_gid)
        checkTlpFiles(self.gid_stack)
        self.gid_stack.update({public_gid: private_gid})
        return makeResponse({'gid': public_gid})


class CreateGraphWithout(Resource):
    def __init__(self, **kwargs):
        self.gid_stack = kwargs['gid_stack']

    def get(self):
        public_gid = int(time.time())
        private_gid = uuid.uuid4().urn[9:]
        creator = CreateTlp()
        parser.add_argument('type', action='append')
        args = parser.parse_args()
        creator.createWithout(args['type'], private_gid)
        checkTlpFiles(self.gid_stack)
        self.gid_stack.update({public_gid: private_gid})
        return makeResponse({'gid': public_gid})


class CreateGraphWithParams(Resource):
    def __init__(self, **kwargs):
        self.gid_stack = kwargs['gid_stack']

    def get(self):
        public_gid = int(time.time())
        private_gid = uuid.uuid4().urn[9:]
        creator = CreateTlp()
        parser.add_argument('uid', action='append')
        parser.add_argument('pid', action='append')
        parser.add_argument('cid', action='append')
        args = parser.parse_args()
        params = []
        if args['uid']:
            for user in args['uid']:
                params.append(('uid', user))
        if args['pid']:
            for post in args['pid']:
                params.append(('pid', post))
        if args['cid']:
            for comment in args['cid']:
                params.append(('cid', comment))
        creator.createWithParams(params, private_gid)
        checkTlpFiles(self.gid_stack)
        self.gid_stack.update({public_gid: private_gid})
        return makeResponse({'gid': public_gid})


def checkTlpFiles(gid_stack):
    if len(gid_stack) > int(config['api']['max_tlp_files']) - 1:
        keys = gid_stack.copy()
        keys.pop('complete')
        keys.pop('usersToUsers')
        keys.pop('commentAndPost')
        min = 9999999999
        min_key = None
        for key in keys:
            if int(key[0:10]) < min:
                min_key = key
                min = int(key[0:10])
        priv = gid_stack.pop(min_key)
        os.remove('%s%s.tlp' % (config['exporter']['tlp_path'], priv))
