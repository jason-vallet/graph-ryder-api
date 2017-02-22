import uuid
import time
import os
import configparser
from flask_restful import Resource, reqparse
from routes.utils import makeResponse
from graphtulip.degreeOfInterest import createDOI
from routes.tulipr.tulip_create import checkTlpFiles
from graphtulip.createfulltlp import CreateFullTlp
from graphtulip.createPostCommentTagTlp import CreatePostCommentTagTlp
from graphtulip.createNeighbourhood import CreateNeighbourhood

parser = reqparse.RequestParser()

config = configparser.ConfigParser()
config.read("config.ini")

class ComputeDOI(Resource):
    def __init__(self, **kwargs):
        self.gid_stack = kwargs['gid_stack']

    def get(self, graph, type, id):
        parser.add_argument('max_size', type=int)
        args = parser.parse_args()
        public_gid = repr(int(time.time())) + uuid.uuid4().urn[19:]
        private_gid = uuid.uuid4().urn[9:]
        if graph == "complete":
            try:
                private_source = self.gid_stack[graph]
            except KeyError:
                creator = CreateFullTlp()
                pgid = uuid.uuid4().urn[9:]
                creator.create(pgid)
                self.gid_stack.update({"complete": pgid})
                private_source = self.gid_stack[graph]
        else:
            private_source = graph
            if (not os.path.exists("%s%s.tlp" % (config['exporter']['tlp_path'], private_source))):
                creator = CreatePostCommentTagTlp(0, int(time.time())*1000, True)
                creator.create()
        if args['max_size']:
            createDOI(private_source, private_gid, type, id, args['max_size'])
        else:
            createDOI(private_source, private_gid, type, id)
        checkTlpFiles(self.gid_stack)
        self.gid_stack.update({public_gid: private_gid})
        return makeResponse({'gid': public_gid})


class ComputeNeighbours(Resource):
    def __init__(self, **kwargs):
        self.gid_stack = kwargs['gid_stack']

    def get(self, type, id):
        public_gid = repr(int(time.time())) + uuid.uuid4().urn[19:]
        private_gid = uuid.uuid4().urn[9:]
        creator = CreateNeighbourhood(type, id)
        creator.create(private_gid)
        checkTlpFiles(self.gid_stack)
        self.gid_stack.update({public_gid: private_gid})
        return makeResponse({'gid': public_gid})

