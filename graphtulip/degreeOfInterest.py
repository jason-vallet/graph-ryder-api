import configparser
from tulip import *

config = configparser.ConfigParser()
config.read("config.ini")


class DOIContext(object):
    """docstring for DOIContexte"""

    def __init__(self, graph):
        super(DOIContext, self).__init__()
        self.graph = graph
        self.original_graph = self.graph

        self.API_metric = self.original_graph.getDoubleProperty('API')
        self.API_metric.setAllNodeValue(1.0)
        self.UI_metric = self.original_graph.getDoubleProperty('UI')
        self.UI_metric.setAllNodeValue(1.0)
        self.DOI_metric = self.original_graph.getDoubleProperty('DOI')

    def get_focus_node(self):
        selection = self.original_graph.getBooleanProperty('viewSelection')
        focus = None
        for n in self.original_graph.getNodes():
            if selection[n]:
                focus = n
                break
        return focus

    # todo: search with neo4j the focus
    def get_node(self, node_type, node_id):
        node = None
        prop = self.original_graph.getProperty(node_type)
        for n in self.original_graph.getNodes():
            if prop[n] == str(node_id):
                node = n
                break
        return node

    def set_API(self, double_property):
        for n in self.original_graph.getNodes():
            self.API_metric[n] = double_property[n]

    def compute_DOI(self, focus_node):
        dist = self.original_graph.getIntegerProperty('distance_to_focus')
        tlp.maxDistance(self.original_graph, focus_node, dist, tlp.UNDIRECTED)
        for n in self.original_graph.getNodes():
            self.DOI_metric[n] = self.API_metric[n] + self.UI_metric[n] - dist[n]

    def compute_context_subgraph(self, focus_node, max_size):
        color = self.original_graph.getColorProperty('viewColor')
        # color.setAllNodeValue(tlp.Color(0, 64, 64))
        self.compute_DOI(focus_node)
        color[focus_node] = tlp.Color(255, 255, 255)
        focus_nodeset = set()
        candidates = [focus_node]
        while len(candidates) > 0 and len(focus_nodeset) < max_size:
            c = sorted(candidates, key=lambda x: self.DOI_metric[x]).pop()
            candidates.pop(candidates.index(c))
            focus_nodeset.add(c)
            if c == focus_node:
                color[c] = tlp.Color(0, 175, 255)
            for n in self.original_graph.getInOutNodes(c):
                if not n in focus_nodeset and not n in candidates:
                    candidates.append(n)
        context_subgraph = self.original_graph.inducedSubGraph(focus_nodeset)
        context_subgraph.setName('context')
        return context_subgraph


def createDOI(start_graph, private_gid, node_type, node_id, max_size=20):
    graph = tlp.loadGraph("%s%s.tlp" % (config['exporter']['tlp_path'], start_graph))
    doi = DOIContext(graph)

    dataset = tlp.getDefaultPluginParameters('Degree', graph)
    degree = doi.original_graph.getDoubleProperty('degree')
    graph.applyDoubleAlgorithm('Degree', degree, dataset)

    dataset = tlp.getDefaultPluginParameters('Betweenness Centrality', graph)
    bc = doi.original_graph.getDoubleProperty('betweenness')
    graph.applyDoubleAlgorithm('Betweenness Centrality', bc, dataset)

    doi.set_API(bc)

    # f = doi.get_focus_node()
    f = doi.get_node(node_type, node_id)
    doi.compute_DOI(f)
    context_subgraph = doi.compute_context_subgraph(f, max_size)
    tlp.saveGraph(context_subgraph, "%s%s.tlp" % (config['exporter']['tlp_path'], private_gid))
