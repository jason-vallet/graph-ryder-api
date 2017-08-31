from tulip import *
import tulipplugins


class ExportSigma(tlp.ExportModule):
    def __init__(self, context):
        tlp.ExportModule.__init__(self, context)

    # you can add parameters to the plugin here through the following syntax
    # self.add<Type>Parameter("<paramName>", "<paramDoc>", "<paramDefaultValue>")
    # (see documentation of class tlp.WithParameter to see what types of parameters are supported)

    def fileExtension(self):
        return "json"

    def exportGraph(self, os):

        # edges
        os << '{ "edges": ['
        for edge in self.graph.getEdges():
            if edge.id == 0:
                os << '{ '
            else:
                os << ',{ '
            # edge properties
            os << '"source":"%s", ' % self.graph.source(edge).id
            os << '"target":"%s", ' % self.graph.target(edge).id
            for prop in self.graph.getObjectProperties():
                # edge color
                if prop.getName() == "viewColor":
                    colors = prop.getEdgeStringValue(edge)[1:][:-1].split(',')
                    os << '"color":"rgb(%s,%s,%s)", ' % (colors[0], colors[1], colors[2])
                # edge label
                elif prop.getName() == "viewLabel":
                    if prop.getEdgeStringValue(edge):
                        label = prop.getEdgeStringValue(edge).replace('\\', '\\\\')
                        label = label.replace('"', '\\\"')
                        os << '"label":"%s", ' % label
                        # else:
                        # os << '%s"label":"edge%s", %s' % (hr_2t, edge.id, hr_n)
                # other
                elif prop.getEdgeDefaultStringValue() != prop.getEdgeStringValue(edge) \
                        and prop.getEdgeStringValue(edge):
                    value = prop.getEdgeStringValue(edge).replace('\\', '\\\\')
                    value = value.replace('"', '\\\"')
#                        .replace("\n", "\\n")\
#                        .replace("\r", "\\r")\
#                        .replace("\t", "\\t")\
#                        \t \r \n \\ \/ \" \b \f \u
                    os << '"%s":"%s", ' % (prop.getName(), value)
            # sigma id
            os << '"id":"%s" ' % edge.id
            os << ' }'
        os << '], '

        # nodes
        os << '"nodes": ['
        for node in self.graph.getNodes():
            if node.id == 0:
                os << '{ '
            else:
                os << ',{ '
            for prop in self.graph.getObjectProperties():
                # node color
                if prop.getName() == "viewColor":
                    colors = prop.getNodeStringValue(node)[1:].split(',')
                    os << '"color":"rgb(%s,%s,%s)", ' % (colors[0], colors[1], colors[2])
                # node label
                elif prop.getName() == "viewLabel":
                    if prop.getNodeStringValue(node):
                        label = prop.getNodeStringValue(node).replace('\\', '\\\\')
                        label = label.replace('"', '\\\"')
                        os << '"label":"%s", ' % label
                    else:
                        os << '"label":"node%s", ' % node.id
                # node size
                elif prop.getName() == "viewSize":
                    #print(prop.getNodeStringValue(node))
                    size = prop.getNodeStringValue(node)[1:-1].split(',')
                    size = (int(size[0]) + int(size[1])) / 2
                    os << '"size":%s, ' % size
                # node layout
                elif prop.getName() == "viewLayout":
                    coord = prop.getNodeStringValue(node)[1:-1].split(',')
                    os << '"x":%s, ' % coord[0]
                    os << '"y":%s, ' % (float(coord[1]) * (-1))
                # other
                elif prop.getNodeDefaultStringValue() != prop.getNodeStringValue(node) \
                        and prop.getNodeStringValue(node):
                    value = prop.getNodeStringValue(node).replace('\\', '\\\\')
                    value = value.replace('"', '\\\"')
#                        .replace('"', '\\\"')\
#                        .replace("\n", "")\
#                        .replace("\r", "")\
#                        .replace("\t", "")
                    os << '"%s":"%s", ' % (prop.getName(), value)
                # sigma id
            os << '"id":"%s"' % node.id
            os << ' }'
        os << ']} '
        return True


# The line below does the magic to register the plugin to the plugin database
# and updates the GUI to make it accessible through the menus.
tulipplugins.registerPlugin("ExportSigma", "SIGMA JSON Export", "Norbert Feron", "01/06/2016",
                            "Export to sigma.js JSON format", "1.0")
