from routes.general.general_aggregations import *


def add_general_routes(api):
    api.add_resource(CountByTimestamp, '/count/<string:type>/<int:min>/<int:max>')


