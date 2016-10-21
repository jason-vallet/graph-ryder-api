from routes.user.user_getter import GetUser, GetUserHydrate, GetUsers
from routes.user.user_aggregations import CountUsersByTimestamp, CountUsers
from routes.user.user_interaction import ShortestPathBetweenUsers


def add_user_routes(api):
    # Getters
    api.add_resource(GetUsers, '/users')
    api.add_resource(GetUser, '/users/<int:user_id>')
    api.add_resource(GetUserHydrate, '/users/hydrate/<int:user_id>')

    # Count
    api.add_resource(CountUsersByTimestamp, '/users/count/timestamp')
    api.add_resource(CountUsers, '/users/count')

    # Work in progress
    api.add_resource(ShortestPathBetweenUsers, '/users/shortestPath/<int:user1_id>/<int:user2_id>/<int:max_hop>')

    # todo GetUserType
    # todo GetUsersByType moderators, ...
