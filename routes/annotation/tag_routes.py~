from routes.post.post_getter import *
from routes.post.post_aggregations import *


def add_post_routes(api):
    # Getters
    # Multiple
    api.add_resource(GetPosts, '/posts')
    api.add_resource(GetPostsByType, '/posts/type/<string:post_type>')
    api.add_resource(GetPostsByAuthor, '/posts/author/<int:author_id>')
    # Single
    api.add_resource(GetPost, '/post/<int:post_id>')
    api.add_resource(GetPostHydrate, '/post/hydrate/<int:post_id>')
    # Distinct
    api.add_resource(GetPostType, '/post/getType')

    # Count
    api.add_resource(CountAllPost, '/posts/count/')
    api.add_resource(CountPostByAuthor, '/posts/count/author/<int:author_id>')
    api.add_resource(CountPostsByTimestamp, '/posts/count/timestamp')

    # todo GetPostsByDate(min, max) need to have a fix time format


