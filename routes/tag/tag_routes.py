from routes.tag.tag_getter import *
from routes.tag.tag_aggregations import *


def add_tag_routes(api):
    # Getters
    # Multiple
    api.add_resource(GetTags, '/tags')
    api.add_resource(GetTagsByParent, '/tags/parent/<int:parent_tag_id>')
    # Single
    api.add_resource(GetTag, '/tag/<int:tag_id>')
    api.add_resource(GetTagHydrate, '/tag/hydrate/<int:tag_id>')

    # Count
    api.add_resource(CountAllTag, '/tags/count/')
    api.add_resource(CountTagsByParent, '/tags/count/parent/<int:parent_tag_id>')

