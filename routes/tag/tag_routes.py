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

    api.add_resource(TagsByDate, '/tags/<int:timestamp1>/<int:timestamp2>/<int:limit>')
    api.add_resource(ContentWithCommonTags, '/tags/common/content/<int:tag_id1>-<int:tag_id2>')
    api.add_resource(ContentWithCommonTagsByDate, '/tags/common/content/<int:tag_id1>/<int:tag_id2>/<int:start_date>/<int:end_date>')

