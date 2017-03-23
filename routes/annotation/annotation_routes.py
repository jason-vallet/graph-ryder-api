from routes.annotation.annotation_getter import *
from routes.annotation.annotation_aggregations import *


def add_annotation_routes(api):
    # Getters
    # Multiple
    api.add_resource(GetAnnotations, '/annotations')
    # Single
    api.add_resource(GetAnnotation, '/annotations/<int:annot_id>')
    api.add_resource(GetAnnotationHydrate, '/annotation/hydrate/<int:annotation_id>')
    api.add_resource(GetAnnotationsOnPosts, '/annotations/posts')
    api.add_resource(GetAnnotationsByPost, '/annotations/post/<int:post_id>')
    api.add_resource(GetAnnotationsOnComments, '/annotations/comments')
    api.add_resource(GetAnnotationsByComment, '/annotations/comment/<int:comment_id>')
    api.add_resource(GetAnnotationsByAuthor, '/annotations/author/<int:user_id>')

    # Count
    api.add_resource(CountAllAnnotations, '/annotations/count')
    api.add_resource(CountAnnotationsOnPosts, '/annotations/count/posts')
    api.add_resource(CountAnnotationsByPost, '/annotations/count/post/<int:post_id>')
    api.add_resource(CountAnnotationsOnComments, '/annotations/count/comments')
    api.add_resource(CountAnnotationsByComment, '/annotations/count/comment/<int:comment_id>')
    api.add_resource(CountAnnotationsByAuthor, '/annotations/count/author/<int:user_id>')

