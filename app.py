import configparser
import os
from flask import Flask
from flask_restful import Api
from routes.user.user_routes import add_user_routes
from routes.comment.comment_routes import add_comment_routes
from routes.post.post_routes import add_post_routes
from routes.annotation.annotation_routes import add_annotation_routes
from routes.tag.tag_routes import add_tag_routes
from routes.tulipr.tulip_routes import add_tulip_routes
from routes.settings.settings_routes import add_settings_routes

config = configparser.ConfigParser()
config.read("config.ini")

if not os.path.exists(config['exporter']['tlp_path']):
    os.makedirs(config['exporter']['tlp_path'])

app = Flask(__name__)
api = Api(app)

add_user_routes(api)
add_post_routes(api)
add_comment_routes(api)
add_annotation_routes(api)
add_tag_routes(api)
add_tulip_routes(api)
add_settings_routes(api)

if __name__ == '__main__':
    app.run(host=config['api']['host'],
            port=int(config['api']['port']),
            debug=config['api']['debug'],
            threaded = True
            )
