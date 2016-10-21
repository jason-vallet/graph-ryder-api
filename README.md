# Graph Rider Api
This is a simple Rest full Api to interact with neo4j database and query opencare users, posts and comments

####1. change config file
```
cp config.example.ini config.ini
nano config.ini
```
Neo4j database need the following graphAware plugins:
- [graphaware-timetree-3.0.1.38.24.jar](http://products.graphaware.com/download/timetree/graphaware-timetree-3.0.1.38.24.jar)
- [graphaware-server-community-all-3.0.1.38.jar](http://products.graphaware.com/download/framework-server-community/graphaware-server-community-all-3.0.1.38.jar)
If you launch neo4j in a container name it with an alias
```
[neo4j]
url = myNeo4j
user = user
password = pass
```
## Local Installation
####2. install requirements
```
pip install -r requirements.txt
```
####3. include Export Sigma Plugin
```
cp exportsigma.py /usr/local/lib/python3.5/site-packages/tulip/native/plugins/exportsigma.py
```
####4. launch api server
```
python app.py
```

## Docker Installation
####3. build
```
docker build -t graph-ryder-api .
```
####4. run
```
docker run -d -p 5000:5000 --name my-graph-ryder-api graph-ryder-api
```
If you launch neo4j in a container you have to link it with '--link' option
```
--link neo4jContainerName:myNeo4j
```
## Post install
### Update database
- via web browser
```
http://localhost:5000/update
```
use hardUpdate to clean database before
### Generate static graphs
- via web browser
```
http://localhost:5000/generateGraphs
```
- via graph-ryder-dashboard settings / Generate Graphs
```
http://localhost:9000/#/dashboard/settings
```
### Generate the apidoc
- install apidoc
```
npm install apidoc -g
```
- generate the doc
```
apidoc -i ./routes/ -o ./routes/apidoc/
```