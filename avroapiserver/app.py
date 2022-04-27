# Using flask to make an api
# import necessary libraries and functions
from flask import Flask, jsonify, request
from avro_reader import avro_output
import json
# creating a Flask app
app = Flask(__name__)
# on the terminal type: curl http://127.0.0.1:5000/
# returns hello world when we use GET.
# returns the data that we send when we use POST.
avro_link={"link":""}
@app.route('/avro', methods = ['POST'])
def process_json():
	content_type = request.headers.get('Content-Type')
	if content_type == 'application/json':
		data = request.json
		avro_link["link"]=data["data"]
		return data
	else:
		return 'Content-Type not supported!'

@app.route('/avro', methods = ['GET'])
def home():
	if request.method == 'GET':
		data = avro_link["link"]
		print(type(data))
		link_url_dict = json.loads(data)
		content = avro_output(link_url_dict["fileUrl"])
		print(type(content))
		return content

# driver function
if __name__ == '__main__':

	app.run(debug = True)
