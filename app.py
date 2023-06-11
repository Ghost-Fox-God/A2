from os import path
from flask import Flask, request, jsonify
import grpc
import computeandstorage_pb2
import computeandstorage_pb2_grpc
app = Flask(__name__)


@app.route('/storedata', methods=['POST'])
def store_request():
    with grpc.insecure_channel('localhost:8085') as channel:
        client = computeandstorage_pb2_grpc.EC2OperationsStub(channel)
        data = request.json.get("data")
        store_request = computeandstorage_pb2.StoreRequest(data=data)
        store_response = client.StoreData(store_request)
        print("File stored. URL:", store_response.s3uri)
        return jsonify({"s3uri": store_response.s3uri})


@app.route('/appenddata', methods=['POST'])
def append_request():
    with grpc.insecure_channel('localhost:8085') as channel:
        client = computeandstorage_pb2_grpc.EC2OperationsStub(channel)
        data = request.json.get("data")
        append_request = computeandstorage_pb2.AppendRequest(data=data)
        client.AppendData(append_request)
        return


@app.route('/deletefile', methods=['POST'])
def delete_request():
    with grpc.insecure_channel('localhost:8085') as channel:
        client = computeandstorage_pb2_grpc.EC2OperationsStub(channel)
        # Test the StoreData method
        s3uri = request.json.get("s3uri")
        delete_request = computeandstorage_pb2.DeleteRequest(s3uri=s3uri)
        client.DeleteFile(delete_request)
        return


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=6000, debug=True)
