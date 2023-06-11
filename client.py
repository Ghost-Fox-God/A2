from __future__ import print_function

import logging
import grpc
import computeandstorage_pb2
import computeandstorage_pb2_grpc


def run_client():
    print("Trying to run client...")
    # Create a gRPC channel and connect to the server on localhost:50051
    with grpc.insecure_channel('localhost:8085') as channel:
        client = computeandstorage_pb2_grpc.EC2OperationsStub(channel)
        # Test the StoreData method
        data = "Hello, gRPC server!"
        store_request = computeandstorage_pb2.StoreRequest(data=data)
        store_response = client.StoreData(store_request)
        print("File stored. URL:", store_response.s3uri)

    # Test the AppendData method
    append_request = computeandstorage_pb2.AppendRequest(
        data=" Appended data.")
    client.AppendData(append_request)
    print("Data appended.")

    # # Test the DeleteFile method
    # delete_request = computeandstorage_pb2.DeleteRequest(
    #     s3uri=store_response.s3uri)
    # client.DeleteFile(delete_request)
    # print("File deleted.")


if __name__ == '__main__':
    logging.basicConfig()
    run_client()
