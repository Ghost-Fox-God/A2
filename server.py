import grpc
from concurrent import futures
import computeandstorage_pb2
import computeandstorage_pb2_grpc
import json
import boto3


class EC2OperationsServicer(computeandstorage_pb2_grpc.EC2OperationsServicer):
    def StoreData(self, request, context):
        # Retrieve the data from the request message
        data = request.data

        # Store the data in a file on Amazon S3
        s3 = boto3.client('s3')
        bucket_name = 'b00925429cloudcomputingassignment2'
        file_name = 'data.txt'
        s3.put_object(Body=data, Bucket=bucket_name, Key=file_name)

        # Generate the publicly readable URL for the file
        url = f"https://{bucket_name}.s3.amazonaws.com/{file_name}"

        # Return the URL in the response
        return computeandstorage_pb2.StoreReply(s3uri=url)

    def AppendData(self, request, context):
        # Retrieve the data from the request message
        data = request.data

        # Append the data to the existing file on Amazon S3
        s3 = boto3.client('s3')
        bucket_name = 'b00925429cloudcomputingassignment2'
        file_name = 'data.txt'
        existing_data = s3.get_object(Bucket=bucket_name, Key=file_name)[
            'Body'].read().decode('utf-8')
        new_data = existing_data + data
        s3.put_object(Body=new_data, Bucket=bucket_name, Key=file_name)

        # Return an empty response
        return computeandstorage_pb2.AppendReply()

    def DeleteFile(self, request, context):
        # Retrieve the S3 URL from the request message
        url = request.s3uri

        # Extract the bucket name and file name from the URL
        parts = url.split('/')
        bucket_name = parts[2].split('.')[0]
        file_name = '/'.join(parts[3:])

        # Delete the file from Amazon S3
        s3 = boto3.client('s3')
        s3.delete_object(Bucket=bucket_name, Key=file_name)

        # Return an empty response
        return computeandstorage_pb2.DeleteReply()


def run_server():
    # Create a gRPC server
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    computeandstorage_pb2_grpc.add_EC2OperationsServicer_to_server(
        EC2OperationsServicer(), server)

    # Start the server on port 50051
    server.add_insecure_port('[::]:50051')
    server.start()

    # Keep the server alive
    try:
        while True:
            pass
    except KeyboardInterrupt:
        server.stop(0)


if __name__ == '__main__':
    run_server()
