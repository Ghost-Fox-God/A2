from concurrent import futures

import grpc
import logging
import computeandstorage_pb2
import computeandstorage_pb2_grpc
import boto3
from boto3.session import Session

aws_access_key_id = "ASIAW75AOYZ3WY3VZRFA"
aws_secret_access_key = "ldq3+/ZPBGg6N65bAz+ew/VtQTkqElObLTstp3C7"
aws_session_token = "FwoGZXIvYXdzEHoaDGLJaBwjkNDwacqKkSLAAYBrqK3iv6KcrD3iHFKznGRLRObukM+29cAXzWFPdN9cRK3KnVD0b4f1J+zLhlMAIx+X7ISVVIuMaCTPEMNLosW1OzKJw3BCS2b2o/Yn5jcRdn0wegE+PzLFaJS47rKYpiZPOL+Zveq+0tg68b/BubuI6BFcqGVAc+KBJBqZ2MuXad4+tTHh0wWPOdHfcc/7g1NeLsK/vWsRrpXAJkDM98mTJ6w3jOOSGqTcLZ+VZGhGxMU2vpYXi+5Bx1QPjlQXTCjun5SkBjItJeqLcM37AV8DvLRNR6PR53RAGDEWWv8pz9yhif0rC7dlAnyB+vRuyg/xxfvT"
region = 'us-east-1'


class EC2OperationsServicerServer(computeandstorage_pb2_grpc.EC2OperationsServicer):
    def StoreData(self, request, context):
        # Retrieve the data from the request message
        data = request.data

        print("Printing Data in storedata : ", data)

        # Store the data in a file on Amazon S3
        s3 = boto3.client("s3", aws_access_key_id="ASIAW75AOYZ3WY3VZRFA",
                          aws_secret_access_key="ldq3+/ZPBGg6N65bAz+ew/VtQTkqElObLTstp3C7",
                          aws_session_token="FwoGZXIvYXdzEHoaDGLJaBwjkNDwacqKkSLAAYBrqK3iv6KcrD3iHFKznGRLRObukM+29cAXzWFPdN9cRK3KnVD0b4f1J+zLhlMAIx+X7ISVVIuMaCTPEMNLosW1OzKJw3BCS2b2o/Yn5jcRdn0wegE+PzLFaJS47rKYpiZPOL+Zveq+0tg68b/BubuI6BFcqGVAc+KBJBqZ2MuXad4+tTHh0wWPOdHfcc/7g1NeLsK/vWsRrpXAJkDM98mTJ6w3jOOSGqTcLZ+VZGhGxMU2vpYXi+5Bx1QPjlQXTCjun5SkBjItJeqLcM37AV8DvLRNR6PR53RAGDEWWv8pz9yhif0rC7dlAnyB+vRuyg/xxfvT")
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
        print("Printing Data in appenddata : ", data)

        # Append the data to the existing file on Amazon S3
        s3 = boto3.client("s3", aws_access_key_id="ASIAW75AOYZ3WY3VZRFA",
                          aws_secret_access_key="ldq3+/ZPBGg6N65bAz+ew/VtQTkqElObLTstp3C7",
                          aws_session_token="FwoGZXIvYXdzEHoaDGLJaBwjkNDwacqKkSLAAYBrqK3iv6KcrD3iHFKznGRLRObukM+29cAXzWFPdN9cRK3KnVD0b4f1J+zLhlMAIx+X7ISVVIuMaCTPEMNLosW1OzKJw3BCS2b2o/Yn5jcRdn0wegE+PzLFaJS47rKYpiZPOL+Zveq+0tg68b/BubuI6BFcqGVAc+KBJBqZ2MuXad4+tTHh0wWPOdHfcc/7g1NeLsK/vWsRrpXAJkDM98mTJ6w3jOOSGqTcLZ+VZGhGxMU2vpYXi+5Bx1QPjlQXTCjun5SkBjItJeqLcM37AV8DvLRNR6PR53RAGDEWWv8pz9yhif0rC7dlAnyB+vRuyg/xxfvT")
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
        print("Printing Data in appenddata : ", url)

        # Extract the bucket name and file name from the URL
        parts = url.split('/')
        bucket_name = parts[2].split('.')[0]
        file_name = '/'.join(parts[3:])

        # Delete the file from Amazon S3
        s3 = boto3.client('s3')
        s3.delete_object(Bucket=bucket_name, Key=file_name)

        # Return an empty response
        return computeandstorage_pb2.DeleteReply()


def serve():
    # Create a gRPC server
    port = '8085'
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    computeandstorage_pb2_grpc.add_EC2OperationsServicer_to_server(
        EC2OperationsServicerServer(), server)

    # Start the server on port 50051
    server.add_insecure_port('[::]:' + port)
    server.start()
    print("Server started, listening on " + port)
    server.wait_for_termination()


if __name__ == '__main__':
    logging.basicConfig()
    serve()
