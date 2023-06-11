from concurrent import futures

import grpc
import logging
import computeandstorage_pb2
import computeandstorage_pb2_grpc
import boto3


class EC2OperationsServicerServer(computeandstorage_pb2_grpc.EC2OperationsServicer):

    def StoreData(self, request, context):
        # Retrieve the data from the request message
        data = request.data

        print("Printing Data in storedata : ", data)

        # Store the data in a file on Amazon S3
        s3 = boto3.client("s3", aws_access_key_id="ASIAW75AOYZ32KHUCZAJ",
                          aws_secret_access_key="XA7UqSr4qIrtQ1teOiZzK9nzDgIAREsqq9OazdEE",
                          aws_session_token="FwoGZXIvYXdzEH4aDOafiAvS7OayHsVbkyLAAUkeXClI6/GPPwfwybpJC1iwD6Htd7ez+CJZfwN50jJt8kCVy8lmHebtrr87TDmyOiOUjmEQkav6zaHrNZ76d12nX0F97YH7iI1F4rPFUH5xrBrnBo8F6oquj/8PMo5lyA0CbxviwOFP4q8etZ0xe4dQsb9Uz6uPeZq/Hxq58HGnXSXbEXnYJY6XLTf2wj+1iKQYGpQJ0vvG1l2sl/F6r50JNoGxoKDjs47KzNbnwXvDb/IWgrUIhSYavmSZI8pnMyitmpWkBjItrO9WWp+EJXpqQvYZIxKI3Q7nYl1jZ04ET6WSdfxmMYpRPo7h/MkZmXj1yEDH"
                          )
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
        s3 = boto3.client("s3", aws_access_key_id="ASIAW75AOYZ32KHUCZAJ",
                          aws_secret_access_key="XA7UqSr4qIrtQ1teOiZzK9nzDgIAREsqq9OazdEE",
                          aws_session_token="FwoGZXIvYXdzEH4aDOafiAvS7OayHsVbkyLAAUkeXClI6/GPPwfwybpJC1iwD6Htd7ez+CJZfwN50jJt8kCVy8lmHebtrr87TDmyOiOUjmEQkav6zaHrNZ76d12nX0F97YH7iI1F4rPFUH5xrBrnBo8F6oquj/8PMo5lyA0CbxviwOFP4q8etZ0xe4dQsb9Uz6uPeZq/Hxq58HGnXSXbEXnYJY6XLTf2wj+1iKQYGpQJ0vvG1l2sl/F6r50JNoGxoKDjs47KzNbnwXvDb/IWgrUIhSYavmSZI8pnMyitmpWkBjItrO9WWp+EJXpqQvYZIxKI3Q7nYl1jZ04ET6WSdfxmMYpRPo7h/MkZmXj1yEDH"
                          )
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
        s3 = boto3.client('s3', aws_access_key_id="ASIAW75AOYZ32KHUCZAJ",
                          aws_secret_access_key="XA7UqSr4qIrtQ1teOiZzK9nzDgIAREsqq9OazdEE",
                          aws_session_token="FwoGZXIvYXdzEH4aDOafiAvS7OayHsVbkyLAAUkeXClI6/GPPwfwybpJC1iwD6Htd7ez+CJZfwN50jJt8kCVy8lmHebtrr87TDmyOiOUjmEQkav6zaHrNZ76d12nX0F97YH7iI1F4rPFUH5xrBrnBo8F6oquj/8PMo5lyA0CbxviwOFP4q8etZ0xe4dQsb9Uz6uPeZq/Hxq58HGnXSXbEXnYJY6XLTf2wj+1iKQYGpQJ0vvG1l2sl/F6r50JNoGxoKDjs47KzNbnwXvDb/IWgrUIhSYavmSZI8pnMyitmpWkBjItrO9WWp+EJXpqQvYZIxKI3Q7nYl1jZ04ET6WSdfxmMYpRPo7h/MkZmXj1yEDH"
                          )
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
