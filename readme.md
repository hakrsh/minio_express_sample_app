## Minio Express Sample App
This is a simple app written in express. When you launch it, it will connect to the minio server. '/createBucket' will make a bucket called 'test'. '/upload' will loop through metadata.json and upload the curresponding image to minio. A python script is used to generate metadata.

## steps to run
1. install node
2. install minio, default username and password is 'minio' and 'miniostorage'. Start the server with 'minio server --console-address ":9000" /path/to/data'
3. clone this repo
4. run 'pip3 install names' to install the names packag, used to generate random names
5. run 'npm install' to install dependencies
6. run 'node app.js' to start the app
7. go to localhost:3000/createBucket to create a bucket, u can change the bucket name in app.js (bucketName)
8. go to localhost:3000/upload to upload the images to minio
9. go to localhost:9000 to see the images in the minio server. Default username and password is 'minio' and 'miniostorage'
