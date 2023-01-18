// Create an API to interact with minio server
const express = require('express');
const app = express();
const Minio = require('minio');
const ACCESS_KEY = 'minio';
const SECRET_KEY = 'miniostorage'

const minioClient = new Minio.Client({
    endPoint: 'localhost',
    port: 9000,
    useSSL: false,
    accessKey: ACCESS_KEY,
    secretKey: SECRET_KEY
})
app.get('/', (req, res) => {
    res.send('minio sample app');
})
// Create a bucket
app.get('/createBucket', (req, res) => {
    minioClient.makeBucket('test', 'us-east-1', function (err) {
        if (err) return console.log(err)
        console.log('Bucket created successfully in "us-east-1".')
    });
    res.send('Bucket created successfully');
})
// List all buckets
app.get('/listBuckets', (req, res) => {
    minioClient.listBuckets(function (err, buckets) {
        if (err) return console.log(err)
        console.log('buckets : ', buckets)
    });
    res.send('Listed all buckets');
})
// on hitting localhost:3000/upload. open metadata.json and iterate over the files and upload them to minio server
app.get('/upload', (req, res) => {
    const fs = require('fs');
    fs.readFile('./metadata.json', 'utf8', (error, data) => {
        if (error) {
            console.log(error);
            return;
        }
        const files = JSON.parse(data);
        console.log('files : ', files);
        files.forEach(file => {
            const fileStream = fs.createReadStream(file.path);
            const metaData = {
                'Content-Type': 'application/octet-stream',
                'name': file.name,
                'age': file.age
            }
            minioClient.putObject('test', file.name, fileStream, metaData, (err, etag) => console.log(err, etag) // err should be null
            );
        })
    })
    res.send('Uploaded files to minio server');
})
app.listen(3000, () => {
    console.log('Server started on port 3000');
})



