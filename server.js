const express = require('express');
const { Client } = require('elasticsearch');
const fs = require('fs');
const app = express();
const Minio = require('minio');

const MINIO_ACCESS_KEY = 'minio';
const MINIO_SECRET_KEY = 'miniostorage'
const MINIO_ENDPOINT = '192.168.1.189';
const MINIO_PORT = 9000;
const MINIO_USE_SSL = false;

const ELASTICSEARCH_HOST = 'http://192.168.1.189:9200';

const index_name = 'bulkdatatest';
const bucketName = 'bulkdatatest';
const BATCH_SIZE = 1000;

const minioClient = new Minio.Client({
    endPoint: MINIO_ENDPOINT,
    port: MINIO_PORT,
    useSSL: MINIO_USE_SSL,
    accessKey: MINIO_ACCESS_KEY,
    secretKey: MINIO_SECRET_KEY
})

const elasticsearchClient = new Client({
    host: ELASTICSEARCH_HOST
});

app.get('/', (req, res) => {
    res.send('minio sample app');
})
// Create a bucket
app.get('/createBucket', (req, res) => {
    minioClient.makeBucket(bucketName, 'us-east-1', function (err) {
        if (err) return console.log(err)
        console.log('Bucket created successfully in "us-east-1".')
    });
    res.send('Bucket created successfully');
})

app.get('/upload', async (req, res) => {
    const indexing_data = [];
    try {
        const data = await fs.promises.readFile('./metadata.json', 'utf8');
        const files = JSON.parse(data);
        console.log('files : ', files);
        for (const file of files) {
            const fileStream = fs.createReadStream(file.path);
            const etag = await new Promise((resolve, reject) => {
                minioClient.putObject(bucketName, file.path, fileStream, (err, etag) => {
                    if (err) {
                        reject(err);
                    } else {
                        resolve(etag);
                    }
                });
            });
            console.log('etag : ', etag);
            const doc = {
                bucketName: bucketName,
                objectID: file.path,
                etag: etag,
                name: file.name,
                age: file.age
            };
            indexing_data.push(doc);
        }
        // await fs.promises.writeFile('./indexing_data.json', JSON.stringify(indexing_data), 'utf8');
        await index(indexing_data);
        res.send('Uploaded files to minio server and indexed data to elasticsearch');
    } catch (error) {
        console.log(error);
        res.status(500).send('An error occurred while uploading files to minio and writing indexing data to file');
    }
});

const index = async (indexing_data) => {
    const body = indexing_data.reduce((bulkRequestBody, doc) => {
        bulkRequestBody += JSON.stringify({ index: { _index: index_name } }) + '\n';
        bulkRequestBody += JSON.stringify(doc) + '\n';
        return bulkRequestBody;
    }, '');

    const response = await elasticsearchClient.bulk({
        body: body
    });
    console.log('indexing completed');
}



app.get('/search/:name', async (req, res) => {
    const name = req.params.name;
    console.log('name : ', name);
    try {
        const response = await elasticsearchClient.search({
            index: index_name,
            body: {
                query: {
                    match: {
                        name: name
                    }
                }
            }
        });
        console.log('response : ', response);
        const hits = response.hits.hits;
        const data = hits.map(hit => hit._source);
        const object_ids = data.map(d => d.objectID);
        res.send('Bucket name : ' + data[0].bucketName + ' Object ids : ' + object_ids);
    } catch (error) {
        console.log(error);
        res.status(500).send('An error occurred while searching data in elasticsearch');
    }
});


app.listen(3000, () => {
    console.log('Server started on port 3000');
})