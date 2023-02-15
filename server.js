const express = require('express');
const { Client } = require('elasticsearch');
const fs = require('fs');
const app = express();
const Minio = require('minio');

const MINIO_ACCESS_KEY = 'minio';
const MINIO_SECRET_KEY = 'miniostorage'
const MINIO_ENDPOINT = 'localhost';
const MINIO_PORT = 9000;
const MINIO_USE_SSL = false;

const ELASTICSEARCH_HOST = 'http://localhost:9200';

const index_name = 'test-index';
const bucketName = 'test-bucket';
// const BATCH_SIZE = 1000;

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

// query - /upload?format=csv&filecolname=file_path
app.get('/upload', async (req, res) => {
    const indexing_data = [];
        try {
            // For accept file of csv format use parameter - ?format=csv in the url.
            var format = req.query.format
            var fileColName = req.query.filecolname
            if(fileColName == undefined)
                fileColName = "file"
            var data = []
            if (format == "csv") {
                csvData = await fs.promises.readFile('./metadata_laptop.csv', 'utf8');
                rows = csvData.split(/(?:\r\n|\n)+/).filter(function(el) {return el.length != 0});
                if(rows.length != 0) {
                    columns = rows.splice(0, 1)[0].split(",");
                    columns = columns.map(c => {
                        return c.trim().replace(/['"]+/g, '')
                     })
                    for (var i=1; i<rows.length; i++) {
                        let valuesRegExp = /(?:\"([^\"]*(?:\"\"[^\"]*)*)\")|([^\",]+)/g;
                        // let elements = [];
    
                        let element = {};
                        let j = 0;
                        rows[i] = rows[i].replace(",,", ",NULL_VAL,")
                        while (matches = valuesRegExp.exec(rows[i])) {
                            var value = matches[1] || matches[2];
                            value = value.replace(/\"\"/g, "\"");
                            if (!isNaN(value))
                                value = Number(value)
                            else if(value == "true" || value == "false")
                                value = value == "true"
                            
                            if(value == "NULL_VAL")
                                value = ""
                            element[columns[j]] = value;
                            j++;
                        }
                        data.push(element);
                    }
                }
                data = JSON.stringify(data);
            } else {
                data = await fs.promises.readFile('./metadata.json', 'utf8');
            }
            const files = JSON.parse(data);
            console.log(files)
            for (const file of files) {
                if(file[fileColName].startsWith("./")) {
                    file[fileColName] = file[fileColName].slice(2)
                }
                if (fs.existsSync(file[fileColName])) {
                    console.log('uploading file : ', file[fileColName]);
                    const fileStream = fs.createReadStream(file[fileColName]);
                    const etag = await new Promise((resolve, reject) => {
                    minioClient.putObject(bucketName, file[fileColName], fileStream, (err, etag) => {
                        if (err) {
                            reject(err);
                        } else {
                            console.log('uploaded file : ', file[fileColName]);
                            resolve(etag);
                        }
                    });
                });
                const doc = {
                    bucketName: bucketName,
                    objectID: file[fileColName],
                    etag: etag,
                    name: file.name,
                    age: file.age
                };
                indexing_data.push(doc);
            } else {
                console.log("file : " + file[fileColName] + " does not exist.")
            }
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
    console.log(response)
    console.log('indexing completed');
}



app.get('/search/:name', async (req, res) => {
    const name = req.params.name;
    console.log('name : ', name);
    try {
        const response = await elasticsearchClient.search({
            index: index_name,
            body: {
                "query": {
                    "match": {
                      "name":name
                    }
                }
            }
        });
        console.log('response : ', response);
        const hits = response.hits.hits;
        const data = hits.map(hit => hit._source);
        ret = data.map(d => {
            obj = {
                bucketName: d.bucketName,
                objectID: d.objectID,
                etag: d.etag,
                metadata: {
                    name: d.name,
                    age: d.age
                }
            }
            // const fileStream = minioClient.getObject(bucketName, d.objectID);
            // d.file = fileStream;
            return obj;
        });
        // set content type to json
        res.setHeader('Content-Type', 'application/json');
        res.send(ret);
    } catch (error) {
        console.log(error);
        res.status(500).send('An error occurred while searching data in elasticsearch');
    }
});


app.listen(3000, () => {
    console.log('Server started on port 3000');
})