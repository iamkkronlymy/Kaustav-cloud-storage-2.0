const express = require('express');
const multer = require('multer');
const fs = require('fs');
const { MongoClient, GridFSBucket } = require('mongodb');
const path = require('path');

const app = express();
const PORT = process.env.PORT || 3000;

const upload = multer({ limits: { fileSize: 10 * 1024 * 1024 } }); // 10MB per file
const MAX_DB_STORAGE = 512 * 1024 * 1024; // 512MB per DB
const TOTAL_LIMIT = 2 * 1024 * 1024 * 1024; // 2GB total

const dbUris = [
  'mongodb+srv://csq5tft8:gEdmhQ94kIgcGi28@cluster0.52zk8gi.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0',
  'mongodb+srv://aohtvv0:bQuM9iRZm91GrPKe@cluster0.qvwhl5x.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0',
  'mongodb+srv://hb6g6vff6gf:Gyjp3wBB8FGyqpXz@cluster0.jkbqe5x.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0',
  'mongodb+srv://aq9d7qh:fKjI8toWE6kVixkL@cluster0.umimpv3.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0'
];

let clients = [], buckets = [];

app.use(express.static('.'));

// Initialize MongoDB clients and GridFS buckets with keep-alive and TLS options
(async () => {
  for (let uri of dbUris) {
    const client = new MongoClient(uri, {
      keepAlive: true,
      socketTimeoutMS: 360000,  // 6 minutes
      tls: true,
      minVersion: 'TLS1.2'
      // tlsAllowInvalidCertificates: false, // Uncomment if needed
    });
    await client.connect();
    clients.push(client);
    buckets.push(new GridFSBucket(client.db('cloud'), { bucketName: 'files' }));
  }
})();

async function getUsedBytes(bucket) {
  const files = await bucket.find().toArray();
  return files.reduce((sum, f) => sum + f.length, 0);
}

async function findFileAllBuckets(filename) {
  for (let i = 0; i < buckets.length; i++) {
    const files = await buckets[i].find({ filename }).toArray();
    if (files.length) return { file: files[0], bucket: buckets[i] };
  }
  return null;
}

app.get('/files', async (req, res) => {
  const skip = parseInt(req.query.skip) || 0;
  let allFiles = [];
  for (let bucket of buckets) {
    const files = await bucket.find().toArray();
    allFiles.push(...files);
  }
  allFiles.sort((a, b) => b.uploadDate - a.uploadDate);
  res.json(allFiles.slice(skip, skip + 10));
});

app.get('/download/:filename', async (req, res) => {
  const match = await findFileAllBuckets(req.params.filename);
  if (!match) return res.status(404).send('Not found');
  res.set('Content-Disposition', `attachment; filename="${match.file.filename}"`);
  match.bucket.openDownloadStreamByName(req.params.filename).pipe(res);
});

app.post('/upload', upload.single('file'), async (req, res) => {
  if (!req.file) return res.status(400).send('No file');

  if (await findFileAllBuckets(req.file.originalname))
    return res.status(400).send('File already exists');

  for (let i = 0; i < buckets.length; i++) {
    const used = await getUsedBytes(buckets[i]);
    if (used + req.file.size < MAX_DB_STORAGE) {
      const stream = buckets[i].openUploadStream(req.file.originalname);
      stream.end(req.file.buffer);
      return res.send('Uploaded');
    }
  }
  res.status(507).send('Storage full');
});

app.delete('/delete/:filename', async (req, res) => {
  for (let bucket of buckets) {
    const files = await bucket.find({ filename: req.params.filename }).toArray();
    for (let file of files) {
      await bucket.delete(file._id);
    }
  }
  res.send('Deleted');
});

app.get('/stats', async (req, res) => {
  let used = 0;
  for (let b of buckets) used += await getUsedBytes(b);
  res.json({ used, free: TOTAL_LIMIT - used });
});

app.listen(PORT, () => console.log(`Server running on http://localhost:${PORT}`));
