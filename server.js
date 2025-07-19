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

// Initialize MongoDB clients and GridFS buckets with TLS options
(async () => {
  try {
    for (let uri of dbUris) {
      const client = new MongoClient(uri, {
        // keepAlive is deprecated and true by default since Mongoose 5.2.0 for MongoDB driver >= 4.0.0
        // Removing it to clear the deprecation warning.
        socketTimeoutMS: 360000,  // 6 minutes
        tls: true,
        // minVersion: 'TLS1.2' // REMOVING THIS TO ADDRESS ERR_SSL_TLSV1_ALERT_INTERNAL_ERROR
                               // This allows the driver to negotiate the best available TLS version.
      });
      await client.connect();
      clients.push(client);
      buckets.push(new GridFSBucket(client.db('cloud'), { bucketName: 'files' }));
      console.log(`Connected to MongoDB: ${uri.substring(0, uri.indexOf('@') + 1)}...`);
    }
    console.log("All MongoDB connections established.");
  } catch (error) {
    console.error("Failed to connect to one or more MongoDB clusters:", error);
    // It's crucial to handle this error. If connections fail, the app can't function.
    // Consider adding more specific error handling or exiting the process.
    process.exit(1); // Exit if initial DB connections fail
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
  if (buckets.length === 0) {
    return res.status(503).send("Server is still initializing database connections. Please try again shortly.");
  }
  for (let bucket of buckets) {
    try {
      const files = await bucket.find().toArray();
      allFiles.push(...files);
    } catch (error) {
      console.error(`Error fetching files from a bucket: ${error.message}`);
      // Decide if you want to partially succeed or fail the request if one bucket has an issue
    }
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

  if (buckets.length === 0) {
    return res.status(503).send("Server is still initializing database connections. Please try again shortly.");
  }

  for (let i = 0; i < buckets.length; i++) {
    try {
      const used = await getUsedBytes(buckets[i]);
      if (used + req.file.size < MAX_DB_STORAGE) {
        const stream = buckets[i].openUploadStream(req.file.originalname);
        stream.end(req.file.buffer);
        return res.send('Uploaded');
      }
    } catch (error) {
      console.error(`Error during upload to bucket ${i}: ${error.message}`);
    }
  }
  res.status(507).send('Storage full');
});

app.delete('/delete/:filename', async (req, res) => {
  if (buckets.length === 0) {
    return res.status(503).send("Server is still initializing database connections. Please try again shortly.");
  }
  let deleted = false;
  for (let bucket of buckets) {
    try {
      const files = await bucket.find({ filename: req.params.filename }).toArray();
      for (let file of files) {
        await bucket.delete(file._id);
        deleted = true; // Mark as deleted if found and deleted from any bucket
      }
    } catch (error) {
      console.error(`Error deleting file from a bucket: ${error.message}`);
    }
  }
  if (deleted) {
    res.send('Deleted');
  } else {
    res.status(404).send('File not found or could not be deleted.');
  }
});

app.get('/stats', async (req, res) => {
  let used = 0;
  if (buckets.length === 0) {
    return res.json({ used: 0, free: TOTAL_LIMIT, message: "Database connections not yet established." });
  }
  for (let b of buckets) {
    try {
      used += await getUsedBytes(b);
    } catch (error) {
      console.error(`Error getting stats from a bucket: ${error.message}`);
    }
  }
  res.json({ used, free: TOTAL_LIMIT - used });
});

app.listen(PORT, () => console.log(`Server running on http://localhost:${PORT}`));
