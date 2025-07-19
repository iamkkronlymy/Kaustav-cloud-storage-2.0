const express = require('express');
const multer = require('multer');
const fs = require('fs');
const { MongoClient, GridFSBucket, ObjectId } = require('mongodb'); // Import ObjectId
const path = require('path');

const app = express();
const PORT = process.env.PORT || 3000;

// Add express.json() middleware to parse JSON request bodies
app.use(express.json());

const upload = multer({ limits: { fileSize: 10 * 1024 * 1024 } }); // 10MB per file
const MAX_DB_STORAGE = 512 * 1024 * 1024; // 512MB per DB
const TOTAL_LIMIT = 2 * 1024 * 1024 * 1024; // 2GB total

const dbUris = [
  'mongodb+srv://csq5tft8:gEdmhQ94kIgcGi28@cluster0.52zk8gi.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0',
  'mongodb+srv://hb6g6vff6gf:Gyjp3wBB8FGyqpXz@cluster0.jkbqe5x.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0',
  'mongodb+srv://aq9d7qh:fKjI8toWE6kVixkL@cluster0.umimpv3.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0',
  'mongodb+srv://tf4hz7z:TXAJQlbquHbNUK2N@cluster0.0qfgeqz.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0'
];

let clients = [], buckets = [];

app.use(express.static('.'));

// Initialize MongoDB clients and GridFS buckets with TLS options
(async () => {
  let successfulConnections = 0;
  for (let uri of dbUris) {
    try {
      console.log(`Attempting to connect to MongoDB: ${uri.substring(0, uri.indexOf('@') + 1)}...`);
      const client = new MongoClient(uri, {
        socketTimeoutMS: 360000,  // 6 minutes
        tls: true,
        // minVersion: 'TLS1.2' // Keeping this commented out. This allows the driver to negotiate
                               // the best available TLS version, which might help bypass
                               // the ERR_SSL_TLSV1_ALERT_INTERNAL_ERROR if it's a specific
                               // client-side TLS version negotiation issue.
      });
      await client.connect();
      clients.push(client);
      buckets.push(new GridFSBucket(client.db('cloud'), { bucketName: 'files' }));
      console.log(`Successfully connected to MongoDB: ${uri.substring(0, uri.indexOf('@') + 1)}...`);
      successfulConnections++;
    } catch (error) {
      console.error(`ERROR: Failed to connect to MongoDB URI ${uri.substring(0, uri.indexOf('@') + 1)}...`);
      console.error("Please check your MongoDB URI, network access (IP whitelist on Atlas), and Node.js environment (especially TLS/OpenSSL compatibility).");
      console.error("Error details:", error.message);
    }
    // 3ms delay between connection attempts
    await new Promise(resolve => setTimeout(resolve, 3));
  }

  if (successfulConnections === 0) {
    console.error("CRITICAL ERROR: Failed to connect to ANY MongoDB clusters. Application will not start.");
    process.exit(1); // Exit if no connections could be established
  } else {
    console.log(`Successfully established connections to ${successfulConnections} out of ${dbUris.length} MongoDB clusters.`);
    console.log("Application will proceed with available connections.");
  }
})();

async function getUsedBytes(bucket) {
  const files = await bucket.find().toArray();
  return files.reduce((sum, f) => sum + f.length, 0);
}

async function findFileAllBuckets(filename) {
  for (let i = 0; i < buckets.length; i++) {
    try {
      // Find the file by filename
      const files = await buckets[i].find({ filename }).toArray();
      if (files.length) return { file: files[0], bucket: buckets[i], bucketIndex: i }; // Return bucketIndex
    } catch (error) {
      console.error(`Error searching for file '${filename}' in bucket ${i}: ${error.message}`);
    }
  }
  return null;
}

app.get('/files', async (req, res) => {
  const skip = parseInt(req.query.skip) || 0;
  let allFiles = [];
  if (buckets.length === 0) {
    return res.status(503).send("Server is still initializing database connections or failed to connect. Please check server logs.");
  }
  for (let bucket of buckets) {
    try {
      const files = await bucket.find().toArray();
      allFiles.push(...files);
    } catch (error) {
      console.error(`Error fetching files from a bucket during /files request: ${error.message}`);
    }
  }
  allFiles.sort((a, b) => b.uploadDate - a.uploadDate);
  res.json(allFiles.slice(skip, skip + 10));
});

app.get('/download/:filename', async (req, res) => {
  const match = await findFileAllBuckets(req.params.filename);
  if (!match) return res.status(404).send('Not found');
  try {
    res.set('Content-Disposition', `attachment; filename="${match.file.filename}"`);
    match.bucket.openDownloadStreamByName(req.params.filename).pipe(res);
  } catch (error) {
    console.error(`Error during download of '${req.params.filename}': ${error.message}`);
    res.status(500).send('Error downloading file.');
  }
});

app.post('/upload', upload.single('file'), async (req, res) => {
  if (!req.file) return res.status(400).send('No file');

  if (await findFileAllBuckets(req.file.originalname))
    return res.status(400).send('File already exists');

  if (buckets.length === 0) {
    console.error("Upload attempted but no database connections are established.");
    return res.status(503).send("Server is not ready: Database connections failed during startup. Please check server logs.");
  }

  for (let i = 0; i < buckets.length; i++) {
    try {
      const used = await getUsedBytes(buckets[i]);
      if (used + req.file.size < MAX_DB_STORAGE) {
        const stream = buckets[i].openUploadStream(req.file.originalname);
        stream.end(req.file.buffer);
        console.log(`File '${req.file.originalname}' uploaded to bucket ${i}.`);
        return res.status(200).send('File uploaded successfully.');
      }
    } catch (error) {
      console.error(`Error during upload of '${req.file.originalname}' to bucket ${i}: ${error.message}`);
    }
  }
  res.status(507).send('Storage full or an error occurred during upload to all available buckets.');
});

// New endpoint for renaming files
app.post('/rename', async (req, res) => {
  const { oldName, newName } = req.body;

  if (!oldName || !newName) {
    return res.status(400).send('Old name and new name are required.');
  }

  if (oldName === newName) {
    return res.status(400).send('New name cannot be the same as the old name.');
  }

  if (buckets.length === 0) {
    return res.status(503).send("Server is not ready: Database connections not established.");
  }

  // Check if a file with the new name already exists
  if (await findFileAllBuckets(newName)) {
    return res.status(409).send(`A file named '${newName}' already exists.`);
  }

  const match = await findFileAllBuckets(oldName);
  if (!match) {
    return res.status(404).send('Original file not found.');
  }

  try {
    // GridFS does not have a direct rename operation on the file document.
    // Instead, you update the filename field in the files collection directly.
    await match.bucket.s.db.collection('files.files').updateOne(
      { _id: match.file._id },
      { $set: { filename: newName } }
    );
    console.log(`File '${oldName}' renamed to '${newName}' in bucket ${match.bucketIndex}.`);
    res.status(200).send('File renamed successfully.');
  } catch (error) {
    console.error(`Error renaming file '${oldName}' to '${newName}': ${error.message}`);
    res.status(500).send('Error renaming file: ' + error.message);
  }
});


app.delete('/delete/:filename', async (req, res) => {
  if (buckets.length === 0) {
    return res.status(503).send("Server is still initializing database connections or failed to connect. Please check server logs.");
  }
  let deleted = false;
  for (let bucket of buckets) {
    try {
      const files = await bucket.find({ filename: req.params.filename }).toArray();
      for (let file of files) {
        await bucket.delete(file._id);
        deleted = true;
        console.log(`File '${req.params.filename}' deleted from a bucket.`);
      }
    } catch (error) {
      console.error(`Error deleting file '${req.params.filename}' from a bucket: ${error.message}`);
    }
  }
  if (deleted) {
    res.status(200).send('File deleted successfully.');
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
