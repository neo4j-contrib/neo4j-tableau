const fs = require('fs');
const https = require('https');

const express = require('express');

let app = express();
app.use('/', express.static('website'));

let errors = [];
if (!process.env.CERTIFICATE) errors.push('CERTIFICATE env var not defined');
if (!process.env.PRIVATE_KEY) errors.push('PRIVATE_KEY env var not defined');

if (errors.length > 0) {
  throw new Error(`Failed with the following errors:${errors.join(', ')}`);
}

var key = fs.readFileSync(process.env.PRIVATE_KEY).toString();
var cert = fs.readFileSync(process.env.CERTIFICATE).toString();
let port = process.env.PORT || 3000;
https.createServer({
  cert,
  key
}, app).listen(port);
console.log(`Listening on ${port}`);