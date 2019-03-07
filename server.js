const fs = require('fs');
const http = require('http');
const https = require('https');
const express = require('express');

let app = express();
let options = {
	// default page should be Neo4jWdc2.html
	index: 'Neo4jWdc2.html'
};
app.use('/', express.static('website', options));

let server;
let on_https = false;
var key = process.env.PRIVATE_KEY ? fs.readFileSync(process.env.PRIVATE_KEY).toString() : undefined;
var cert = process.env.CERTIFICATE ? fs.readFileSync(process.env.CERTIFICATE).toString() : undefined;
let port = process.env.PORT || 3000;

// try HTTPS, otherwise go with HTTP
if (key && cert) {
	console.log('key and cert detected, use HTTPS');
	on_https = true;
	server = https.createServer(
		{
			cert,
			key
		},
		app
	);
} else {
	console.log('key and cert not detected, use HTTP');
	server = http.createServer(app);
}

server.listen(port);
if (on_https)
	console.log(
		'Because listening on HTTPS, you may need to access the server on a different domain. Set your DNS accordingly!'
	);
console.log(`Listening on ${on_https ? 'https' : 'http'}://localhost:${port}`);
