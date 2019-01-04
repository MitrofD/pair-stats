// @flow
const http = require('http');

const {
  PORT,
} = process.env;

const port = parseInt(PORT) || 3000;

const server = http.createServer((req, res) => {
  res.write('Hello world');
  res.end();
});

server.listen(port);
