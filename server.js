var config = require('./config.json');
var port = process.argv[2] || config.port;
config.port = port;
var dataPath = process.argv[3] || config.data_path;
config.data_path = dataPath;
var sensitive = Math.floor(Math.random() * 2);
config.sensitive = parseInt(sensitive)
console.log("machine with port " + port + "has sensitivity=" + config.sensitive)
var express = require('express');
var bodyParser = require('body-parser')
var formidable = require('formidable');
var fs = require('fs');

var app = express();
app.use(express.static('public'))
app.use(bodyParser.json({limit: '50mb'}));


var discovery = require('./discovery/discovery.js');
discovery.init(app, config);

var replicationEngine = require('./replicationEngine/replicationEngine.js');
replicationEngine.init(app, config);

app.post('/fileupload', (req, res) => {
  var form = new formidable.IncomingForm();
  form.parse(req, function (err, fields, files, checkbox) {
    var filename = files.filetoupload.name;
    var sensitivity = parseInt(fields.sensitivity || "0");
    var b64content = fs.readFileSync(files.filetoupload.path).toString('base64');
    //console.log(filename, b64content);
    replicationEngine.putFile(filename, sensitivity, b64content, function(err, response) {
      if (err) {
        console.log(err);
        res.write(JSON.stringify(err));
        res.end();
      } else {
        res.write(JSON.stringify(response));
        res.end();
      }
    });
  });
});

app.get('/filedownload/:filename', (req, res) => {
  var filename = req.params.filename;
  console.log(filename);
  replicationEngine.getFile(filename, function(err, response) {
    if (err) {
      console.log(err);
      res.write(JSON.stringify(err));
      res.end();
    } else {
      var buf = Buffer.from(response.fileContent, 'base64');
      res.write(buf);
      res.end();
    }
  });
});


app.listen(config.port, () => console.log('App listening on port ' + config.port + '!'))