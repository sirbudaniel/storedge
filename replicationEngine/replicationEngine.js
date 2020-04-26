
var config = {}
var async = require('async'); 
var utils = require('../utils.js');
var fsWrapper = require('./FSWrapper.js');
var discovery = require('../discovery/discovery.js');
var request = require('request');
var md5 = require('md5');


var my_host = '';


// var neighbours = [{
// 	host: 'localhost',
// 	port: '8501'
// }, {
// 	host: 'localhost',
// 	port: '8502'
// }]


var global_requests = {}


//var neighbours = [];

var loaded_cache = {}


/* Add a new file to the system */
function putNewFile(fileName, sensitivity, b64Content, callback) {
	var buf = Buffer.from(b64Content, 'base64');
	var shards = config.shards;
	utils.log("replication", "Put file " + fileName + "with sensitivity="+ sensitivity + " with size " + buf.byteLength);

	/* Create N chunks from the file */
	var chunk_size = Math.ceil(buf.byteLength / shards);

	
	
	for (var k = 0; k < config.replicas; ++k) {
		var offset = 0;
		for (var i = 0; i < shards; ++i) {
			frag = {
				start: offset,
				end: i == shards - 1 ? buf.byteLength : offset + chunk_size,
				chunk_no: i + 1,
				total_chunks: shards,
				fileName: fileName,
				sensitive: sensitivity,
				b64Content: buf.slice(offset , i == shards - 1 ? buf.byteLength : offset + chunk_size).toString('base64')
			};
			fsWrapper.writeFragment(frag);
			offset += chunk_size
		}
	}

	callback(null, {success: true});
}


function checkNodeAvailability(neighbour, cb) {
	var options = {
	  uri: "http://" + neighbour.ip + ":" + neighbour.port + "/availability",
	  method: 'POST',
	  json: {
	    'source': `${discovery.getMyself().ip}:${discovery.getMyself().port}`,
	    //'source': 'localhost:' + config.port,
	    'source_size': fsWrapper.getUsedSize()
	  }
	};

	request(options, function (error, response, body) {
	  if (!error && response.statusCode == 200) {
	  	cb(null, body);
	  } else {
	  	cb({
	  		error: "Connexion failed"
	  	});
	  }
	});
}

function sendFragmentToNode(neighbour, size, sensitivity, cb) {
	/* Check if duplicate replics exists */
	var my_file_map = fsWrapper.getFiles();
	var max_replicas = 0;
	var selected_chunk = undefined;
	var selected_file = undefined;
	var sensitive = 0;

	for (var file in my_file_map) {
		for (var chunk in my_file_map[file]) {
			if (my_file_map[file][chunk].replicas > max_replicas) {
				selected_file = file;
				selected_chunk = chunk;
				max_replicas = my_file_map[file][chunk].replicas;
				sensitive = my_file_map[file][chunk].sensitive;
			}
		}
	}

	if (max_replicas == 1) {
		/* Select based on the exitend chunks */
		var max_chunks = 0;
		for (var file in my_file_map) {
			if (Object.keys(my_file_map[file]).length > max_chunks) {
				max_chunks = my_file_map[file].length;
				selected_file = file;
				selected_chunk = Object.keys(my_file_map[file])[0];
			}
		}
	}

	if (selected_file == undefined) {
		console.log("THIS SHOULD NEVER HAPPEND");
		return;
	}

	if (sensitive == 1 && sensitivity == 0) {
		console.log("Different sensitivity match")
		return;
	}

	/* Update the file */
	var data = fsWrapper.removeChunk(selected_file, selected_chunk);

	//console.log(data);
	var options = {
	  uri: "http://" + neighbour.ip + ":" + neighbour.port + "/putNewChunk",
	  method: 'POST',
	  json: data
	};
	utils.log("replication", "Move file " + data.fileName + ", chunk " + data.chunk_no + " to " + neighbour.ip + " " + neighbour.port);
	/* Mark the node as used */
	loaded_cache[neighbour.ip + ":" + neighbour.port] = true;
	setTimeout(function() {
		loaded_cache[neighbour.ip + ":" + neighbour.port] = false;
	}, 60 * 1000);

	request(options, function (error, response, body) {
	  if (!error && response.statusCode == 200) {
	  	cb(null, body);
	  } else {
	  	console.log("DATA TRANFER FAILED!");
	  	cb({
	  		error: "Connexion failed"
	  	});
	  }
	});
}


function nodeBalancer() {
	setInterval(function() {
		/* Check if some of my replicas could be propagated to 
		 * neighbours */
		var neighbours = discovery.getNeighbours();

		async.eachSeries(neighbours, function(neighbour, cb) {
			/* Check if the neighbour could receive from me */
			checkNodeAvailability(neighbour, function(err, res) {
				if (err) {
					utils.log("replication", "Could not connect to node " + neighbour.ip + ":" + neighbour.port);
					cb();
					return;
				}
				if (res.available) {
					/* Send a fragment to node */
					sendFragmentToNode(neighbour, res.size, res.sensitive, function(err, res) {
						if (err) {
							utils.log("replication", "Could not finish transaction with node " + neighbour.ip + ":" + neighbour.port);
							cb();
							return;
						}
						cb();
					});
				} else {
					cb();
				}
			});
		}, function(err) {
			
		});
	}, config.rebalance_time);
}

function askForChunks(file_id, neighbour, req_id, sensitivity, cb) {
	var options = {
	  uri: "http://" + neighbour.ip + ":" + neighbour.port + "/chunks",
	  method: 'POST',
	  json: {
	    //'source': discovery.getMyself()
	    'file_id': file_id,
	    'req_id': req_id,
	  	'sensitivity': sensitivity
	  }
	};

	request(options, function (error, response, body) {
	  if (!error && response.statusCode == 200 && !body["error"]) {
	  	cb(null, body);
	  } else {
	  	console.log("options", JSON.stringify(options, null, 2));
	  	console.log("body", JSON.stringify(body, null, 2));
	  	cb({
	  		error: "Connexion failed"
	  	});
	  }
	});
}


function getFile(fileName, callback) {
	var file_id = md5(fileName);
	/* Create request id */
	var request_id = Math.random().toString(36).substring(7);

	global_requests[request_id] = true;

	setTimeout(function() {
		delete global_requests[request_id];
	}, 60 * 1000);

	var neighbours = discovery.getNeighbours();

	var total = fsWrapper.getFileTotal(file_id);
	var result = fsWrapper.getFile(file_id);

	//console.log(total);
	//console.log(result);

	async.eachLimit(neighbours, 5, function(neighbour, cb) {
		askForChunks(file_id, neighbour, request_id, config.sensitive, function(err, response) {
			if (err) {
				cb(err);
			} else {
				/* Update the result if it is not a deplay echo */
				if (!response["duplicate"] && response["total"]) {
					total = response["total"];
					for (var chunk in response["chunks"]) {
						result[chunk] = response["chunks"][chunk];
					}
				}
				cb();
			}
		});
	}, function(err) {
		if (err) {
			callback({
				message: "File not found"
			});
		} else {
			console.log("result = ", JSON.stringify(result, null, 2));
			if (total == 0 || Object.keys(result).length < total) {
				callback({
					message: "File not found"
				});
			} else {
				/* Combine the chunks */
				var chunks = [];
				for (var chunk in result) {
					chunks.push(new Buffer(result[chunk], 'base64'));
				}
				var final_buffer = Buffer.concat(chunks)
				callback(null, {
					fileContent: final_buffer.toString('base64')
				});
			}
		}
	});
}

function init(expressApp, cfg) {
	config = cfg;
	fsWrapper.init(config, function(err) {
		if (err) {
			console.log("ERROR readind FS");
		}

		nodeBalancer();

		expressApp.get('/replicationEngine', (req, res) => {
	    res.send('replicationEngine!')
	  });


	  expressApp.post('/putNewFile', (req, res) => {
	    putNewFile(req.body.fileName, req.body.b64Content, function(err, response) {
	    	if (err) {
	    		res.status(500).send({
	    			'error' : err.message
	    		});
	    	} else {
	    		res.status(200).send(response);
	    	}
	    });
	  });


	  expressApp.post('/getFile', (req, res) => {
	  	getFile(req.body.fileName, function(err, response) {
	  		if (err) {
	    		res.status(500).send({
	    			'error' : err.message
	    		});
	    	} else {
	    		res.status(200).send(response);
	    	}
	  	});
	  });


	  expressApp.post('/availability', (req, res) => {
	  	var req_size = req.body.source_size;
	  	var my_size = fsWrapper.getUsedSize();

	  	/* Check if I have load data on this node in the last minute */
	  	if (loaded_cache[req.body.source]) {
	  		//utils.log("replication", "I have " + req.body.source + " in cache ");
	  		res.status(200).send({'available' : false});
	  		return;
	  	}
	  	//console.log("My size is " + my_size + " and his size is "+ req_size);
	  	if (my_size < req_size) {
	  		/* I could take some data */
	  		/* Update my size with the mean of chunk size */
	  		fsWrapper.increaseVirtualStorage();
	  		utils.log("replication", "I am ready to receive from " + req.body.source + " with Sensitivity= " + config.sensitive);

	  		res.status(200).send({'available' : true, 'size': my_size, 'sensitive': config.sensitive});
	  		return;
	  	}
	  	res.status(200).send({'available': false});
	  });

	  expressApp.post('/putNewChunk', (req, res) => {
	  	/* I have received new chunk from neighbour */
	  	fsWrapper.processNewChunk(req.body);
	  	res.status(200).send({'done': true});
	  });

		expressApp.post('/chunks', (req, res) => {
			var req_id = req.body["req_id"];
			var file_id = req.body["file_id"];
			var sensitivity = req.body['sensitivity']

			if (global_requests[req_id]) {
				/* Duplicate */
				res.status(200).send({'duplicate': true});
			} else {
				global_requests[req_id] = true;

				setTimeout(function() {
					delete global_requests[req_id];
				}, 60 * 1000);

				var neighbours = discovery.getNeighbours();

				var fileSensitivity = fsWrapper.getFileSensitivity(file_id);
				// console.log("SENSITIVITY " + fileSensitivity + " " + sensitivity)
				if (fileSensitivity == 1 && sensitivity == 0) {
					res.status(200).send({'error': true});
					return;
				}

				total = fsWrapper.getFileTotal(file_id);
				var result = fsWrapper.getFile(file_id);

				async.eachLimit(neighbours, 5, function(neighbour, cb) {
					askForChunks(file_id, neighbour, req_id, sensitivity, function(err, response) {
						if (err) {
							cb(err);
						} else {
							/* Update the result if it is not a deplay echo */
							if (!response["duplicate"] && response["total"]) {
								total = response["total"];	
								for (var chunk in response["chunks"]) {
									result[chunk] = response["chunks"][chunk];
								}
							}
							cb();
						}
					});
				}, function(err) {
					if (err) {
						res.status(200).send({'error': true});
				  	} else {
				  		res.status(200).send({
				  			total: total,
				  			chunks: result
				  		});
				  	}
				});
			}
		});
	});
}


module.exports = {
  init: init,
  putFile: putNewFile,
  getFile: getFile
};