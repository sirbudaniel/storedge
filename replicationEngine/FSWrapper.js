var md5 = require('md5');
var fs = require('fs');
//var config = require('../config.json');
var config = {};

var global_files_map = {

}

var mean_chunk = 0;
var virtual_size = 0;

function init(cfg, cb) {
	config = cfg;
	fs.readdir(config.data_path, function(err, files) {
		if (err) {
			cb(err);
			return;
		}
		files.forEach(function(file, index) {
			if (file.indexOf("-meta.json") != -1) {
				var file_name = file.split("-")[0];
				var chunk_no = file.split("-")[1];
				var meta = JSON.parse(fs.readFileSync(config.data_path + file, 'utf8'));
				if (global_files_map[file_name] == undefined) {
					global_files_map[file_name] = {}
				}
				global_files_map[file_name][chunk_no] = {replicas: meta.replicas, size: meta.replicas * (meta.end - meta.start), sensitive: meta.sensitive};
				/* Update mean chunk */
				if (mean_chunk) {
					mean_chunk = (mean_chunk + (meta.end - meta.start)) / 2;
				} else {
					mean_chunk = (meta.end - meta.start);
				}
			}
		});
		console.log(global_files_map);
		cb();
	});
}


function getFSPath(fragment) {
	return config.data_path + md5(fragment.fileName) + "-" + fragment.chunk_no;
}

function getFiles() {
	return global_files_map
}

function writeFragment(fragment) {
	var fsPath = getFSPath(fragment);
	var binFile = fsPath + ".bin";
	var metaFile = fsPath + "-meta.json";

	/* Update mean chunk */
	if (mean_chunk == 0) {
		mean_chunk = (fragment.end - fragment.start);
	} else {
		mean_chunk = (mean_chunk + (fragment.end - fragment.start)) / 2;
	}

	if (fs.existsSync(binFile)) {
		/* An existent replica of the file already exists. Just update the content */
		var meta = JSON.parse(fs.readFileSync(metaFile, 'utf8'));
		meta.replicas += 1;
		fs.writeFileSync(metaFile, JSON.stringify(meta, null, 2), 'utf8');
		if (global_files_map[md5(fragment.fileName)][fragment.chunk_no] == undefined) {
			global_files_map[md5(fragment.fileName)][fragment.chunk_no] = {}
		}
		global_files_map[md5(fragment.fileName)][fragment.chunk_no + ""].replicas++;
		global_files_map[md5(fragment.fileName)][fragment.chunk_no + ""].size += (fragment.end - fragment.start);
	} else {
		/* Create files for the chunk */
		var meta = {
			replicas: 1,
			total_chunks: fragment.total_chunks,
			fileName: fragment.fileName,
			chunk_no: fragment.chunk_no,
			start: fragment.start,
			end: fragment.end,
			sensitive: fragment.sensitive
		};
		fs.writeFileSync(metaFile, JSON.stringify(meta, null, 2), 'utf8');
		fs.writeFileSync(binFile, new Buffer(fragment.b64Content, 'base64'));
		if (global_files_map[md5(fragment.fileName)] == undefined) {
			global_files_map[md5(fragment.fileName)]= {};
		}
		global_files_map[md5(fragment.fileName)][fragment.chunk_no + ""] = {replicas: 1, size: fragment.end - fragment.start, sensitive: fragment.sensitive};
	}
	console.log(global_files_map);
}

function removeChunk(file, chunk) {
	var fsPath = config.data_path + file + "-" + chunk;
	var binFile = fsPath + ".bin";
	var metaFile = fsPath + "-meta.json";

	var meta = JSON.parse(fs.readFileSync(metaFile, 'utf8'));
	var buffer = fs.readFileSync(binFile).toString('base64');

	if (global_files_map[file][chunk].replicas == 1) {
		/* This is the last replica of the chunk. Delete the files */
		fs.unlinkSync(binFile);
		fs.unlinkSync(metaFile);
		delete global_files_map[file][chunk];
		if (Object.keys(global_files_map[file]).length == 0) {
			delete global_files_map[file]
		}
	} else {
		global_files_map[file][chunk].replicas --;
		global_files_map[file][chunk].size -= (meta.end - meta.start);

		/* Update meta on the file */
		meta.replicas--;
		fs.writeFileSync(metaFile, JSON.stringify(meta, null, 2), 'utf8');
	}

	/* Send the response */
	meta.b64Content = buffer;
	return meta;
}

function processNewChunk(meta) {
	virtual_size -= mean_chunk;
	if (virtual_size < 0) {
		virtual_size = 0;
	}
	writeFragment(meta);
}

function increaseVirtualStorage() {
	virtual_size += mean_chunk;
}

function getUsedSize() {
	var used_size = 0;
	for (var file in global_files_map) {
		for (var fragment in global_files_map[file]) {
			used_size += global_files_map[file][fragment].size;
		}
	}
	return used_size + virtual_size;
}

function getFileTotal(file_id) {
	if (global_files_map[file_id] == undefined || global_files_map[file_id] == {}) {
		return 0;
	}
	var chunk = Object.keys(global_files_map[file_id])[0];
	var metaPath = config.data_path + file_id + "-" + chunk + "-meta.json";
	var meta = JSON.parse(fs.readFileSync(metaPath, 'utf8'));
	return meta["total_chunks"];
}

function getFileSensitivity(file_id) {
	if (global_files_map[file_id] == undefined || global_files_map[file_id] == {}) {
		return -1;
	}
	var chunk = Object.keys(global_files_map[file_id])[0];
	var metaPath = config.data_path + file_id + "-" + chunk + "-meta.json";
	var meta = JSON.parse(fs.readFileSync(metaPath, 'utf8'));
	return meta["sensitive"];
}

function getFile(file_id) {
	if (global_files_map[file_id] == undefined) {
		return {};
	}
	res = {}
	for (var chunk in global_files_map[file_id]) {
		var fsPath = config.data_path + file_id + "-" + chunk;
		var binFile = fsPath + ".bin";
		res[chunk] = fs.readFileSync(binFile).toString('base64');
	}
	return res;
}

module.exports = {
  writeFragment: writeFragment,
  init: init,
  getUsedSize: getUsedSize,
  getFiles: getFiles,
  increaseVirtualStorage: increaseVirtualStorage,
  removeChunk: removeChunk,
  processNewChunk: processNewChunk,
  getFileTotal: getFileTotal,
  getFileSensitivity: getFileSensitivity,
  getFile: getFile
};

