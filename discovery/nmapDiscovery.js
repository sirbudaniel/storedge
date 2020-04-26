var nmap = require('node-nmap');
nmap.nmapLocation = "nmap";

function getAvailableHosts(range, callback) {
  var quickscan = new nmap.QuickScan(range.join(' '));
  quickscan.on('complete', function(data) {
    callback(null, data.map((elem) => elem.ip));
  });
  quickscan.on('error', function(error) {
    callback(error);
  });
  quickscan.startScan();
}

module.exports = {
  getAvailableHosts
}