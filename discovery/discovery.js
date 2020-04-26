var os = require('os');
var ipAddress = require('ip-address');
var ip = require('ip');
var async = require('async');
var request = require('request');
var nmap = require('./nmapDiscovery.js');

var currentHosts = [];
var neighbours = [];
var config = {};
var myself = {
  ip: '',
  port: -1,
  sensitive: 0
}

function getIpAddress(callback) {
  var ifaces = os.networkInterfaces();
  for (var ifname in ifaces) {
    var ifaceArr = ifaces[ifname];
    for (var i in ifaceArr) {
      var iface = ifaceArr[i];
      if (iface.family === 'IPv4' && iface.internal == false) {
        callback(ifname, iface);
        return;
      }
    }
  }
}

function scan(options, callback) {
  var results = [];
  var expectedResponses = options.range.length;
  var arrivedResponses = 0;
  var range = options.range;
  var ports = options.ports;
  async.eachOfLimit(range, config.max_ips, function(ip, index, callbackIp) {
    async.eachOf(ports, function(port, index, callbackPort) {
      request({
        method: 'GET',
        url: `http://${ip}:${port}/alive`,
        timeout: 3000
      }, function(error, response, body) {
        if (!error) {
          let obj = JSON.parse(body);
          results.push({
            ip: ip,
            port: port,
            sensitive: obj.sensitivity
          });
        }
        callbackPort();
      });
    }, function(err) {
      if (err) console.error(err);
      callbackIp();
    });
  }, function(err) {
    callback(results);
  });
}

function scanPeriodically(ifname, iface) {
  var cidr = ip.cidrSubnet(iface.cidr);
  var startIp = ip.toLong(cidr.firstAddress);
  var endIp = ip.toLong(cidr.lastAddress);
  var range = [];
  for (var i = startIp; i < endIp; i++) {
    range.push(ip.fromLong(i));
  }
  var ports = [];
  for (var i = config.scan_port_range.from; i < config.scan_port_range.to; i++) {
    ports.push(i);
  }
  nmap.getAvailableHosts(range, (err, newRange) => {
    if (err) {
      console.log("error ", err, "happened when nmap was discovering")
      newRange = range;
    } else {
      //console.log("new available IP range is: ", newRange);
    }
    var options = {
      range: newRange,
      ports: ports
    };
    //console.log("Start scanning");
    scan(options, (results) => {
      console.log("Scanning complete all hosts: ", results);
      update(results);
    });
  });
}

function hostsEquals(host1, host2) {
  return host1.ip == host2.ip && host1.port == host2.port;
}

function getDifference(hosts1, hosts2) {
  var diff = [];
  for (var i in hosts1) {
    var host1 = hosts1[i];
    var found = false;
    for (var j in hosts2) {
      var host2 = hosts2[j];
      if (hostsEquals(host1, host2)) {
        found = true;
        break;
      }
    }
    if (!found) {
      diff.push(host1);
    }
  }
  return diff;
}

function haveToChoose(allNewHosts) {
  for (var i in allNewHosts) {
    var host = allNewHosts[i];
    if (!hostsEquals(host, myself) && !host.chosenByHim) {
      return true;
    }
  }
  return false;
}

function chooseNeighbour(allNewHosts) {
  if (myself.sensitive == 1) { // try to find also reliable nodes
    var reliableHosts = allNewHosts.filter(host => host.sensitive == 1);
    if (reliableHosts.length != 0) {
      return reliableHosts[Math.floor(Math.random() * reliableHosts.length)];
    }
  }
  return allNewHosts[Math.floor(Math.random() * allNewHosts.length)];
}

function findNeighbourToLink(currentHosts) {
  var allNewHosts = getDifference(currentHosts, neighbours);
  var chosenNeighbour = chooseNeighbour(allNewHosts);
  return chosenNeighbour;
}

function update(newHosts) {
  currentHosts = newHosts;

  if (currentHosts.length == 1 || neighbours.length >= 3) {
    if (currentHosts.length == 1) {
      //console.log("I am the only one");
    } else {
      //console.log("I already have enough neighbours");
    }
    return;
  }
  var newNeighbour = findNeighbourToLink(currentHosts);
  neighbours.push(newNeighbour);
  request({
    method: 'POST',
    uri: `http://${newNeighbour.ip}:${newNeighbour.port}/registerNeighbour`,
    json: myself
  }, function(error, response, body) {
    if (!error) {
      //console.log("body", body);
    }
  });
}

setInterval(() => {
  //console.log('My neighbours are: ', getNeighbours());
}, 5000);

function init(expressApp, cfg) {
  config = cfg;
  getIpAddress((ifname, iface) => {
    console.log(ifname, iface);

    myself.ip = iface.address;
    myself.port = config.port;
    myself.sensitive = config.sensitive;
    console.log("Myself is: ", myself);
    neighbours.push(myself);

    scanPeriodically(ifname, iface);
    setInterval(() => {
      //console.log("Scanning again!");
      scanPeriodically(ifname, iface);
    }, config.scan_ms);
  });

  expressApp.get('/alive', (req, res) => {
    res.status(200).send({'sensitivity': myself.sensitive});
  });
  expressApp.post('/registerNeighbour', (req, res) => {
    neighbours.push(req.body);
    res.sendStatus(200);
  });
}

function getNeighbours() {
  return neighbours.filter((neighbour) => !hostsEquals(neighbour, myself));
}

function getMyself() {
  return myself;
}

module.exports = {
  init: init,
  getNeighbours: getNeighbours,
  getMyself: getMyself
};
