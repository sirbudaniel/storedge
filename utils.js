function log(component, message) {
	console.log("[" + new Date().toISOString() + "] [" + component.toUpperCase() + "] [" + message + "]");
}

module.exports = {
  log: log
};
