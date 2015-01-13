var NMServer = require("nm_server");
var server = new NMServer();
server.port = 1935;
server.run();