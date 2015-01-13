/**
 *	Node Media Server
 *
 */
var net = require('net');
var NMConnection = require("nm_rtmp_conn");

var generateSessionID = function() {
    var possible = "abcdefghijklmnopqrstuvwxyz0123456789";
    var numPossible = possible.length;
    var SessionID = '';
    for (var i = 0; i < 12; i++) {
        SessionID += possible.charAt((Math.random() * numPossible) | 0);
    }
    return SessionID;
};

function NMServer() {
    this.port = 1935;
    this.conns = new Map();
    this.producers = new Map();
    this.rtmpServer;


    NMServer.prototype.run = function() {
        var $ = this;
        this.rtmpServer = new net.TcpServer(this.port, function(client) {
            var id = $.generateNewSessionID();
            console.log('[rtmp server] Get a client. id=' + id);
            var conn = new NMConnection(id, client, $.conns, $.producers);
            $.conns.set(id, conn);
            console.log('[rtmp server] Add conn to Map, conns=' + $.conns.size);
            conn.run();
            console.info("pub:" + conn.publishStreamName);
            if (conn.publishStreamName != null) {
                console.info("Delete publiser from producers. Stream name " + conn.publishStreamName);

                $.producers.delete(conn.publishStreamName);
                console.info("Send Stream EOF to publiser's consumers. Stream name " + conn.publishStreamName);
                conn.consumers.forEach(function(value, key) {
                    value.sendStreamEOF();
                    value.socket.close();
                });
            }

            console.info("play:" + conn.playStreamName);
            if (conn.playStreamName != null) {
                if ($.producers.has(conn.playStreamName)) {
                    console.info("Delete player from consumers. Stream name " + conn.playStreamName);
                    $.conns.get($.producers.get(conn.playStreamName)).consumers.delete(conn.id);
                }

            }
            $.conns.delete(id);
            conn = null;
            if ($.conns.size == 0) {
                GC();
            }
            console.log('[rtmp server] Client exit. id:' + id + ' Remove conn from Map. conns=' + $.conns.size);
        });
        console.log('[rtmp server] Starting Node Media Server on port ' + this.port);
        this.rtmpServer.run();
    };

    NMServer.prototype.generateNewSessionID = function() {
        var SessionID;
        do {
            SessionID = generateSessionID();
        } while (this.conns.has(SessionID))
        return SessionID;
    };
}

module.exports = NMServer;