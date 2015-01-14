var AMF = require('nm_rtmp_amf');
var RtmpHandshake = require('nm_rtmp_handshake');
var coroutine = require("coroutine");
var assert = require('assert');

var aac_sample_rates = [
    96000, 88200, 64000, 48000,
    44100, 32000, 24000, 22050,
    16000, 12000, 11025, 8000,
    7350, 0, 0, 0
];
var adts_header = new Buffer([0xff, 0xf1, 0x00, 0x00, 0x00, 0x0f, 0xfc]);
var NAL_HEADER = new Buffer([0, 0, 0, 1]);

var ReadUInt24BE = function(buf, offset) {
    if (offset == null) {
        offset = 0;
    }
    return (buf[0 + offset] << 16) + (buf[1 + offset] << 8) + buf[2 + offset];
}

var WriteUInt24BE = function(buf, value, offset) {
    if (offset == null) {
        offset = 0;
    }
    buf[offset] = (value >> 16) & 0xFF;
    buf[offset + 1] = (value >> 8) & 0xFF;
    buf[offset + 2] = value & 0xFF;
}

function NMConnection(id, client, conns, producers) {
    this.id = id;
    this.socket = client;
    this.conns = conns;
    this.producers = producers;
    this.isStarting = false;
    this.inChunkSize = 128;
    this.outChunkSize = 128;
    this.previousChunkMessage = {};
    this.connectCmdObj = null;
    this.isFirstAudioReceived = true;
    this.isFirstVideoReceived = true;
    this.lastAudioTimestamp = 0;
    this.lastVideoTimestamp = 0;

    this.codec = {
        width: 0,
        height: 0,
        duration: 0,
        framerate: 0,
        videodatarate: 0,
        audiosamplerate: 0,
        audiosamplesize: 0,
        audiodatarate: 0,
        spsLen: 0,
        sps: new Buffer(),
        ppsLen: 0,
        pps: new Buffer(),
    };

    this.publishStreamName = null;
    this.PlayStreamName = null;

    this.cacheAudioSequenceBuffer = new Buffer();
    this.cacheVideoSequenceBuffer = new Buffer();

    this.sendMessageQueue = new coroutine.BlockQueue(100);
    this.isReadyToPlay = false;

    NMConnection.prototype.run = function() {
        console.log('[NSConnection] run');
        this.handshake();
        console.log('[NSConnection] handshake success.');
        this.isStarting = true;
        this.parseRtmpPackage();
    };

    NMConnection.prototype.createRtmpMessage = function(rtmpHeader, rtmpBody) {
        var formatTypeID = 0;

        if (rtmpHeader.chunkStreamID == null) {
            console.warn("[rtmp] warning: createRtmpMessage(): chunkStreamID is not set for RTMP message");
        }
        if (rtmpHeader.timestamp == null) {
            console.warn("[rtmp] warning: createRtmpMessage(): timestamp is not set for RTMP message");
        }
        if (rtmpHeader.messageTypeID == null) {
            console.warn("[rtmp] warning: createRtmpMessage(): messageTypeID is not set for RTMP message");
        }
        if (rtmpHeader.messageStreamID == null) {
            console.warn("[rtmp] warning: createRtmpMessage(): messageStreamID is not set for RTMP message");
        }

        var bodyLength = rtmpBody.length;
        var type3Header = new Buffer([(3 << 6) | rtmpHeader.chunkStreamID]);

        var useExtendedTimestamp = false;
        var timestamp;

        if (rtmpHeader.timestamp >= 0xffffff) {
            useExtendedTimestamp = true;
            timestamp = [0xff, 0xff, 0xff];
        } else {
            timestamp = [(rtmpHeader.timestamp >> 16) & 0xff, (rtmpHeader.timestamp >> 8) & 0xff, rtmpHeader.timestamp & 0xff];
        }

        var bufs = new Buffer([(formatTypeID << 6) | rtmpHeader.chunkStreamID, timestamp[0], timestamp[1], timestamp[2], (bodyLength >> 16) & 0xff, (bodyLength >> 8) & 0xff, bodyLength & 0xff, rtmpHeader.messageTypeID, rtmpHeader.messageStreamID & 0xff, (rtmpHeader.messageStreamID >>> 8) & 0xff, (rtmpHeader.messageStreamID >>> 16) & 0xff, (rtmpHeader.messageStreamID >>> 24) & 0xff]);

        if (useExtendedTimestamp) {
            bufs.write(new Buffer([(rtmpHeader.timestamp >> 24) & 0xff, (rtmpHeader.timestamp >> 16) & 0xff, (rtmpHeader.timestamp >> 8) & 0xff, rtmpHeader.timestamp & 0xff]));
        }

        do {
            if (bodyLength > this.outChunkSize) {
                bufs.write(rtmpBody.slice(0, this.outChunkSize));
                bufs.write(type3Header);
                rtmpBody = rtmpBody.slice(this.outChunkSize);
                bodyLength -= this.outChunkSize;
            } else {
                bufs.write(rtmpBody.slice(0, bodyLength));
                bodyLength -= bodyLength;
            }
        } while (bodyLength > 0)

        return bufs;
    };

    NMConnection.prototype.handshake = function() {
        var ret = 0
        var c0c1 = this.socket.read(1537);
        if (c0c1.length != 1537) {
            console.warn("[rtmp handshake] read c0c1 failed.");
        }
        console.log('[rtmp handshake] read c0c1 success.');
        var s0s1s2 = RtmpHandshake.generateS0S1S2(c0c1);
        if (s0s1s2 != null) {
            //console.log("s0s1s2: "+s0s1s2.length);
            var ret = this.socket.write(s0s1s2);
            console.log('[rtmp handshake] write s0s1s2.');
        }
        var c2 = this.socket.read(1536);
        console.log('[rtmp handshake] read c2 success.');
    };

    NMConnection.prototype.parseRtmpPackage = function() {
        console.log('[NSConnection] parseRtmpPackage cycle start.');
        while (this.isStarting) {
            var message = {};
            var chunkMessageHeader = null;
            var previousChunk = null;
            try {
                //RTMP Chunk Format
                var chunkBasicHeader = this.socket.read(1);
                message.formatType = chunkBasicHeader[0] >> 6;
                message.chunkStreamID = chunkBasicHeader[0] & 0x3F;
                if (message.chunkStreamID == 0) {
                    message.chunkStreamID = this.socket.read(1) + 64;
                } else if (message.chunkStreamID == 1) {
                    message.chunkStreamID = (this.socket.read(1) << 8) + this.socket.read(1) + 64;
                }

                // console.log(message.chunkStreamID);
                //Chunk Message Header
                if (message.formatType == 0) {
                    //  console.log('message.formatType 0 ');

                    // Type 0 (11 bytes)
                    chunkMessageHeader = this.socket.read(11);
                    //  console.log(chunkMessageHeader.hex());
                    message.timestamp = ReadUInt24BE(chunkMessageHeader, 0);
                    message.timestampDelta = 0;
                    message.messageLength = ReadUInt24BE(chunkMessageHeader, 3);
                    message.messageTypeID = chunkMessageHeader[6];
                    message.messageStreamID = chunkMessageHeader.readInt32LE(7);
                } else if (message.formatType == 1) {
                    //   console.log('message.formatType 1 ');
                    // Type 1 (7 bytes)
                    chunkMessageHeader = this.socket.read(7);
                    message.timestampDelta = ReadUInt24BE(chunkMessageHeader, 0);
                    message.messageLength = ReadUInt24BE(chunkMessageHeader, 3);
                    message.messageTypeID = chunkMessageHeader[6]
                    previousChunk = this.previousChunkMessage[message.chunkStreamID];
                    if (previousChunk != null) {
                        message.timestamp = previousChunk.timestamp;
                        message.messageStreamID = previousChunk.messageStreamID;
                    } else {
                        throw new Error("Chunk reference error for type 1: previous chunk for id " + message.chunkStreamID + " is not found");
                    }
                } else if (message.formatType == 2) {
                    //    console.log('message.formatType 2 ');
                    // Type 2 (3 bytes)
                    chunkMessageHeader = this.socket.read(3);
                    message.timestampDelta = ReadUInt24BE(chunkMessageHeader, 0);
                    previousChunk = this.previousChunkMessage[message.chunkStreamID];
                    if (previousChunk != null) {
                        message.timestamp = previousChunk.timestamp
                        message.messageStreamID = previousChunk.messageStreamID
                        message.messageLength = previousChunk.messageLength
                        message.messageTypeID = previousChunk.messageTypeID
                    } else {
                        throw new Error("Chunk reference error for type 2: previous chunk for id " + message.chunkStreamID + " is not found");
                    }
                } else if (message.formatType == 3) {
                    //    console.log('message.formatType 3 ');
                    // Type 3 (0 byte)
                    chunkMessageHeader = new Buffer();
                    previousChunk = this.previousChunkMessage[message.chunkStreamID];
                    if (previousChunk != null) {
                        message.timestamp = previousChunk.timestamp;
                        message.messageStreamID = previousChunk.messageStreamID;
                        message.messageLength = previousChunk.messageLength;
                        message.timestampDelta = previousChunk.timestampDelta;
                        message.messageTypeID = previousChunk.messageTypeID;
                    } else {
                        throw new Error("Chunk reference error for type 3: previous chunk for id " + message.chunkStreamID + " is not found");
                    }
                } else {
                    throw new Error("Unknown format type: " + message.formatType);
                }

                //Extended Timestamp
                if (message.formatType === 0) {
                    if (message.timestamp === 0xffffff) {
                        var chunkBodyHeader = this.socket.read(4);
                        message.timestamp = (chunkBodyHeader[0] * Math.pow(256, 3)) + (chunkBodyHeader[1] << 16) + (chunkBodyHeader[2] << 8) + chunkBodyHeader[3];
                    }
                } else if (message.timestampDelta === 0xffffff) {
                    var chunkBodyHeader = this.socket.read(4);
                    message.timestampDelta = (chunkBodyHeader[0] * Math.pow(256, 3)) + (chunkBodyHeader[1] << 16) + (chunkBodyHeader[2] << 8) + chunkBodyHeader[3];

                }
                // console.log(message);
                var chunkBody = new Buffer();
                var needRead = message.messageLength;
                do {
                    if (needRead > this.inChunkSize) {
                        chunkBody.write(this.socket.read(this.inChunkSize));
                        needRead -= this.inChunkSize;
                        var AMFSizeHeader = this.socket.read(1)
                    } else {
                        chunkBody.write(this.socket.read(needRead));
                        needRead -= needRead;
                    }

                } while (needRead > 0)
                message.timestamp += message.timestampDelta;
                //console.info(message);
                this.previousChunkMessage[message.chunkStreamID] = message;
                this.handleRtmpMessage(message, chunkBody);

                chunkBody.dispose();
                chunkMessageHeader.dispose();
            } catch (e) {
                console.warn("parseRtmpPackage :" + e);
                this.isStarting = false;
            }
        }
        console.log('[NSConnection] parseRtmpPackage cycle stop.');
    };



    NMConnection.prototype.handleRtmpMessage = function(rtmpHeader, rtmpBody) {
        // console.log('handleRtmpMessage type:' + rtmpHeader.messageTypeID);
        switch (rtmpHeader.messageTypeID) {
            case 0x01:
                this.inChunkSize = rtmpBody.readUInt32BE(0);
                console.log('[rtmp handleRtmpMessage] Set In chunkSize:' + this.inChunkSize);
                break;

            case 0x04:
                var userControlMessage = this.parseUserControlMessage(rtmpBody);
                if (userControlMessage.eventType === 3) {
                    var streamID = (userControlMessage.eventData[0] << 24) + (userControlMessage.eventData[1] << 16) + (userControlMessage.eventData[2] << 8) + userControlMessage.eventData[3];
                    var bufferLength = (userControlMessage.eventData[4] << 24) + (userControlMessage.eventData[5] << 16) + (userControlMessage.eventData[6] << 8) + userControlMessage.eventData[7];
                    console.log("[rtmp handleRtmpMessage] SetBufferLength: streamID=" + streamID + " bufferLength=" + bufferLength);
                } else if (userControlMessage.eventType === 7) {
                    var timestamp = (userControlMessage.eventData[0] << 24) + (userControlMessage.eventData[1] << 16) + (userControlMessage.eventData[2] << 8) + userControlMessage.eventData[3];
                    console.log("[rtmp handleRtmpMessage] PingResponse: timestamp=" + timestamp);
                } else {
                    console.log("[rtmp handleRtmpMessage] User Control Message");
                    console.log(userControlMessage);
                }
                break;
            case 0x08:
                //Audio Data
                //console.log(rtmpHeader);
                // console.log('Audio Data: '+rtmpBody.length);\
                this.parseAudioMessage(rtmpHeader, rtmpBody);
                break;
            case 0x09:
                //Video Data
                // console.log(rtmpHeader);
                // console.log('Video Data: '+rtmpBody.length);
                this.parseVideoMessage(rtmpHeader, rtmpBody);
                break;
            case 0x12:
                //AMF0 Data
                var cmd = AMF.decodeAmf0Cmd(rtmpBody);
                this.handleAMFDataMessage(cmd);
                break;
            case 0x14:
                //AMF0 Command
                var cmd = AMF.decodeAmf0Cmd(rtmpBody);
                this.handleAMFCommandMessage(cmd);
                break;

        }
    };

    NMConnection.prototype.handleAMFDataMessage = function(cmd) {

        switch (cmd.cmd) {
            case '@setDataFrame':
                this.receiveSetDataFrame(cmd.method, cmd.cmdObj);
                break;
            default:
                console.warn("[rtmp:receive] unknown AMF data: " + dataMessage.objects[0].value);
        }
    };

    NMConnection.prototype.handleAMFCommandMessage = function(cmd) {
        //  console.log('[handleAMFCommandMessage] ' + cmd.cmd);
        switch (cmd.cmd) {
            case 'connect':
                this.connectCmdObj = cmd.cmdObj;
                this.windowACK(2500000);
                this.setPeerBandwidth(2500000, 2);
                this.outChunkSize = 4000;
                this.setChunkSize(this.outChunkSize);
                this.respondConnect();
                break;
            case 'createStream':
                this.respondCreateStream(cmd);
                break;
            case 'play':
                var streamName = this.connectCmdObj.app + '/' + cmd.streamName;
                console.debug("[rtmp streamPlay] Client want to play:" + streamName);
                this.respondPlay();
                this.playStreamName = streamName;

                if (!this.producers.has(streamName)) {
                    console.debug("[rtmp streamPlay]  There's no stream named " + streamName + " is publushing! Create a producer.");
                    this.producers.set(streamName, {
                        id: null,
                        consumers: new Map()
                    });
                } else if (this.producers.get(streamName).id == null) {
                    console.debug("[rtmp streamPlay]  There's no stream named " + streamName + " is publushing! But the producer is created.");
                } else {
                    console.debug("[rtmp streamPlay]  There's a stream named " + streamName + " is publushing! id=" + this.producers.get(streamName).id);
                    this.isReadyToPlay = true;
                }
                this.producers.get(streamName).consumers.set(this.id, this);
                var rtmpMessage = new Buffer("020000000000060400000000000000000001", 'hex');
                this.sendMessageQueue.offer(rtmpMessage);
                if (this.isReadyToPlay) {
                    this.startPlay(this.conns.get(this.producers.get(streamName).id));
                }
                break;
            case 'closeStream':
                this.closeStream();
                break;
            case 'deleteStream':
                this.deleteStream();
                break;
            case 'pause':
                console.log('pause received');
                this.pauseOrUnpauseStream();
                break;
            case 'releaseStream':
                this.respondReleaseStream();
                break;
            case 'FCPublish':
                this.respondFCPublish();
                break;
            case 'publish':
                var streamName = this.connectCmdObj.app + '/' + cmd.name;
                console.debug("[rtmp publish] A client want to publish a stream named " + streamName);
                this.publishStreamName = streamName;
                if (!this.producers.has(streamName)) {
                    this.producers.set(streamName, {
                        id: this.id,
                        consumers: new Map()
                    });
                } else if (this.producers.get(streamName).id == null) {
                    this.producers.get(streamName).id = this.id;
                } else {
                    console.warn("[rtmp publish] Already has a stream named " + streamName);
                    this.respondPublishError();
                    return;
                }
                this.consumers = this.producers.get(streamName).consumers;
                this.respondPublish();
                break;
            case 'FCUnpublish':
                this.respondFCUnpublish();
                break;
            default:
                console.warn("[rtmp:receive] unknown AMF command: " + cmd.cmd);
                break;

        }
    };

    NMConnection.prototype.windowACK = function(size) {
        var rtmpBuffer = new Buffer('02000000000004050000000000000000', 'hex');
        rtmpBuffer.writeUInt32BE(size, 12);
        // console.log('windowACK: '+rtmpBuffer.hex());
        this.socket.write(rtmpBuffer);
    };

    NMConnection.prototype.setPeerBandwidth = function(size, type) {
        var rtmpBuffer = new Buffer('0200000000000506000000000000000000', 'hex');
        rtmpBuffer.writeUInt32BE(size, 12);
        rtmpBuffer[16] = type;
        // console.log('setPeerBandwidth: '+rtmpBuffer.hex());
        this.socket.write(rtmpBuffer);
    };

    NMConnection.prototype.setChunkSize = function(size) {
        var rtmpBuffer = new Buffer('02000000000004010000000000000000', 'hex');
        rtmpBuffer.writeUInt32BE(size, 12);
        // console.log('setChunkSize: '+rtmpBuffer.hex());
        this.socket.write(rtmpBuffer);
    };

    NMConnection.prototype.respondConnect = function() {
        var rtmpHeader = {
            chunkStreamID: 3,
            timestamp: 0,
            messageTypeID: 0x14,
            messageStreamID: 0
        };
        var opt = {
            cmd: '_result',
            transId: 1,
            cmdObj: {
                fmsVer: 'FMS/3,0,1,123',
                capabilities: 31
            },
            info: {
                level: 'status',
                code: 'NetConnection.Connect.Success',
                description: 'Connection succeeded.',
                objectEncoding: 0
            }
        };
        var rtmpBody = AMF.encodeAmf0Cmd(opt);
        var rtmpMessage = this.createRtmpMessage(rtmpHeader, rtmpBody);
        this.socket.write(rtmpMessage);
    };

    NMConnection.prototype.respondRejectConnect = function(first_argument) {
        var rtmpHeader = {
            chunkStreamID: 3,
            timestamp: 0,
            messageTypeID: 0x14,
            messageStreamID: 0
        };

        var opt = {
            cmd: '_error',
            transId: 1,
            cmdObj: {
                fmsVer: 'FMS/3,0,1,123',
                capabilities: 31
            },
            info: {
                level: 'error',
                code: 'NetConnection.Connect.Rejected',
                description: 'Connection failed.',
                objectEncoding: 0
            }
        };
        var rtmpBody = AMF.encodeAmf0Cmd(opt);
        var rtmpMessage = this.createRtmpMessage(rtmpHeader, rtmpBody);
        this.socket.write(rtmpMessage);
    };

    NMConnection.prototype.respondCreateStream = function(cmd) {
        // console.log(cmd);
        //var rtmpBuffer = new Buffer('0300000000001d14000000000200075f726573756c7400401000000000000005003ff0000000000000', 'hex');
        //this.socket.write(rtmpBuffer);
        var rtmpHeader = {
            chunkStreamID: 3,
            timestamp: 0,
            messageTypeID: 0x14,
            messageStreamID: 0
        };
        var opt = {
            cmd: "_result",
            transId: cmd.transId,
            cmdObj: null,
            info: 1,
            objectEncoding: 0

        };
        var rtmpBody = AMF.encodeAmf0Cmd(opt);
        var rtmpMessage = this.createRtmpMessage(rtmpHeader, rtmpBody);
        this.socket.write(rtmpMessage);

    };

    NMConnection.prototype.respondPlay = function() {
        var rtmpHeader = {
            chunkStreamID: 3,
            timestamp: 0,
            messageTypeID: 0x14,
            messageStreamID: 1
        };
        var opt = {
            cmd: 'onStatus',
            transId: 0,
            cmdObj: null,
            info: {
                level: 'status',
                code: 'NetStream.Play.Start',
                description: 'Start live'
            }
        };
        var rtmpBody = AMF.encodeAmf0Cmd(opt);
        var rtmpMessage = this.createRtmpMessage(rtmpHeader, rtmpBody);
        this.socket.write(rtmpMessage);

        var rtmpHeader = {
            chunkStreamID: 5,
            timestamp: 0,
            messageTypeID: 0x12,
            messageStreamID: 1
        };
        var opt = {
            cmd: '|RtmpSampleAccess',
            bool1: true,
            bool2: true
        };

        var rtmpBody = AMF.encodeAmf0Cmd(opt);
        var rtmpMessage = this.createRtmpMessage(rtmpHeader, rtmpBody);
        this.socket.write(rtmpMessage);
    };

    NMConnection.prototype.startPlay = function(producer) {

        if (producer.metaData != null) {
            var rtmpHeader = {
                chunkStreamID: 5,
                timestamp: 0,
                messageTypeID: 0x12,
                messageStreamID: 1
            };

            var opt = {
                cmd: 'onMetaData',
                cmdObj: producer.metaData
            };

            var rtmpBody = AMF.encodeAmf0Cmd(opt);
            var rtmpMessage = this.createRtmpMessage(rtmpHeader, rtmpBody);
            this.sendMessageQueue.offer(rtmpMessage);
        }

        var rtmpHeader = {
            chunkStreamID: 4,
            timestamp: 0,
            messageTypeID: 0x08,
            messageStreamID: 1
        };
        var rtmpMessage = this.createRtmpMessage(rtmpHeader, producer.cacheAudioSequenceBuffer);
        this.sendMessageQueue.offer(rtmpMessage);

        var rtmpHeader = {
            chunkStreamID: 4,
            timestamp: 0,
            messageTypeID: 0x09,
            messageStreamID: 1
        };
        var rtmpMessage = this.createRtmpMessage(rtmpHeader, producer.cacheVideoSequenceBuffer);
        this.sendMessageQueue.offer(rtmpMessage);
        this.doSendRtmpMessage.start(this);
    };

    NMConnection.prototype.closeStream = function() {

    };

    NMConnection.prototype.deleteStream = function() {

    };

    NMConnection.prototype.pauseOrUnpauseStream = function() {

    };

    NMConnection.prototype.respondReleaseStream = function() {

    };

    NMConnection.prototype.respondFCPublish = function() {

    };

    NMConnection.prototype.respondPublish = function() {
        var rtmpHeader = {
            chunkStreamID: 5,
            timestamp: 0,
            messageTypeID: 0x14,
            messageStreamID: 1
        };
        var opt = {
            cmd: 'onStatus',
            transId: 0,
            cmdObj: null,
            info: {
                level: 'status',
                code: 'NetStream.Publish.Start',
                description: 'Start publishing'
            }
        };
        var rtmpBody = AMF.encodeAmf0Cmd(opt);
        var rtmpMessage = this.createRtmpMessage(rtmpHeader, rtmpBody);
        this.socket.write(rtmpMessage);

    };

    NMConnection.prototype.respondPublishError = function() {
        var rtmpHeader = {
            chunkStreamID: 5,
            timestamp: 0,
            messageTypeID: 0x14,
            messageStreamID: 1
        };
        var opt = {
            cmd: 'onStatus',
            transId: 0,
            cmdObj: null,
            info: {
                level: 'error',
                code: 'NetStream.Publish.BadName',
                description: 'Already publishing'
            }
        };
        var rtmpBody = AMF.encodeAmf0Cmd(opt);
        var rtmpMessage = this.createRtmpMessage(rtmpHeader, rtmpBody);
        this.socket.write(rtmpMessage);
    };

    NMConnection.prototype.respondFCUnpublish = function() {

    };

    NMConnection.prototype.receiveSetDataFrame = function(method, obj) {
        console.log('[receiveSetDataFrame] method:' + method);
        this.metaData = obj;

        var rtmpHeader = {
            chunkStreamID: 5,
            timestamp: 0,
            messageTypeID: 0x12,
            messageStreamID: 1
        };
        var opt = {
            cmd: 'onMetaData',
            cmdObj: obj
        };

        var rtmpBody = AMF.encodeAmf0Cmd(opt);
        var rtmpMessage = this.createRtmpMessage(rtmpHeader, rtmpBody);

        this.consumers.forEach(function(conn, id) {
            if (!conn.isReadyToPlay) {
                conn.doSendRtmpMessage.start(conn);
                conn.isReadyToPlay = true;
            }
            console.log("on metadata put onMetadata to queue:");
            conn.sendMessageQueue.offer(rtmpMessage);
        });
    };

    NMConnection.prototype.createUserControlMessage = function(rtmpHeader, rtmpBody) {
        var formatTypeID = 0;

        if (rtmpHeader.chunkStreamID == null) {
            console.warn("[rtmp] warning: createRtmpMessage(): chunkStreamID is not set for RTMP message");
        }
        if (rtmpHeader.timestamp == null) {
            console.warn("[rtmp] warning: createRtmpMessage(): timestamp is not set for RTMP message");
        }
        if (rtmpHeader.messageTypeID == null) {
            console.warn("[rtmp] warning: createRtmpMessage(): messageTypeID is not set for RTMP message");
        }
        if (rtmpHeader.messageStreamID == null) {
            console.warn("[rtmp] warning: createRtmpMessage(): messageStreamID is not set for RTMP message");
        }

        var bodyLength = rtmpBody.length;
        var type3Header = new Buffer([(3 << 6) | rtmpHeader.chunkStreamID]);
        var useExtendedTimestamp = false;
        var timestamp;

        if (rtmpHeader.timestamp >= 0xffffff) {
            useExtendedTimestamp = true;
            timestamp = [0xff, 0xff, 0xff];
        } else {
            timestamp = [(rtmpHeader.timestamp >> 16) & 0xff, (rtmpHeader.timestamp >> 8) & 0xff, rtmpHeader.timestamp & 0xff];
        }

        var bufs = new Buffer([(formatTypeID << 6) | rtmpHeader.chunkStreamID, timestamp[0], timestamp[1], timestamp[2], (bodyLength >> 16) & 0xff, (bodyLength >> 8) & 0xff, bodyLength & 0xff, rtmpHeader.messageTypeID, rtmpHeader.messageStreamID & 0xff, (rtmpHeader.messageStreamID >>> 8) & 0xff, (rtmpHeader.messageStreamID >>> 16) & 0xff, (rtmpHeader.messageStreamID >>> 24) & 0xff]);

    };

    NMConnection.prototype.parseUserControlMessage = function(buf) {
        var eventData, eventType;
        var eventType = (buf[0] << 8) + buf[1];
        var eventData = buf.slice(2);
        var message = {
            eventType: eventType,
            eventData: eventData
        };
        if (eventType === 3) {
            message.streamID = (eventData[0] << 24) + (eventData[1] << 16) + (eventData[2] << 8) + eventData[3];
            message.bufferLength = (eventData[4] << 24) + (eventData[5] << 16) + (eventData[6] << 8) + eventData[7];
        }
        return message;
    };



    NMConnection.prototype.parseAudioMessage = function(rtmpHeader, rtmpBody) {
        if (this.isFirstAudioReceived) {
            var sound_format = rtmpBody[0];
            var sound_type = sound_format & 0x01;
            var sound_size = (sound_format >> 1) & 0x01;
            var sound_rate = (sound_format >> 2) & 0x03;
            sound_format = (sound_format >> 4) & 0x0f;
            if (sound_format != 10) {
                console.error("Only support audio aac codec. actual=" + sound_format);
                return -1;
            }
            console.debug("Parse AudioTagHeader sound_format=" + sound_format + " sound_type=" + sound_type + " sound_size=" + sound_size + " sound_rate=" + sound_rate);
            var aac_packet_type = rtmpBody[1];
            if (aac_packet_type == 0) {
                //AudioSpecificConfig
                // only need to decode  2bytes:
                // audioObjectType, aac_profile, 5bits.
                // samplingFrequencyIndex, aac_sample_rate, 4bits.
                // channelConfiguration, aac_channels, 4bits
                this.codec.aac_profile = rtmpBody[2];
                this.codec.aac_sample_rate = rtmpBody[3];

                this.codec.aac_channels = (this.codec.aac_sample_rate >> 3) & 0x0f;
                this.codec.aac_sample_rate = ((this.codec.aac_profile << 1) & 0x0e) | ((this.codec.aac_sample_rate >> 7) & 0x01);
                this.codec.aac_profile = (this.codec.aac_profile >> 3) & 0x1f;
                this.codec.audiosamplerate = aac_sample_rates[this.codec.aac_sample_rate];
                if (this.codec.aac_profile == 0 || this.codec.aac_profile == 0x1f) {
                    console.error("Parse audio aac sequence header failed, adts object=" + this.codec.aac_profile + "invalid.");
                    return -1;
                }
                this.codec.aac_profile--;
                console.info("Parse audio aac sequence header success! ");
                // console.debug(this.codec);
                this.isFirstAudioReceived = false;
                this.cacheAudioSequenceBuffer.write(rtmpBody);
                var sendRtmpHeader = {
                    chunkStreamID: 4,
                    timestamp: 0,
                    messageTypeID: 0x08,
                    messageStreamID: 1
                };
                var rtmpMessage = this.createRtmpMessage(sendRtmpHeader, rtmpBody);
                this.consumers.forEach(function(conn, id) {
                    if (!conn.isReadyToPlay) {
                        conn.doSendRtmpMessage.start(conn);
                        conn.isReadyToPlay = true;
                    }
                    conn.sendMessageQueue.offer(rtmpMessage);
                });
            }

        } else {
            var sendRtmpHeader = {
                chunkStreamID: 4,
                timestamp: rtmpHeader.timestamp,
                messageTypeID: 0x08,
                messageStreamID: 1
            };
            var rtmpMessage = this.createRtmpMessage(sendRtmpHeader, rtmpBody);
            this.consumers.forEach(function(conn, id) {
                conn.sendMessageQueue.offer(rtmpMessage);
            });

            /* 
            var frame_length = rtmpBody.length - 2 + 7;
            var audioBuffer = new Buffer(frame_length);
            adts_header.copy(audioBuffer);
            audioBuffer[2] = (this.codec.aac_profile << 6) & 0xc0;
            // sampling_frequency_index 4bits
            audioBuffer[2] |= (this.codec.aac_sample_rate << 2) & 0x3c;
            // channel_configuration 3bits
            audioBuffer[2] |= (this.codec.aac_channels >> 2) & 0x01;
            audioBuffer[3] = (this.codec.aac_channels << 6) & 0xc0;
            // frame_length 13bits
            audioBuffer[3] |= (frame_length >> 11) & 0x03;
            audioBuffer[4] = (frame_length >> 3) & 0xff;
            audioBuffer[5] = ((frame_length << 5) & 0xe0);
            // adts_buffer_fullness; //11bits
            audioBuffer[5] |= 0x1f;
            //console.log(adts_header.hex());

            rtmpBody.copy(audioBuffer, 7, 2, rtmpBody.length - 2);
            return audioBuffer;
            */
        }
    };


    NMConnection.prototype.parseVideoMessage = function(rtmpHeader, rtmpBody) {
        var index = 0;
        var frame_type = rtmpBody[0];
        var codec_id = frame_type & 0x0f;
        frame_type = (frame_type >> 4) & 0x0f;
        // only support h.264/avc
        if (codec_id != 7) {
            console.error("Only support video h.264/avc codec. actual=" + codec_id);
            return -1;
        }
        var avc_packet_type = rtmpBody[1];
        var composition_time = ReadUInt24BE(rtmpBody, 2);
        //  printf("v composition_time %d\n",composition_time);

        if (avc_packet_type == 0) {
            if (this.isFirstVideoReceived) {
                //AVC sequence header
                var configurationVersion = rtmpBody[5];
                this.codec.avc_profile = rtmpBody[6];
                var profile_compatibility = rtmpBody[7];
                this.codec.avc_level = rtmpBody[8];
                var lengthSizeMinusOne = rtmpBody[9];
                lengthSizeMinusOne &= 0x03;
                this.codec.NAL_unit_length = lengthSizeMinusOne;

                //  sps
                var numOfSequenceParameterSets = rtmpBody[10];
                numOfSequenceParameterSets &= 0x1f;

                if (numOfSequenceParameterSets != 1) {
                    console.error("Decode video avc sequenc header sps failed.\n");
                    return -1;
                }

                this.codec.spsLen = rtmpBody.readUInt16BE(11);

                index = 11 + 2;
                if (this.codec.spsLen > 0) {
                    this.codec.sps.resize(this.codec.spsLen);
                    rtmpBody.copy(this.codec.sps, 0, 13, 13 + this.codec.spsLen);
                }
                // pps
                index += this.codec.spsLen;
                var numOfPictureParameterSets = rtmpBody[index];
                numOfPictureParameterSets &= 0x1f;
                if (numOfPictureParameterSets != 1) {
                    console.error("Decode video avc sequenc header pps failed.\n");
                    return -1;
                }

                index++;
                this.codec.ppsLen = rtmpBody.readUInt16BE(index);
                index += 2;
                if (this.codec.ppsLen > 0) {
                    this.codec.pps.resize(this.codec.ppsLen);
                    rtmpBody.copy(this.codec.pps, 0, index, index + this.codec.ppsLen);
                }
                this.isFirstVideoReceived = false;

                console.info("Parse video avc sequence header success! ");
                // console.debug(this.codec);
                // console.info('sps: ' + this.codec.sps.hex());
                // console.info('pps: ' + this.codec.pps.hex());
                this.cacheVideoSequenceBuffer.write(rtmpBody);

                var sendRtmpHeader = {
                    chunkStreamID: 4,
                    timestamp: 0,
                    messageTypeID: 0x09,
                    messageStreamID: 1
                };
                var rtmpMessage = this.createRtmpMessage(sendRtmpHeader, rtmpBody);
                this.consumers.forEach(function(conn, id) {
                    if (!conn.isReadyToPlay) {
                        conn.doSendRtmpMessage.start(conn);
                        conn.isReadyToPlay = true;
                    }
                    conn.sendMessageQueue.offer(rtmpMessage);
                });
            }
        } else if (avc_packet_type == 1) {
            var sendRtmpHeader = {
                chunkStreamID: 4,
                timestamp: rtmpHeader.timestamp,
                messageTypeID: 0x09,
                messageStreamID: 1
            };
            var rtmpMessage = this.createRtmpMessage(sendRtmpHeader, rtmpBody);
            this.consumers.forEach(function(conn, id) {
                conn.sendMessageQueue.offer(rtmpMessage);
            });
            /*
            //AVC NALU
            var NALUnitLength = 0;
            if (this.codec.NAL_unit_length == 3) {
                NALUnitLength = rtmpBody.readUInt32BE(5);
            } else if (this.codec.NAL_unit_length == 2) {
                NALUnitLength = ReadUInt24BE(rtmpBody, 5);
            } else if (this.codec.NAL_unit_length == 1) {
                NALUnitLength = rtmpBody.readUInt16BE(5);
            } else {
                NALUnitLength = rtmpBody.readInt8(5);
            }

            var videoBufferLen = 0;
            var videoBuffer = null;
            if (frame_type == 1) {
                videoBufferLen = 4 + this.codec.spsLen + 4 + this.codec.ppsLen + 4 + NALUnitLength;
                videoBuffer = new Buffer(videoBufferLen);
                NAL_HEADER.copy(videoBuffer);
                this.codec.sps.copy(videoBuffer, 4);
                NAL_HEADER.copy(videoBuffer, 4 + this.codec.spsLen);
                this.codec.pps.copy(videoBuffer, 4 + this.codec.spsLen + 4);
                NAL_HEADER.copy(videoBuffer, 4 + this.codec.spsLen + 4 + this.codec.ppsLen);
                rtmpBody.copy(videoBuffer, 4 + this.codec.spsLen + 4 + this.codec.ppsLen + 4, 9, NALUnitLength);
            } else {
                NAL_HEADER.copy(videoBuffer);
                rtmpBody.copy(videoBuffer, 4, 9, NALUnitLength);
            }
            return videoBuffer;
            */
        } else {
            //AVC end of sequence (lower level NALU sequence ender is not required or supported)
        }
    };

    NMConnection.prototype.sendStreamEOF = function() {
        var rtmpBuffer = new Buffer("020000000000060400000000000100000001", 'hex');
        this.socket.write(rtmpBuffer);
    };

    NMConnection.prototype.doSendRtmpMessage = function(_this) {
        console.log("doSendRtmpMessage is stating! id:" + _this.id);
        while (_this.isStarting) {
            var rtmpMessage = _this.sendMessageQueue.take();
            if (rtmpMessage == null) {
                break;
            }
            try {
                _this.socket.write(rtmpMessage);
            } catch (e) {
                console.warn('doSendRtmpMessage:' + e);
            }
        }
        console.log("doSendRtmpMessage is stop! id:" + _this.id);
    };
}

module.exports = NMConnection;