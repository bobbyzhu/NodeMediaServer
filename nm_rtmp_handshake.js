var Hash = require('hash');
var Crypto = require('crypto');

var MESSAGE_FORMAT_0 = 0;
var MESSAGE_FORMAT_1 = 1;
var MESSAGE_FORMAT_2 = 2;

var RTMP_SIG_SIZE = 1536;
var SHA256DL = 32;
var KEY_LENGTH = 128;

var RandomCrud = new Buffer([
    0xf0, 0xee, 0xc2, 0x4a, 0x80, 0x68, 0xbe, 0xe8,
    0x2e, 0x00, 0xd0, 0xd1, 0x02, 0x9e, 0x7e, 0x57,
    0x6e, 0xec, 0x5d, 0x2d, 0x29, 0x80, 0x6f, 0xab,
    0x93, 0xb8, 0xe6, 0x36, 0xcf, 0xeb, 0x31, 0xae
]);
var GenuineFMSConst = "Genuine Adobe Flash Media Server 001";
var GenuineFMSConstCrud = new Buffer();
GenuineFMSConstCrud.write(GenuineFMSConst, 'utf8');
GenuineFMSConstCrud.write(RandomCrud);

var GenuineFPConst = "Genuine Adobe Flash Player 001";
var GenuineFPConstCrud = new Buffer();
GenuineFPConstCrud.write(GenuineFPConst, 'utf8');
GenuineFPConstCrud.write(RandomCrud);


function calcHmac(data, key) {
    var hmac = Hash.hmac_sha256(key);
    hmac.update(data);
    return hmac.digest();
};

function GetClientGenuineConstDigestOffset(buf) {
    var offset = buf[0] + buf[1] + buf[2] + buf[3];
    offset = (offset % 728) + 12;
    return offset;
};

function GetServerGenuineConstDigestOffset(buf) {
    var offset = buf[0] + buf[1] + buf[2] + buf[3];
    offset = (offset % 728) + 776;
    return offset;
};

function GetClientDHOffset(buf) {
    var offset = buf[0] + buf[1] + buf[2] + buf[3];
    offset = (offset % 632) + 772;
    return offset;
};

function GetServerDHOffset(buf) {
    var offset = buf[0] + buf[1] + buf[2] + buf[3];
    offset = (offset % 632) + 8;
    return offset;
};

function hasSameBytes(buf1, buf2) {
    for (var i = 0; i < buf1.length; i++) {
        if (buf1[i] !== buf2[i]) {
            return false;
        }
    }
    return true;
};

function detectClientMessageFormat(clientsig) {
    var sdl = GetServerGenuineConstDigestOffset(clientsig.slice(772, 776));
    var msg = new Buffer();
    msg.write(clientsig.slice(0, sdl));
    msg.write(clientsig.slice(sdl + SHA256DL));
    var computedSignature = calcHmac(msg, GenuineFPConst);
    var providedSignature = clientsig.slice(sdl, sdl + SHA256DL);
    if (hasSameBytes(computedSignature, providedSignature)) {
        return MESSAGE_FORMAT_2;
    }
    sdl = GetClientGenuineConstDigestOffset(clientsig.slice(8, 12));
    msg.dispose();
    var msg = new Buffer();
    msg.write(clientsig.slice(0, sdl));
    msg.write(clientsig.slice(sdl + SHA256DL));

    computedSignature = calcHmac(msg, GenuineFPConst);
    providedSignature = clientsig.slice(sdl, sdl + SHA256DL);

    if (hasSameBytes(computedSignature, providedSignature)) {
        return MESSAGE_FORMAT_1;
    }
    return MESSAGE_FORMAT_0;
};


function generateS1(messageFormat) {
    var s1Bytes = new Buffer([0, 0, 0, 0, 1, 2, 3, 4]);
    var randomBytes = Crypto.randomBytes(RTMP_SIG_SIZE - 8);
    s1Bytes.write(randomBytes);

    var serverDigestOffset;
    if (messageFormat == 1) {
        serverDigestOffset = GetClientGenuineConstDigestOffset(s1Bytes.slice(8, 12));
    } else {
        serverDigestOffset = GetServerGenuineConstDigestOffset(s1Bytes.slice(772, 776));
    }

    var msg = s1Bytes.slice(0, serverDigestOffset);
    msg.write(s1Bytes.slice(serverDigestOffset + SHA256DL));
    var hash = calcHmac(msg, GenuineFMSConst);
    hash.copy(s1Bytes, serverDigestOffset, 0, 32);
    return s1Bytes;
};

function generateS2(messageFormat, clientsig) {
    var s2Bytes = Crypto.randomBytes(RTMP_SIG_SIZE - 32);
    var challengeKeyOffset;
    if (messageFormat === 1) {
        challengeKeyOffset = GetClientGenuineConstDigestOffset(clientsig.slice(8, 12));
    } else {
        challengeKeyOffset = GetServerGenuineConstDigestOffset(clientsig.slice(772, 776));
    }
    var challengeKey = clientsig.slice(challengeKeyOffset, challengeKeyOffset + 32);
    var hash = calcHmac(challengeKey, GenuineFMSConstCrud);
    var signature = calcHmac(s2Bytes, hash);
    s2Bytes.write(signature);
    return s2Bytes;
};


function generateS0S1S2(clientsig) {
    var clientType = clientsig[0];
    console.log("[rtmp handshake] client type: " + clientType);
    if (clientType != 0x03) {
        console.error("[rtmp handshake] only support rtmp plain text. ret=%d", ret);
        return null;
    }
    clientsig = clientsig.slice(1);
    var messageFormat = detectClientMessageFormat(clientsig);
    console.log('[rtmp handshake] client message format = ' + messageFormat);
    var s0s1s2 = new Buffer(1);
    s0s1s2[0] = clientType;
    if (messageFormat == MESSAGE_FORMAT_0) {
        console.log('[rtmp handshake] using simple handshake.');
        s0s1s2.write(clientsig);
        s0s1s2.write(clientsig);
    } else {
        console.log('[rtmp handshake] using complex handshake.');
        s0s1s2.write(generateS1(messageFormat));
        s0s1s2.write(generateS2(messageFormat, clientsig));
    }
    return s0s1s2;
};

module.exports = {
    generateS0S1S2: generateS0S1S2
};