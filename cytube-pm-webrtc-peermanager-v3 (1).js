(function () {
    "use strict";

    /* ============================================================
       CYTUBE PM + WEBRTC PEER MANAGER TEST  (v3)

       Run on 3-4 devices (separate CyTube accounts, same channel,
       ideally different networks). Then click "Start Auto" on each.

       What it does:
         - Every browser runs its OWN peer manager (no leader).
         - Discovery (TEST MODE): sends a tiny "hello" PM to every
           user in the channel; script users answer, others ignore
           it. Non-script users WILL see a stray PM, so use a test
           channel. A directory bot would replace this later.
         - Initiator rule: for each pair, a hash of the two lower-
           cased usernames picks who sends the offer, so both sides
           agree without talking (no glare).
         - Keeps up to "target" connections, accepts up to
           target + 2, replies "busy" beyond that, times out stuck
           handshakes, backs off after failures, replaces dropped
           peers.
         - Local ICE candidates are batched into one signal.
         - Ignores the server's echo of your own PMs.
         - Paced PM sending (150 ms) to stay under the chat limiter.

       Not handled: TURN (STUN only), PM tab noise in the stock
       client, WebTorrent / p2p-media-loader integration.
       ============================================================ */

    if (!window.socket || typeof socket.emit !== "function") {
        alert("CyTube socket was not found.");
        return;
    }

    if (window.__cyHS && typeof window.__cyHS.cleanup === "function") {
        try { window.__cyHS.cleanup(); } catch (e) {}
    }

    /* ---------- Config ---------- */

    var CONFIG = {
        targetDegree: 3,
        chunkSize: 220,
        sendIntervalMs: 150,
        reassemblyTimeoutMs: 15000,
        handshakeTimeoutMs: 20000,
        disconnectGraceMs: 10000,
        retryBackoffMs: 30000,
        maintainEveryMs: 2000,
        helloMax: 40,
        iceFlushFallbackMs: 3000,
        iceServers: [
            { urls: "stun:stun.l.google.com:19302" },
            { urls: "stun:stun1.l.google.com:19302" }
        ]
    };

    /* ---------- State ---------- */

    var present = {};      // lowerName -> displayName (channel users, not me)
    var scriptPeers = {};  // lowerName -> {name} (answered hello / sent signal)
    var helloSent = {};    // lowerName -> true
    var peers = {};        // lowerName -> peer record
    var backoff = {};      // lowerName -> timestamp until which we skip
    var incoming = {};     // "sender|id" -> reassembly state
    var outgoingId = 0;
    var autoRunning = false;
    var chain = Promise.resolve();
    var intervals = [];
    var listeners = [];

    /* ---------- UI ---------- */

    var OLD = document.getElementById("cy-hs-panel");
    if (OLD) OLD.remove();

    var panel = document.createElement("div");
    panel.id = "cy-hs-panel";
    panel.style.cssText =
        "position:fixed;left:10px;right:10px;top:10px;max-height:92vh;" +
        "overflow:auto;z-index:999999;background:#111;color:#eee;" +
        "padding:12px;border:2px solid #0f0;font-family:monospace;" +
        "font-size:12px;";

    panel.innerHTML =
        '<div style="font-size:16px;font-weight:bold;margin-bottom:8px">' +
        'CyTube Peer Manager Test v3</div>' +
        '<div style="margin-bottom:8px">Target peers: ' +
        '<input id="cy-hs-degree" type="number" min="1" max="8" value="3" ' +
        'style="width:50px;background:#222;color:#fff;border:1px solid #777"> ' +
        '(accepts up to target+2)</div>' +
        '<button id="cy-hs-start">Start Auto</button> ' +
        '<button id="cy-hs-stop">Stop</button> ' +
        '<button id="cy-hs-hello">Hello All</button> ' +
        '<button id="cy-hs-ping">Ping All</button> ' +
        '<button id="cy-hs-big">Send 16KB All</button> ' +
        '<button id="cy-hs-copy">Copy Log</button> ' +
        '<button id="cy-hs-clear">Clear Log</button> ' +
        '<button id="cy-hs-remove">Remove</button>' +
        '<pre id="cy-hs-status" style="white-space:pre-wrap;' +
        'background:#001a00;padding:8px;margin-top:10px;' +
        'max-height:22vh;overflow:auto;"></pre>' +
        '<pre id="cy-hs-log" style="white-space:pre-wrap;' +
        'word-break:break-word;background:#050505;padding:10px;' +
        'margin-top:8px;max-height:45vh;overflow:auto;"></pre>';

    document.body.appendChild(panel);

    var logBox = document.getElementById("cy-hs-log");
    var statusBox = document.getElementById("cy-hs-status");
    var degreeBox = document.getElementById("cy-hs-degree");

    function log() {
        var line = Array.prototype.slice.call(arguments).map(function (x) {
            if (typeof x === "string") return x;
            try { return JSON.stringify(x); } catch (e) { return String(x); }
        }).join(" ");
        logBox.textContent +=
            "[" + new Date().toLocaleTimeString() + "] " + line + "\n";
        if (logBox.textContent.length > 200000) {
            logBox.textContent = logBox.textContent.slice(-150000);
        }
        logBox.scrollTop = logBox.scrollHeight;
    }

    /* ---------- Helpers ---------- */

    function myName() {
        return (window.CLIENT && window.CLIENT.name) || "";
    }

    function meKey() {
        return myName().toLowerCase();
    }

    function encode64(str) {
        return btoa(unescape(encodeURIComponent(str)));
    }

    function decode64(str) {
        return decodeURIComponent(escape(atob(str)));
    }

    function makeString(length) {
        var pattern =
            "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        var out = "";
        while (out.length < length) out += pattern;
        return out.substring(0, length);
    }

    function shuffle(a) {
        for (var i = a.length - 1; i > 0; i--) {
            var j = Math.floor(Math.random() * (i + 1));
            var t = a[i]; a[i] = a[j]; a[j] = t;
        }
        return a;
    }

    /*
     * Deterministic initiator rule. Both sides compute the same
     * answer from the two lower-cased names, so exactly one of
     * them sends the offer, and roles are spread across pairs
     * instead of always favoring the alphabetically first name.
     */
    function pairInitiator(a, b) {
        var lo = a < b ? a : b;
        var hi = a < b ? b : a;
        var s = lo + "|" + hi;
        var h = 2166136261;
        for (var i = 0; i < s.length; i++) {
            h ^= s.charCodeAt(i);
            h = Math.imul(h, 16777619) >>> 0;
        }
        return ((h >>> 15) & 1) === 0 ? lo : hi;
    }

    function isInitiator(otherKey) {
        return pairInitiator(meKey(), otherKey) === meKey();
    }

    function activeCount() {
        return Object.keys(peers).length;
    }

    /* ---------- Paced PM sender ---------- */

    var queue = [];
    var pumping = false;

    function sendPM(to, msg) {
        queue.push({ to: to, msg: msg });
        if (!pumping) {
            pumping = true;
            (function pump() {
                var m = queue.shift();
                if (!m) { pumping = false; return; }
                socket.emit("pm", { to: m.to, msg: m.msg, meta: {} });
                setTimeout(pump, CONFIG.sendIntervalMs);
            })();
        }
    }

    function sendCtl(to, kind) {
        sendPM(to, "CYCTL|" + kind);
    }

    function sendSignal(to, type, data) {
        var id = (++outgoingId) + "-" + Date.now();
        var encoded = encode64(JSON.stringify(data));
        var total = Math.ceil(encoded.length / CONFIG.chunkSize);

        log("SIGNAL SEND:", type, "to", to,
            "encoded:", encoded.length, "chunks:", total);

        for (var i = 0; i < total; i++) {
            var msg = "CYWRTC|" + id + "|" + type + "|" + i + "|" + total +
                "|" + encoded.substring(
                    i * CONFIG.chunkSize, (i + 1) * CONFIG.chunkSize);

            if (msg.length >= 320) {
                log("ERROR: signaling PM too large:", msg.length);
                return;
            }
            sendPM(to, msg);
        }
    }

    /* ---------- Incoming PM routing ---------- */

    function onPM(data) {
        if (!data || typeof data.msg !== "string") return;

        // The server also sends every PM back to the sender.
        // Only handle messages addressed to me.
        if ((data.to || "").toLowerCase() !== meKey()) return;

        var msg = data.msg;

        if (msg.indexOf("CYCTL|") === 0) {
            handleCtl(data.username, msg.substring(6));
        } else if (msg.indexOf("CYWRTC|") === 0) {
            handleSignal(data);
        }
    }

    function handleCtl(from, kind) {
        var key = from.toLowerCase();
        if (key === meKey()) return;

        if (kind === "hello") {
            if (!scriptPeers[key]) {
                scriptPeers[key] = { name: from };
                log("SCRIPT PEER FOUND:", from);
            }
            present[key] = from;
            sayHello(from); // reply once so they learn about me too
            renderStatus();
            return;
        }

        if (kind === "busy") {
            log("PEER BUSY:", from);
            backoff[key] = Date.now() + CONFIG.retryBackoffMs * 2;
            closePeer(key, "remote busy", false);
            return;
        }

        if (kind === "bye") {
            closePeer(key, "remote said bye", false);
            return;
        }

        log("Unknown control message:", kind, "from", from);
    }

    function handleSignal(data) {
        var pieces = data.msg.split("|");
        if (pieces.length < 6) {
            log("Malformed CYWRTC packet.");
            return;
        }

        var id = pieces[1];
        var type = pieces[2];
        var part = parseInt(pieces[3], 10);
        var total = parseInt(pieces[4], 10);
        var payload = pieces.slice(5).join("|");
        var key = data.username + "|" + id;

        if (!(part >= 0 && total > 0 && part < total)) {
            log("Bad part/total in CYWRTC packet.");
            return;
        }

        var st = incoming[key];
        if (!st) {
            st = incoming[key] = { type: type, total: total, parts: [] };
            st.timer = setTimeout(function () {
                if (incoming[key]) {
                    var have = incoming[key].parts.filter(Boolean).length;
                    log("REASSEMBLY TIMEOUT:", type, "from", data.username,
                        "got", have + "/" + total);
                    delete incoming[key];
                }
            }, CONFIG.reassemblyTimeoutMs);
        }

        st.parts[part] = payload;

        for (var i = 0; i < st.total; i++) {
            if (typeof st.parts[i] !== "string") return;
        }

        clearTimeout(st.timer);
        delete incoming[key];

        var decoded;
        try {
            decoded = JSON.parse(decode64(st.parts.join("")));
        } catch (e) {
            log("ERROR decoding signal from", data.username, String(e));
            return;
        }

        log("SIGNAL COMPLETE:", type, "from", data.username);

        // One signal at a time, in arrival order.
        chain = chain.then(function () {
            return handleSignalObject(data.username, type, decoded);
        }).catch(function (e) {
            log("SIGNAL HANDLER ERROR:", String(e));
        });
    }

    /* ---------- Discovery (test mode) ---------- */

    function sayHello(name) {
        var key = name.toLowerCase();
        if (key === meKey() || helloSent[key]) return;
        helloSent[key] = true;
        sendCtl(name, "hello");
    }

    function readUserlistFromDOM() {
        var out = [];
        var items = document.querySelectorAll("#userlist .userlist_item");
        for (var i = 0; i < items.length; i++) {
            var span = items[i].children[1];
            if (span && span.textContent.trim()) {
                out.push(span.textContent.trim());
            }
        }
        return out;
    }

    function addPresent(name) {
        if (!name) return;
        var key = name.toLowerCase();
        if (key === meKey()) return;
        present[key] = name;
    }

    function discover() {
        var names = Object.keys(present).map(function (k) {
            return present[k];
        });
        if (names.length > CONFIG.helloMax) {
            log("Too many users (" + names.length + "); hello capped at",
                CONFIG.helloMax);
            names = shuffle(names).slice(0, CONFIG.helloMax);
        }
        names.forEach(sayHello);
    }

    function onUserlist(users) {
        present = {};
        (users || []).forEach(function (u) { addPresent(u && u.name); });
        if (autoRunning) discover();
        renderStatus();
    }

    function onAddUser(u) {
        if (!u || !u.name) return;
        addPresent(u.name);
        if (autoRunning) sayHello(u.name);
        renderStatus();
    }

    function onUserLeave(u) {
        if (!u || !u.name) return;
        var key = u.name.toLowerCase();
        delete present[key];
        delete scriptPeers[key];
        delete helloSent[key];
        closePeer(key, "user left the channel", false);
        renderStatus();
    }

    /* ---------- Peer records ---------- */

    function newPeer(key, name, initiator) {
        var p = {
            key: key,
            name: name,
            initiator: initiator,
            state: "negotiating",
            pc: null,
            dc: null,
            pendingIce: [],
            iceBuf: [],
            iceTimer: null,
            gatherDone: false,
            candCount: 0,
            candTypes: {},
            emptySent: false,
            sdpSent: false,
            handshakeTimer: null,
            discTimer: null,
            pair: "",
            rtt: null
        };

        p.handshakeTimer = setTimeout(function () {
            if (peers[key] === p && p.state !== "connected") {
                log("HANDSHAKE TIMEOUT:", name);
                closePeer(key, "handshake timeout", true);
            }
        }, CONFIG.handshakeTimeoutMs);

        peers[key] = p;
        renderStatus();
        return p;
    }

    function closePeer(key, reason, notify) {
        var p = peers[key];
        if (!p) return;

        delete peers[key];

        clearTimeout(p.handshakeTimer);
        clearTimeout(p.iceTimer);
        clearTimeout(p.discTimer);

        try { if (p.dc) p.dc.close(); } catch (e) {}
        try { if (p.pc) p.pc.close(); } catch (e) {}

        backoff[key] = Date.now() + CONFIG.retryBackoffMs;

        if (notify) sendCtl(p.name, "bye");

        log("PEER CLOSED:", p.name, "(" + reason + ")");
        renderStatus();
    }

    function flushIce(p, force) {
        if (peers[p.key] !== p) return;
        if (!p.sdpSent) return;
        if (!p.iceBuf.length) {
            if (p.gatherDone && p.candCount === 0 && !p.emptySent) {
                p.emptySent = true;
                sendSignal(p.name, "ice", { list: [] });
            }
            return;
        }
        if (!p.gatherDone && !force) return;

        clearTimeout(p.iceTimer);
        p.iceTimer = null;

        var list = p.iceBuf;
        p.iceBuf = [];
        sendSignal(p.name, "ice", { list: list });
    }

    function createPC(p) {
        var pc = new RTCPeerConnection({ iceServers: CONFIG.iceServers });
        p.pc = pc;

        pc.onicecandidate = function (ev) {
            if (peers[p.key] !== p || p.pc !== pc) return;

            if (ev.candidate) {
                var c = ev.candidate;
                var tm = / typ (\w+)/.exec(c.candidate);
                p.candCount++;
                if (tm) p.candTypes[tm[1]] = (p.candTypes[tm[1]] || 0) + 1;
                p.iceBuf.push({
                    candidate: c.candidate,
                    sdpMid: c.sdpMid,
                    sdpMLineIndex: c.sdpMLineIndex,
                    usernameFragment: c.usernameFragment || null
                });
                if (!p.iceTimer) {
                    p.iceTimer = setTimeout(function () {
                        p.iceTimer = null;
                        flushIce(p, true);
                    }, CONFIG.iceFlushFallbackMs);
                }
            } else {
                p.gatherDone = true;
                log("ICE GATHERED for", p.name + ":", p.candCount,
                    "candidate(s)", JSON.stringify(p.candTypes));
                if (p.candCount === 0) {
                    log("WARNING: no local ICE candidates for", p.name,
                        "- WebRTC may be blocked (VPN / browser setting).");
                }
                flushIce(p, false);
            }
        };

        pc.onconnectionstatechange = function () {
            if (peers[p.key] !== p || p.pc !== pc) return;

            log("STATE", p.name + ":", pc.connectionState);

            if (pc.connectionState === "connected") {
                p.state = "connected";
                clearTimeout(p.handshakeTimer);
                clearTimeout(p.discTimer);
                recordPair(p);
            } else if (pc.connectionState === "disconnected") {
                clearTimeout(p.discTimer);
                p.discTimer = setTimeout(function () {
                    if (peers[p.key] === p &&
                        pc.connectionState === "disconnected") {
                        closePeer(p.key, "stayed disconnected", true);
                    }
                }, CONFIG.disconnectGraceMs);
            } else if (pc.connectionState === "failed" ||
                       pc.connectionState === "closed") {
                closePeer(p.key, "connection " + pc.connectionState, true);
            }
            renderStatus();
        };

        pc.ondatachannel = function (ev) {
            attachChannel(p, ev.channel, "REMOTE");
        };

        return pc;
    }

    function attachChannel(p, ch, label) {
        p.dc = ch;

        ch.onopen = function () {
            log(label, "DATA CHANNEL OPEN with", p.name);
            try { ch.send("hello from " + myName()); } catch (e) {}
            renderStatus();
        };

        ch.onmessage = function (ev) {
            var d = ev.data;
            if (typeof d === "string" && d.indexOf("ping:") === 0) {
                try { ch.send("pong:" + d.substring(5)); } catch (e) {}
            } else if (typeof d === "string" && d.indexOf("pong:") === 0) {
                p.rtt = Date.now() - parseInt(d.substring(5), 10);
                log("PONG from", p.name, "RTT ms:", p.rtt);
                renderStatus();
            } else if (typeof d === "string" && d.length > 200) {
                log("BIG MESSAGE from", p.name, "length:", d.length);
            } else {
                log("DATACHANNEL from", p.name + ":", d);
            }
        };

        ch.onclose = function () {
            log(label, "DATA CHANNEL CLOSED with", p.name);
        };
    }

    async function recordPair(p) {
        try {
            var stats = await p.pc.getStats();
            var byId = {};
            var pair = null;

            stats.forEach(function (r) { byId[r.id] = r; });
            stats.forEach(function (r) {
                if (r.type === "transport" && r.selectedCandidatePairId) {
                    pair = byId[r.selectedCandidatePairId];
                }
            });
            if (!pair) {
                stats.forEach(function (r) {
                    if (r.type === "candidate-pair" &&
                        (r.selected ||
                         (r.nominated && r.state === "succeeded"))) {
                        pair = r;
                    }
                });
            }
            if (!pair) { log("SELECTED PAIR not found for", p.name); return; }

            var l = byId[pair.localCandidateId];
            var r2 = byId[pair.remoteCandidateId];
            p.pair = (l && l.candidateType) + "/" +
                (r2 && r2.candidateType) + " " + (l && l.protocol);

            log("SELECTED PAIR", p.name + ":", p.pair,
                "rtt_ms=" + (pair.currentRoundTripTime != null
                    ? Math.round(pair.currentRoundTripTime * 1000) : "?"));
            renderStatus();
        } catch (e) {
            log("getStats error:", String(e));
        }
    }

    /* ---------- Handshake logic ---------- */

    async function startOffer(key) {
        var info = scriptPeers[key];
        var name = (info && info.name) || present[key];
        if (!name || peers[key]) return;

        var p = newPeer(key, name, true);
        log("OFFERING to", name);

        try {
            createPC(p);
            attachChannel(p, p.pc.createDataChannel("swarm", {
                ordered: true
            }), "LOCAL");

            var offer = await p.pc.createOffer();
            await p.pc.setLocalDescription(offer);

            sendSignal(name, "offer", {
                type: p.pc.localDescription.type,
                sdp: p.pc.localDescription.sdp
            });
            p.sdpSent = true;
            flushIce(p, false);
        } catch (e) {
            log("OFFER ERROR for", name + ":", String(e));
            closePeer(key, "offer error", true);
        }
    }

    async function flushRemoteIce(p) {
        var list = p.pendingIce;
        p.pendingIce = [];
        for (var i = 0; i < list.length; i++) {
            try {
                await p.pc.addIceCandidate(new RTCIceCandidate(list[i]));
            } catch (e) {
                log("REMOTE ICE ERROR from", p.name + ":", String(e));
            }
        }
    }

    async function handleSignalObject(from, type, obj) {
        var key = from.toLowerCase();

        if (!scriptPeers[key]) scriptPeers[key] = { name: from };
        present[key] = from;

        if (type === "offer") {
            if (isInitiator(key)) {
                log("Ignoring offer from", from,
                    "(by the pair rule I should be the one offering).");
                return;
            }

            if (peers[key]) closePeer(key, "replaced by new offer", false);

            if (activeCount() >= CONFIG.targetDegree + 2) {
                log("Too many peers; telling", from, "I'm busy.");
                sendCtl(from, "busy");
                return;
            }

            var p = newPeer(key, from, false);
            log("ANSWERING", from);

            createPC(p);
            await p.pc.setRemoteDescription(new RTCSessionDescription(obj));
            await flushRemoteIce(p);

            var answer = await p.pc.createAnswer();
            await p.pc.setLocalDescription(answer);

            sendSignal(from, "answer", {
                type: p.pc.localDescription.type,
                sdp: p.pc.localDescription.sdp
            });
            p.sdpSent = true;
            flushIce(p, false);
            return;
        }

        var peer = peers[key];

        if (type === "answer") {
            if (!peer || !peer.initiator || !peer.pc) {
                log("Unexpected answer from", from);
                return;
            }
            await peer.pc.setRemoteDescription(new RTCSessionDescription(obj));
            await flushRemoteIce(peer);
            return;
        }

        if (type === "ice") {
            if (!peer) {
                log("ICE from", from, "with no peer record; dropped.");
                return;
            }
            var list = obj && obj.list ? obj.list : [obj];
            if (list.length === 0) {
                log("PEER REPORTED NO ICE CANDIDATES:", from,
                    "- it may have WebRTC blocked (VPN / browser setting).");
                return;
            }
            if (!peer.pc || !peer.pc.remoteDescription) {
                peer.pendingIce = peer.pendingIce.concat(list);
                return;
            }
            for (var i = 0; i < list.length; i++) {
                try {
                    await peer.pc.addIceCandidate(new RTCIceCandidate(list[i]));
                } catch (e) {
                    log("REMOTE ICE ERROR from", from + ":", String(e));
                }
            }
            return;
        }

        log("Unknown signal type:", type);
    }

    /* ---------- Peer manager loop ---------- */

    function maintain() {
        if (!autoRunning) return;

        CONFIG.targetDegree = Math.max(
            1, Math.min(8, parseInt(degreeBox.value, 10) || 3));

        var need = CONFIG.targetDegree - activeCount();
        if (need <= 0) return;

        var now = Date.now();
        var candidates = Object.keys(scriptPeers).filter(function (k) {
            return present[k] &&
                !peers[k] &&
                (!backoff[k] || backoff[k] < now) &&
                isInitiator(k);
        });

        shuffle(candidates).slice(0, need).forEach(function (k) {
            startOffer(k);
        });
    }

    function renderStatus() {
        var keys = Object.keys(peers);
        var connected = keys.filter(function (k) {
            return peers[k].state === "connected";
        }).length;

        var lines = [
            "me: " + (myName() || "(unknown)") +
                " | auto: " + (autoRunning ? "ON" : "off"),
            "channel users: " + Object.keys(present).length +
                " | script peers known: " + Object.keys(scriptPeers).length,
            "connections: " + connected + " connected, " +
                (keys.length - connected) + " negotiating" +
                " (target " + CONFIG.targetDegree + ")"
        ];

        keys.forEach(function (k) {
            var p = peers[k];
            lines.push("  " + p.name + " | " + p.state +
                " | " + (p.initiator ? "I offered" : "I answered") +
                (p.pair ? " | " + p.pair : "") +
                (p.rtt != null ? " | rtt " + p.rtt + "ms" : ""));
        });

        statusBox.textContent = lines.join("\n");
    }

    /* ---------- Buttons ---------- */

    function forEachOpenChannel(fn) {
        Object.keys(peers).forEach(function (k) {
            var p = peers[k];
            if (p.dc && p.dc.readyState === "open") fn(p);
        });
    }

    document.getElementById("cy-hs-start").onclick = function () {
        if (!myName()) {
            log("ERROR: CLIENT.name not found (are you logged in?).");
            return;
        }
        if (!window.RTCPeerConnection) {
            log("ERROR: RTCPeerConnection unavailable.");
            return;
        }
        autoRunning = true;
        readUserlistFromDOM().forEach(addPresent);
        log("AUTO STARTED. Channel users seen:",
            Object.keys(present).length);
        discover();
        maintain();
        renderStatus();
    };

    document.getElementById("cy-hs-stop").onclick = function () {
        autoRunning = false;
        Object.keys(peers).forEach(function (k) {
            closePeer(k, "stopped by user", true);
        });
        log("AUTO STOPPED.");
        renderStatus();
    };

    document.getElementById("cy-hs-hello").onclick = function () {
        readUserlistFromDOM().forEach(addPresent);
        helloSent = {};
        discover();
        log("Hello sent to channel users.");
    };

    document.getElementById("cy-hs-ping").onclick = function () {
        var n = 0;
        forEachOpenChannel(function (p) {
            p.dc.send("ping:" + Date.now());
            n++;
        });
        log("Ping sent to", n, "peer(s).");
    };

    document.getElementById("cy-hs-big").onclick = function () {
        var n = 0;
        forEachOpenChannel(function (p) {
            p.dc.send(makeString(16384));
            n++;
        });
        log("16384-char payload sent to", n, "peer(s).");
    };

    document.getElementById("cy-hs-copy").onclick = function () {
        var text = logBox.textContent;

        function fallback() {
            var ta = document.createElement("textarea");
            ta.value = text;
            ta.style.cssText = "position:fixed;left:0;top:0;opacity:0;";
            document.body.appendChild(ta);
            ta.focus();
            ta.select();
            var ok = false;
            try { ok = document.execCommand("copy"); } catch (e) {}
            ta.remove();
            log(ok ? "Log copied to clipboard."
                   : "Copy failed. Long-press the log text to select it.");
        }

        if (navigator.clipboard && navigator.clipboard.writeText) {
            navigator.clipboard.writeText(text).then(function () {
                log("Log copied to clipboard.");
            }, fallback);
        } else {
            fallback();
        }
    };

    document.getElementById("cy-hs-clear").onclick = function () {
        logBox.textContent = "";
    };

    /* ---------- Listeners and cleanup ---------- */

    function listen(event, fn) {
        socket.on(event, fn);
        listeners.push([event, fn]);
    }

    listen("pm", onPM);
    listen("userlist", onUserlist);
    listen("addUser", onAddUser);
    listen("userLeave", onUserLeave);
    listen("cooldown", function (ms) {
        log("SERVER COOLDOWN (a PM may have been dropped):", ms);
    });
    listen("errorMsg", function (d) {
        log("SERVER ERRORMSG:", d && d.msg);
    });

    function onVisibility() {
        log("PAGE VISIBILITY:", document.visibilityState);
    }
    document.addEventListener("visibilitychange", onVisibility);

    intervals.push(setInterval(maintain, CONFIG.maintainEveryMs));
    intervals.push(setInterval(renderStatus, 1000));

    function cleanup() {
        autoRunning = false;
        intervals.forEach(clearInterval);
        Object.keys(peers).forEach(function (k) {
            closePeer(k, "script cleanup", true);
        });
        listeners.forEach(function (l) {
            try {
                if (socket.off) socket.off(l[0], l[1]);
                else socket.removeListener(l[0], l[1]);
            } catch (e) {}
        });
        listeners = [];
        document.removeEventListener("visibilitychange", onVisibility);
        panel.remove();
    }

    window.__cyHS = { cleanup: cleanup };

    document.getElementById("cy-hs-remove").onclick = cleanup;

    readUserlistFromDOM().forEach(addPresent);

    log("================================");
    log("CyTube peer manager v3 ready.");
    log("My name:", myName() || "(not found)");
    log("Users already in channel:", Object.keys(present).length);
    log("Click 'Start Auto' on every device.");
    log("================================");
    renderStatus();

})();
