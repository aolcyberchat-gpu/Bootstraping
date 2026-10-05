(function () {
    "use strict";

    /* ============================================================
       CYTUBE PM SIGNALING + WEBTORRENT TEST  (v4)

       Goal: move REAL torrent data over peer links that were set up
       with CyTube PMs (no tracker, no DHT, no HTTP origin).

       How it works
         1. Loads WebTorrent 3.0.21 and @thaunknown/simple-peer
            10.1.2 as ES modules from a CDN (pinned versions).
         2. Peers find each other and connect exactly like v3
            (hello PMs, hash-based initiator rule, degree cap), but
            each link is a simple-peer connection (trickle:false, so
            one offer + one answer per pair, each sent in PM chunks).
         3. SEEDER: pick a small file. WebTorrent hashes it and
            makes a torrent with NO trackers. The infoHash is sent to
            every script peer in a tiny control PM.
         4. LEECHERS: add that infoHash, then hand every connected
            simple-peer to the torrent with torrent.addPeer(). The
            BitTorrent wire protocol (metadata + pieces) runs over
            those links. Everything is peer-to-peer.
         5. On completion the leecher logs the SHA-256 of what it
            received (compare with the seeder's "SOURCE SHA-256"),
            and shows the file in a <video> element.

       Verified by reading the library source: addPeer() accepts a
       simple-peer instance; WebRTC peers are keyed by peer.id (so
       this script sets a unique id on each link); both ends send a
       handshake on connect.
       NOT verified in a browser: CDN/CSP loading on your channel,
       and the end-to-end transfer. That is what this test shows.

       SAFETY: this loads third-party code into a logged-in CyTube
       page. Use a throwaway test account (like "burner"), keep the
       pinned versions, and do not run it on an account you care
       about.

       Never attach your own "data" listener to a simple-peer link:
       it would steal bytes from WebTorrent.
       ============================================================ */

    if (!window.socket || typeof socket.emit !== "function") {
        alert("CyTube socket was not found.");
        return;
    }

    if (window.__cyWT && typeof window.__cyWT.cleanup === "function") {
        try { window.__cyWT.cleanup(); } catch (e) {}
    }

    /* ---------- Config ---------- */

    var CONFIG = {
        targetDegree: 3,
        chunkSize: 220,
        sendIntervalMs: 150,
        reassemblyTimeoutMs: 20000,
        handshakeTimeoutMs: 30000,
        retryBackoffMs: 30000,
        maintainEveryMs: 2000,
        helloMax: 40,
        maxHashBytes: 200 * 1024 * 1024,
        iceServers: [
            { urls: "stun:stun.l.google.com:19302" },
            { urls: "stun:stun1.l.google.com:19302" }
        ],
        wtUrls: [
            "https://cdn.jsdelivr.net/npm/webtorrent@3.0.21/dist/webtorrent.min.js",
            "https://esm.sh/webtorrent@3.0.21"
        ],
        spUrls: [
            "https://esm.sh/@thaunknown/simple-peer@10.1.2",
            "https://cdn.jsdelivr.net/npm/@thaunknown/simple-peer@10.1.2/+esm"
        ]
    };

    /* ---------- State ---------- */

    var WT = null;            // WebTorrent class
    var SP = null;            // SimplePeer class
    var wtClient = null;
    var torrent = null;
    var torrentRole = null;   // "seed" | "leech"
    var pendingIH = null;

    var present = {};
    var scriptPeers = {};
    var helloSent = {};
    var ihSent = {};
    var peers = {};
    var backoff = {};
    var incoming = {};
    var outgoingId = 0;
    var autoRunning = false;
    var chain = Promise.resolve();
    var intervals = [];
    var listeners = [];
    var loadingLibs = null;

    /* ---------- UI ---------- */

    var OLD = document.getElementById("cy-wt-panel");
    if (OLD) OLD.remove();

    var panel = document.createElement("div");
    panel.id = "cy-wt-panel";
    panel.style.cssText =
        "position:fixed;left:10px;right:10px;top:10px;max-height:92vh;" +
        "overflow:auto;z-index:999999;background:#111;color:#eee;" +
        "padding:12px;border:2px solid #0af;font-family:monospace;" +
        "font-size:12px;";

    panel.innerHTML =
        '<div style="font-size:16px;font-weight:bold;margin-bottom:8px">' +
        'CyTube PM + WebTorrent Test v4</div>' +
        '<div style="margin-bottom:6px">Target peers: ' +
        '<input id="cy-wt-degree" type="number" min="1" max="8" value="3" ' +
        'style="width:50px;background:#222;color:#fff;border:1px solid #777"> ' +
        'Piece KiB: <input id="cy-wt-piece" type="number" min="16" ' +
        'max="4096" value="128" ' +
        'style="width:70px;background:#222;color:#fff;border:1px solid #777">' +
        '</div>' +
        '<div style="margin-bottom:6px">infoHash: ' +
        '<input id="cy-wt-ih" type="text" placeholder="40 hex chars" ' +
        'style="width:60%;background:#222;color:#fff;border:1px solid #777">' +
        '</div>' +
        '<button id="cy-wt-load">Load Libs</button> ' +
        '<button id="cy-wt-start">Start Auto</button> ' +
        '<button id="cy-wt-stop">Stop</button> ' +
        '<button id="cy-wt-seed">Seed File...</button> ' +
        '<button id="cy-wt-add">Add infoHash</button> ' +
        '<button id="cy-wt-copy">Copy Log</button> ' +
        '<button id="cy-wt-clear">Clear Log</button> ' +
        '<button id="cy-wt-remove">Remove</button>' +
        '<input id="cy-wt-file" type="file" style="display:none">' +
        '<pre id="cy-wt-status" style="white-space:pre-wrap;' +
        'background:#001a2a;padding:8px;margin-top:10px;' +
        'max-height:22vh;overflow:auto;"></pre>' +
        '<video id="cy-wt-video" controls playsinline ' +
        'style="display:none;width:100%;max-height:25vh;background:#000;' +
        'margin-top:8px"></video>' +
        '<pre id="cy-wt-log" style="white-space:pre-wrap;' +
        'word-break:break-word;background:#050505;padding:10px;' +
        'margin-top:8px;max-height:40vh;overflow:auto;"></pre>';

    document.body.appendChild(panel);

    var logBox = document.getElementById("cy-wt-log");
    var statusBox = document.getElementById("cy-wt-status");
    var degreeBox = document.getElementById("cy-wt-degree");
    var pieceBox = document.getElementById("cy-wt-piece");
    var ihBox = document.getElementById("cy-wt-ih");
    var fileBox = document.getElementById("cy-wt-file");
    var videoBox = document.getElementById("cy-wt-video");

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

    function shuffle(a) {
        for (var i = a.length - 1; i > 0; i--) {
            var j = Math.floor(Math.random() * (i + 1));
            var t = a[i]; a[i] = a[j]; a[j] = t;
        }
        return a;
    }

    function kb(n) {
        return (n / 1024).toFixed(1) + " KB";
    }

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

    async function sha256Hex(buf) {
        var d = await crypto.subtle.digest("SHA-256", buf);
        return Array.prototype.map.call(new Uint8Array(d), function (b) {
            return ("0" + b.toString(16)).slice(-2);
        }).join("");
    }

    /* ---------- Library loading ---------- */

    async function importFirst(urls, label) {
        for (var i = 0; i < urls.length; i++) {
            try {
                var m = await import(urls[i]);
                log("LOADED", label, "from", urls[i]);
                return m;
            } catch (e) {
                log("IMPORT FAILED", urls[i], String(e));
            }
        }
        throw new Error("Could not load " + label +
            " (CSP, network, or CDN problem).");
    }

    function loadLibs() {
        if (WT && SP && wtClient) return Promise.resolve();
        if (loadingLibs) return loadingLibs;

        loadingLibs = (async function () {
            var wm = await importFirst(CONFIG.wtUrls, "WebTorrent");
            WT = wm.default || wm.WebTorrent;
            var sm = await importFirst(CONFIG.spUrls, "SimplePeer");
            SP = sm.default || sm.Peer || sm;

            if (typeof WT !== "function" || typeof SP !== "function") {
                throw new Error("Unexpected module shape from CDN.");
            }

            wtClient = new WT({
                tracker: false,
                dht: false,
                lsd: false,
                natUpnp: false,
                utp: false
            });
            wtClient.on("error", function (e) {
                log("WT CLIENT ERROR:", String(e));
            });

            log("WebTorrent client ready. WebRTC supported:",
                !!SP.WEBRTC_SUPPORT);

            if (pendingIH) {
                var ih = pendingIH;
                pendingIH = null;
                addInfoHash(ih);
            }
        })();

        loadingLibs.catch(function (e) {
            log("LOAD LIBS ERROR:", String(e));
            loadingLibs = null;
        });

        return loadingLibs;
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
        if ((data.to || "").toLowerCase() !== meKey()) return; // my echo

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
            sayHello(from);
            offerInfoHash(from);
            renderStatus();
            return;
        }

        if (kind.indexOf("ih|") === 0) {
            log("INFOHASH from", from + ":", kind.substring(3));
            addInfoHash(kind.substring(3));
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
        if (pieces.length < 6) { log("Malformed CYWRTC packet."); return; }

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
        delete ihSent[key];
        closePeer(key, "user left the channel", false);
        renderStatus();
    }

    /* ---------- Peer records (simple-peer links) ---------- */

    function newPeer(key, name, initiator) {
        var p = {
            key: key,
            name: name,
            initiator: initiator,
            state: "negotiating",
            sp: null,
            wtAdded: false,
            handshakeTimer: null,
            pair: ""
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

        try {
            if (torrent && !torrent.destroyed && p.wtAdded && p.sp) {
                torrent.removePeer(p.sp.id);
            }
        } catch (e) {}
        try { if (p.sp) p.sp.destroy(); } catch (e) {}

        backoff[key] = Date.now() + CONFIG.retryBackoffMs;

        if (notify) sendCtl(p.name, "bye");

        log("PEER CLOSED:", p.name, "(" + reason + ")");
        renderStatus();
    }

    function createSP(p) {
        var sp = new SP({
            initiator: p.initiator,
            trickle: false,
            config: { iceServers: CONFIG.iceServers }
        });

        // WebTorrent keys WebRTC peers by peer.id, and simple-peer does
        // not set one. It must be a unique string per link.
        sp.id = "cy:" + p.key;
        p.sp = sp;

        sp.on("signal", function (sig) {
            if (peers[p.key] !== p) return;
            sendSignal(p.name, "sig", sig);
        });

        sp.on("connect", function () {
            if (peers[p.key] !== p) return;
            p.state = "connected";
            clearTimeout(p.handshakeTimer);
            log("LINK CONNECTED with", p.name);
            recordPair(p);
            attachToTorrent(p);
            renderStatus();
        });

        sp.on("close", function () {
            if (peers[p.key] === p) {
                closePeer(p.key, "link closed", false);
            }
        });

        sp.on("error", function (e) {
            log("LINK ERROR with", p.name + ":", String(e && e.message || e));
            if (peers[p.key] === p) {
                closePeer(p.key, "link error", true);
            }
        });

        // Intentionally NO sp.on("data"): that would steal torrent bytes.
        return sp;
    }

    async function recordPair(p) {
        try {
            var pc = p.sp && p.sp._pc;
            if (!pc || !pc.getStats) return;

            var stats = await pc.getStats();
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
            if (!pair) return;

            var l = byId[pair.localCandidateId];
            var r2 = byId[pair.remoteCandidateId];
            p.pair = (l && l.candidateType) + "/" +
                (r2 && r2.candidateType) + " " + (l && l.protocol);
            log("SELECTED PAIR", p.name + ":", p.pair);
            renderStatus();
        } catch (e) {
            log("getStats error:", String(e));
        }
    }

    /* ---------- Handshake logic ---------- */

    function startOffer(key) {
        var info = scriptPeers[key];
        var name = (info && info.name) || present[key];
        if (!name || peers[key] || !SP) return;

        var p = newPeer(key, name, true);
        log("OFFERING to", name);

        try {
            createSP(p); // initiator: emits its offer via the signal event
        } catch (e) {
            log("OFFER ERROR for", name + ":", String(e));
            closePeer(key, "offer error", true);
        }
    }

    async function handleSignalObject(from, type, obj) {
        var key = from.toLowerCase();

        if (!scriptPeers[key]) scriptPeers[key] = { name: from };
        present[key] = from;

        if (type !== "sig") {
            log("Unknown signal type:", type);
            return;
        }

        if (!SP) {
            log("Signal from", from, "but libs not loaded; ignoring.");
            return;
        }

        var p = peers[key];

        if (p && !p.initiator && obj && obj.type === "offer") {
            closePeer(key, "replaced by new offer", false);
            p = null;
        }

        if (!p) {
            if (isInitiator(key)) {
                log("Ignoring signal from", from,
                    "(by the pair rule I should be the one offering).");
                return;
            }
            if (!obj || obj.type !== "offer") {
                log("Ignoring stray signal from", from);
                return;
            }
            if (activeCount() >= CONFIG.targetDegree + 2) {
                log("Too many peers; telling", from, "I'm busy.");
                sendCtl(from, "busy");
                return;
            }

            p = newPeer(key, from, false);
            log("ANSWERING", from);
            createSP(p);
        }

        try {
            p.sp.signal(obj);
        } catch (e) {
            log("SIGNAL APPLY ERROR for", from + ":", String(e));
            closePeer(key, "signal error", true);
        }
    }

    /* ---------- Torrent plumbing ---------- */

    function attachToTorrent(p) {
        if (!torrent || torrent.destroyed || !torrent.infoHash) return;
        if (p.wtAdded || p.state !== "connected" || !p.sp) return;

        try {
            var ok = torrent.addPeer(p.sp);
            p.wtAdded = !!ok;
            log("TORRENT addPeer", p.name, ok ? "ok" : "REJECTED");
        } catch (e) {
            log("addPeer error for", p.name + ":", String(e));
        }
    }

    function attachAll() {
        Object.keys(peers).forEach(function (k) { attachToTorrent(peers[k]); });
    }

    function offerInfoHash(name) {
        var key = name.toLowerCase();
        if (torrentRole !== "seed" || !torrent || !torrent.infoHash) return;
        if (ihSent[key]) return;
        ihSent[key] = true;
        sendCtl(name, "ih|" + torrent.infoHash);
    }

    function broadcastInfoHash() {
        Object.keys(scriptPeers).forEach(function (k) {
            offerInfoHash(scriptPeers[k].name);
        });
    }

    function setTorrent(t, role) {
        torrent = t;
        torrentRole = role;
        t._cyStart = Date.now();

        t.on("error", function (e) { log("TORRENT ERROR:", String(e)); });
        t.on("warning", function (e) { log("TORRENT WARNING:", String(e)); });
        t.on("wire", function () {
            log("WIRE OPEN. peers on torrent:", t.numPeers);
        });
        t.on("metadata", function () {
            log("METADATA received. name:", t.name, "length:", t.length,
                "pieces:", t.pieces ? t.pieces.length : "?");
        });
        t.on("done", function () { onTorrentDone(t); });

        var go = function () {
            log("TORRENT infoHash:", t.infoHash);
            attachAll();
            if (role === "seed") broadcastInfoHash();
        };

        if (t.infoHash) go(); else t.once("infoHash", go);
        renderStatus();
    }

    async function seedFile(file) {
        await loadLibs();

        if (torrent) {
            log("A torrent is already active. Use Remove and reload the script.");
            return;
        }

        var pieceKiB = parseInt(pieceBox.value, 10) || 128;
        log("SEEDING:", file.name, file.size, "bytes, piece length",
            pieceKiB, "KiB");

        if (file.size <= CONFIG.maxHashBytes) {
            file.arrayBuffer().then(sha256Hex).then(function (h) {
                log("SOURCE SHA-256:", h);
            }).catch(function (e) {
                log("Source hash error:", String(e));
            });
        } else {
            log("File is large; skipping SHA-256 check.");
        }

        var t = wtClient.seed(file, {
            announce: [],
            pieceLength: pieceKiB * 1024
        }, function (tt) {
            log("SEED READY. infoHash:", tt.infoHash,
                "pieces:", tt.pieces.length);
        });

        setTorrent(t, "seed");
    }

    function addInfoHash(ih) {
        ih = (ih || "").trim().toLowerCase();

        if (!/^[0-9a-f]{40}$/.test(ih)) {
            log("Not a valid infoHash:", ih);
            return;
        }

        if (!wtClient) {
            pendingIH = ih;
            log("InfoHash queued until libs are loaded.");
            return;
        }

        if (torrent) {
            if (torrent.infoHash !== ih) {
                log("A torrent is already active; ignoring", ih);
            }
            return;
        }

        log("ADDING torrent by infoHash:", ih);
        var t = wtClient.add(ih, { announce: [] });
        setTorrent(t, "leech");
    }

    async function onTorrentDone(t) {
        var secs = ((Date.now() - t._cyStart) / 1000).toFixed(1);
        log("TORRENT DONE:", t.name, t.length, "bytes in", secs + "s",
            "(role " + torrentRole + ")");

        if (torrentRole !== "leech") return;

        try {
            var blob = await t.files[0].blob();
            log("RECEIVED size:", blob.size, "type:", blob.type || "(none)");

            if (blob.size <= CONFIG.maxHashBytes) {
                log("RECEIVED SHA-256:",
                    await sha256Hex(await blob.arrayBuffer()));
            }

            videoBox.src = URL.createObjectURL(blob);
            videoBox.style.display = "block";
        } catch (e) {
            log("Post-download error:", String(e));
        }
    }

    /* ---------- Peer manager loop ---------- */

    function maintain() {
        if (!autoRunning || !SP) return;

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

        shuffle(candidates).slice(0, need).forEach(startOffer);
    }

    function wireStatsFor(p) {
        try {
            if (!torrent || !p.sp || !torrent._peers) return "";
            var tp = torrent._peers.get(p.sp.id);
            if (!tp || !tp.wire) return "";
            return " | down " + kb(tp.wire.downloaded) +
                " up " + kb(tp.wire.uploaded);
        } catch (e) {
            return "";
        }
    }

    function renderStatus() {
        var keys = Object.keys(peers);
        var connected = keys.filter(function (k) {
            return peers[k].state === "connected";
        }).length;

        var lines = [
            "me: " + (myName() || "(unknown)") +
                " | auto: " + (autoRunning ? "ON" : "off") +
                " | libs: " + (wtClient ? "loaded" : "not loaded"),
            "channel users: " + Object.keys(present).length +
                " | script peers: " + Object.keys(scriptPeers).length,
            "links: " + connected + " connected, " +
                (keys.length - connected) + " negotiating"
        ];

        if (torrent && !torrent.destroyed) {
            lines.push("torrent: " + (torrent.name || "(fetching metadata)") +
                " | " + torrentRole +
                " | " + (torrent.progress * 100).toFixed(1) + "%" +
                " | peers " + torrent.numPeers +
                " | down " + kb(torrent.downloaded) +
                " | up " + kb(torrent.uploaded));
            lines.push("infoHash: " + (torrent.infoHash || "?"));
        }

        keys.forEach(function (k) {
            var p = peers[k];
            lines.push("  " + p.name + " | " + p.state +
                " | " + (p.initiator ? "I offered" : "I answered") +
                (p.pair ? " | " + p.pair : "") +
                (p.wtAdded ? " | in torrent" : "") +
                wireStatsFor(p));
        });

        statusBox.textContent = lines.join("\n");
    }

    /* ---------- Buttons ---------- */

    document.getElementById("cy-wt-load").onclick = function () {
        loadLibs();
    };

    document.getElementById("cy-wt-start").onclick = async function () {
        if (!myName()) {
            log("ERROR: CLIENT.name not found (are you logged in?).");
            return;
        }
        try {
            await loadLibs();
        } catch (e) {
            return;
        }
        autoRunning = true;
        backoff = {};
        readUserlistFromDOM().forEach(addPresent);
        log("AUTO STARTED. Channel users seen:", Object.keys(present).length);
        discover();
        maintain();
        renderStatus();
    };

    document.getElementById("cy-wt-stop").onclick = function () {
        autoRunning = false;
        Object.keys(peers).forEach(function (k) {
            closePeer(k, "stopped by user", true);
        });
        backoff = {};
        log("AUTO STOPPED.");
        renderStatus();
    };

    document.getElementById("cy-wt-seed").onclick = function () {
        fileBox.click();
    };

    fileBox.onchange = function () {
        var f = fileBox.files && fileBox.files[0];
        if (f) {
            seedFile(f).catch(function (e) {
                log("SEED ERROR:", String(e));
            });
        }
        fileBox.value = "";
    };

    document.getElementById("cy-wt-add").onclick = function () {
        addInfoHash(ihBox.value);
    };

    document.getElementById("cy-wt-copy").onclick = function () {
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

    document.getElementById("cy-wt-clear").onclick = function () {
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
        try { if (wtClient) wtClient.destroy(); } catch (e) {}
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

    window.__cyWT = { cleanup: cleanup };
    document.getElementById("cy-wt-remove").onclick = cleanup;

    readUserlistFromDOM().forEach(addPresent);

    log("================================");
    log("CyTube PM + WebTorrent test v4 ready.");
    log("My name:", myName() || "(not found)");
    log("Users already in channel:", Object.keys(present).length);
    log("1) Load Libs  2) Start Auto on every device");
    log("3) On ONE device: Seed File...  (use a small file first)");
    log("Leechers get the infoHash by PM automatically.");
    log("================================");
    renderStatus();

})();
