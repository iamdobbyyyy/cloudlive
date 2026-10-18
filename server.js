const express = require("express");
const http = require("http");
const { Server } = require("socket.io");
const { WebcastPushConnection } = require("tiktok-live-connector");
const googleTTS = require("google-tts-api");

const PORT = process.env.PORT || 3000;

const app = express();
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: "*" } });

app.use(express.static("public"));

// ─── Multi-User Connection Pool ──────────────────────────────────────
const connections = new Map(); // username -> { tiktok, clients: Set, roomInfo, timer }

function getOrCreateConnection(username) {
  if (connections.has(username)) {
    const conn = connections.get(username);
    if (conn.timer) {
      clearTimeout(conn.timer);
      conn.timer = null;
    }
    return conn;
  }
  return null;
}

function connectToTikTok(username, socket) {
  const room = `tiktok:${username}`;
  socket.join(room);

  const existing = getOrCreateConnection(username);
  if (existing && existing.connected) {
    existing.clients.add(socket.id);
    socket.emit("connected", { username, roomInfo: existing.roomInfo });
    return;
  }

  console.log(`[CloudLive] Connecting to @${username}...`);

  const tiktok = new WebcastPushConnection(username, {
    processInitialData: true,
    enableExtendedGiftInfo: true,
    enableWebsocketUpgrade: true,
    requestPollingIntervalMs: 2000,
  });

  const connData = {
    tiktok,
    connected: false,
    roomInfo: null,
    clients: new Set([socket.id]),
    timer: null,
  };
  connections.set(username, connData);

  tiktok
    .connect()
    .then((state) => {
      connData.connected = true;
      connData.roomInfo = {
        roomId: state.roomId,
        title: state.roomInfo?.title || "",
        viewers: state.roomInfo?.user_count || 0,
      };
      console.log(`[CloudLive] Connected @${username} (Room ID: ${state.roomId})`);
      io.to(room).emit("connected", { username, roomInfo: connData.roomInfo });
    })
    .catch((err) => {
      console.error(`[CloudLive] Error @${username}:`, err.message);
      io.to(room).emit("connection_error", { message: err.message });
      connections.delete(username);
    });

  tiktok.on("chat", (data) => {
    io.to(room).emit("chat", {
      type: "chat",
      userId: data.userId,
      uniqueId: data.uniqueId,
      nickname: data.nickname,
      profilePictureUrl: data.profilePictureUrl,
      comment: data.comment,
      followRole: data.followRole,
      isModerator: data.isModerator,
      isSubscriber: data.isSubscriber,
      timestamp: Date.now(),
    });
  });

  tiktok.on("gift", (data) => {
    if (data.giftType === 1 && !data.repeatEnd) return;
    io.to(room).emit("gift", {
      type: "gift",
      userId: data.userId,
      uniqueId: data.uniqueId,
      nickname: data.nickname,
      profilePictureUrl: data.profilePictureUrl,
      giftName: data.giftName || "Gift",
      giftId: data.giftId,
      giftPictureUrl: data.giftPictureUrl,
      diamondCount: data.diamondCount || 0,
      repeatCount: data.repeatCount || 1,
      totalDiamonds: (data.diamondCount || 0) * (data.repeatCount || 1),
      timestamp: Date.now(),
    });
  });

  tiktok.on("like", (data) => {
    io.to(room).emit("like", {
      type: "like",
      nickname: data.nickname,
      profilePictureUrl: data.profilePictureUrl,
      likeCount: data.likeCount,
      totalLikeCount: data.totalLikeCount,
      timestamp: Date.now(),
    });
  });

  tiktok.on("follow", (data) => {
    io.to(room).emit("follow", {
      type: "follow",
      nickname: data.nickname,
      profilePictureUrl: data.profilePictureUrl,
      timestamp: Date.now(),
    });
  });

  tiktok.on("share", (data) => {
    io.to(room).emit("share", {
      type: "share",
      nickname: data.nickname,
      profilePictureUrl: data.profilePictureUrl,
      timestamp: Date.now(),
    });
  });

  tiktok.on("roomUser", (data) => {
    io.to(room).emit("viewer_count", { viewerCount: data.viewerCount });
  });

  tiktok.on("disconnected", () => {
    connData.connected = false;
    io.to(room).emit("disconnected");
    scheduleCleanup(username);
  });

  tiktok.on("error", (err) => {
    console.error(`[CloudLive] Stream Error @${username}:`, err.message);
  });
}

function scheduleCleanup(username) {
  const conn = connections.get(username);
  if (!conn) return;
  if (conn.clients.size === 0) {
    conn.timer = setTimeout(() => {
      try {
        conn.tiktok.disconnect();
      } catch (_) {}
      connections.delete(username);
      console.log(`[CloudLive] Connection @${username} removed`);
    }, 60000);
  }
}

// ─── High Quality Thai TTS Endpoint (Audio proxy for OBS) ────────────
app.get("/api/tts", async (req, res) => {
  const text = (req.query.text || "").trim();
  if (!text) return res.status(400).send("Text is required");

  try {
    const url = googleTTS.getAudioUrl(text, {
      lang: "th",
      slow: false,
      host: "https://translate.google.com",
      timeout: 10000,
    });
    res.redirect(url);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ─── Status API ─────────────────────────────────────────────────────
app.get("/api/status", (req, res) => {
  const list = [];
  connections.forEach((conn, username) => {
    list.push({
      username,
      connected: conn.connected,
      clients: conn.clients.size,
      viewers: conn.roomInfo?.viewers || 0,
    });
  });
  res.json({ activeStreams: list.length, streams: list });
});

// ─── Socket Gateway ─────────────────────────────────────────────────
io.on("connection", (socket) => {
  socket.on("connect_tiktok", (data) => {
    const username = data.username?.trim()?.replace("@", "");
    if (username) {
      connectToTikTok(username, socket);
    }
  });

  // Simulator Events for Testing in Studio
  socket.on("simulate_event", (data) => {
    const username = data.username?.trim()?.replace("@", "");
    if (!username) return;
    const room = `tiktok:${username}`;

    if (data.type === "chat") {
      io.to(room).emit("chat", {
        type: "chat",
        nickname: data.nickname || "User_Test",
        comment: data.comment || "สวัสดีครับ ทดสอบแชท CloudLive",
        profilePictureUrl: "https://images.unsplash.com/photo-1534528741775-53994a69daeb?w=100&h=100&fit=crop&crop=faces",
        isSubscriber: false,
        isModerator: false,
        followRole: 1,
        timestamp: Date.now(),
      });
    } else if (data.type === "gift") {
      io.to(room).emit("gift", {
        type: "gift",
        nickname: data.nickname || "Top_Supporter",
        giftName: data.giftName || "Rose",
        giftPictureUrl: "https://p16-webcast.tiktokcdn.com/img/maliva/webcast-va/5657_rose.png~tplv-obj.png",
        repeatCount: data.repeatCount || 1,
        totalDiamonds: data.diamonds || 1,
        profilePictureUrl: "https://images.unsplash.com/photo-1507003211169-0a1dd7228f2d?w=100&h=100&fit=crop&crop=faces",
        timestamp: Date.now(),
      });
    } else if (data.type === "follow") {
      io.to(room).emit("follow", {
        type: "follow",
        nickname: data.nickname || "New_Follower",
        profilePictureUrl: "https://images.unsplash.com/photo-1494790108377-be9c29b29330?w=100&h=100&fit=crop&crop=faces",
        timestamp: Date.now(),
      });
    }
  });

  socket.on("disconnect", () => {
    connections.forEach((conn, username) => {
      conn.clients.delete(socket.id);
      if (conn.clients.size === 0 && !conn.connected) {
        scheduleCleanup(username);
      }
    });
  });
});

server.listen(PORT, () => {
  console.log(`
  ☁️  CloudLive Studio Server Started
  ------------------------------------
  Website:  http://localhost:${PORT}
  Overlay:  http://localhost:${PORT}/overlay.html?user=USERNAME
  TTS API:  http://localhost:${PORT}/api/tts?text=...
  `);
});
