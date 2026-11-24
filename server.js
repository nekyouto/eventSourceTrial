const express = require("express");
const cors = require("cors");
const path = require("path");

const app = express();
app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname, "public")));

let msgs = [];
let activeUsers = [];

app.get("/events", (req, res) => {
  res.setHeader("Content-Type", "text/event-stream");
  res.setHeader("Cache-Control", "no-cache");
  res.setHeader("Connection", "keep-alive");

  const userName = req.query.userName;

  const isUserActive = activeUsers.filter((msg) => msg.userName == userName);
  if (isUserActive.length == 0) {
    activeUsers.push({ userName: userName, lastMsgTime: 0 });
  }

  res.write("retry: 3000\n\n");
  res.write(
    `data: ${JSON.stringify({ message: "Connection established", type: "heartbeat" })}\n\n`,
  );

  const intervalId = setInterval(() => {
    let lastMsgTime = 0;
    for (let i = 0; i < activeUsers.length; i++) {
      if (activeUsers[i].userName == userName) {
        lastMsgTime = activeUsers[i].lastMsgTime;
        break;
      }
    }
    const activeMsgsToSend = msgs.filter(
      (msg) => msg.userName == userName && msg.createdTime > lastMsgTime,
    );
    activeMsgsToSend.sort((a, b) => a.createdTime - b.createdTime);
    if (activeMsgsToSend.length > 0) {
      let tempMsg = {};
      tempMsg.type = "message";
      tempMsg.msgs = activeMsgsToSend;
      for (let i = 0; i < activeUsers.length; i++) {
        if (activeUsers[i].userName == userName) {
          activeUsers[i].lastMsgTime =
            activeMsgsToSend[activeMsgsToSend.length - 1].createdTime;
          break;
        }
      }
      res.write(`data: ${JSON.stringify(tempMsg)}\n\n`);
    } else {
      const payload = { time: new Date().toISOString(), type: "heartbeat" };
      res.write(`data: ${JSON.stringify(payload)}\n\n`);
    }
  }, 5000);

  req.on("close", () => {
    console.log("Client disconnected. Cleaning up...");
    const userName = req.query.userName;
    const tempActiveUsers = activeUsers.filter(
      (item) => item.userName !== userName,
    );
    activeUsers = tempActiveUsers;
    clearInterval(intervalId);
    res.end();
  });
});

app.post("/msgs", (req, res) => {
  let msgBody = req.body;
  msgBody.createdTime = new Date().getTime();
  msgs.push(msgBody);
  return res.status(200).json({ ok: true });
});

app.listen(3000, () => {
  msgs = [];
  activeUsers = [];
  console.log("Server listening on http://localhost:3000");
});
