// ============================================================
//  server.js — WebSocket сервер для Yandere vs Tsunderes
// ============================================================
const http = require('http');
const fs   = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');

const PORT = 3000;

// ---------- HTTP сервер ----------
const MIME = {
  '.html': 'text/html; charset=utf-8',
  '.css':  'text/css',
  '.js':   'application/javascript',
  '.glb':  'model/gltf-binary',
  '.json': 'application/json',
  '.png':  'image/png',
  '.jpg':  'image/jpeg',
  '.mp3':  'audio/mpeg',
  '.wav':  'audio/wav',
  '.ogg':  'audio/ogg',
  '.hdr':  'image/vnd.radiance',
};


const httpServer = http.createServer((req, res) => {
  let rawUrl = req.url.split('?')[0].split('#')[0];
  try { rawUrl = decodeURIComponent(rawUrl); } catch(e) {}
  const urlPath = rawUrl;
  let filePath = path.join(__dirname, urlPath === '/' ? 'index.html' : urlPath);
  const ext = path.extname(filePath).toLowerCase();

  fs.stat(filePath, (statErr, stat) => {
    if (statErr) {
      res.writeHead(404, { 'Content-Type': 'text/plain' });
      res.end('Not found: ' + urlPath);
      return;
    }

    const contentType = MIME[ext] || 'application/octet-stream';
    res.writeHead(200, {
      'Content-Type':   contentType,
      'Content-Length': stat.size,
      'Cache-Control':  'no-cache, no-store, must-revalidate',
      'Pragma':         'no-cache',
      'Expires':        '0',
      'Access-Control-Allow-Origin': '*',
    });

    const stream = fs.createReadStream(filePath);
    stream.on('error', err => {
      if (!res.headersSent) res.writeHead(500);
      res.end();
    });
    stream.pipe(res);
  });
});

let rooms = {};
let playerRoomMap = {};

function genId() {
  return Math.random().toString(36).slice(2, 9);
}

function broadcast(room, msg, excludeId = null) {
  Object.values(room.players).forEach(p => {
    if (p.id !== excludeId && p.ws.readyState === 1) {
      p.ws.send(JSON.stringify(msg));
    }
  });
}

function broadcastAll(room, msg) {
  broadcast(room, msg);
}

function sendRoomList(ws) {
  const list = Object.values(rooms).map(r => ({
    id: r.id,
    name: r.name,
    hostId: r.hostId,
    playerCount: Object.keys(r.players).length,
    maxPlayers: r.maxPlayers,
    state: r.state
  }));
  ws.send(JSON.stringify({ type: 'room_list', rooms: list }));
}

function sendLobbyState(room) {
  const players = Object.values(room.players).map(p => ({
    id: p.id,
    name: p.name,
    role: p.role,
    ready: p.ready,
    isHost: p.id === room.hostId
  }));
  broadcastAll(room, {
    type: 'lobby_state',
    players,
    state: room.state,
    hostId: room.hostId,
    map: room.map || 'map_erd',
    matchDuration: room.matchDuration || 180,
    chaseMusic: room.chaseMusic || '624_-MATADORA.mp3',
    itemRotateInterval: room.itemRotateInterval !== undefined ? room.itemRotateInterval : 120
  });
}

function checkAndStartCountdown(room) {
  const pArr = Object.values(room.players);
  if (pArr.length < 1) return;
  if (pArr.every(p => p.ready)) {
    assignRoles(room);
    let count = 3;
    broadcastAll(room, { type: 'countdown', value: count });
    broadcastAll(room, { type: 'play_sound', sound: 'dota_match_ready.mp3' });
    room.state = 'countdown';
    room.countdown = setInterval(() => {
      count--;
      if (count <= 0) {
        clearInterval(room.countdown);
        startMatch(room);
      } else {
        broadcastAll(room, { type: 'countdown', value: count });
      }
    }, 1000);
  }
}

function assignRoles(room) {
  const pArr = Object.values(room.players);
  const explicitYandere = pArr.find(p => p.role === 'yandere');
  if (!explicitYandere) {
    const idx = Math.floor(Math.random() * pArr.length);
    pArr.forEach((p, i) => {
      p.role = (i === idx) ? 'yandere' : 'tsundere';
    });
  } else {
    pArr.forEach(p => {
      if (p !== explicitYandere) p.role = 'tsundere';
    });
  }

  pArr.forEach(p => {
    p.hp = 3;
    p.maxHp = 3;
    p.isDead = false;
    p.usedItems = [];
    p.currentItem = null;
  });
}

const ITEM_POOL = ['choker', 'bubble_tea', 'ninjutsu', 'cosplay', 'ghoul_eye', 'reincarnation', 'feminization', 'yarik'];

function getRandomItemForPlayer(player) {
  if (!player.usedItems) player.usedItems = [];
  let available = ITEM_POOL.filter(it => !player.usedItems.includes(it));
  if (available.length === 0) {
    player.usedItems = [];
    available = ITEM_POOL;
  }
  const item = available[Math.floor(Math.random() * available.length)];
  player.usedItems.push(item);
  player.currentItem = item;
  return item;
}

function startMatch(room) {
  room.state = 'game';
  const gamePlayers = getGamePlayers(room);

  const initialItems = {};
  Object.values(room.players).forEach(p => {
    p.usedItems = [];
    if (p.role === 'tsundere') {
      initialItems[p.id] = getRandomItemForPlayer(p);
    }
  });

  room.matchDuration = room.matchDuration || 180;
  room.matchTimeRemaining = room.matchDuration;

  broadcastAll(room, {
    type: 'game_start',
    players: gamePlayers,
    hostId: room.hostId,
    initialItems,
    map: room.map || 'map_erd',
    matchDuration: room.matchDuration,
    chaseMusic: room.chaseMusic || '624_-MATADORA.mp3'
  });

  // Синхронизация визуальных моделей при старте
  Object.keys(initialItems).forEach(pid => {
    broadcastAll(room, { type: 'model_change', id: pid, itemId: initialItems[pid] });
  });

  // ---- Основной таймер матча (Эвакуация / Победа выживших при 00:00) ----
  if (room.matchTimerInterval) clearInterval(room.matchTimerInterval);
  room.matchTimerInterval = setInterval(() => {
    if (room.state !== 'game') return;
    room.matchTimeRemaining--;

    broadcastAll(room, { type: 'match_time_update', seconds: room.matchTimeRemaining });

    if (room.matchTimeRemaining <= 0) {
      clearInterval(room.matchTimerInterval);

      // КИНЕМАТИКА ПОБЕДЫ ВЫЖИВШИХ по истечению времени!
      if (room.globalTimer) clearInterval(room.globalTimer);
      if (room.itemTimer) clearInterval(room.itemTimer);
      if (room.feminiTimer) clearInterval(room.feminiTimer);
      if (room.manSkillAutoTimer) clearInterval(room.manSkillAutoTimer);

      broadcastAll(room, { type: 'survivor_wins_cinematic' });
      setTimeout(() => {
        broadcastAll(room, { type: 'game_over', winner: 'tsundere' });
        stopMatch(room);
      }, 10000);
    }
  }, 1000);

  if (room.globalTimer) clearInterval(room.globalTimer);
  room.globalTimer = setInterval(() => {
    if (room.state !== 'game') return;
    triggerGlobalEvent(room);
  }, 40000);

  if (room.itemTimer) clearInterval(room.itemTimer);
  const rotIntervalSec = room.itemRotateInterval !== undefined ? room.itemRotateInterval : 120;
  if (rotIntervalSec > 0) {
    room.itemTimer = setInterval(() => {
      if (room.state !== 'game') return;
      rotateItems(room);
    }, rotIntervalSec * 1000);
  }

  if (room.feminiTimer) clearInterval(room.feminiTimer);
  room.feminiTimer = setInterval(() => {
    if (room.state !== 'game') return;
    const hasFemini = Object.values(room.players).some(p => !p.isDead && p.currentItem === 'feminization');
    if (hasFemini) {
      broadcastAll(room, { type: 'play_sound', sound: 'gojo_kun.mp3' });
    }
  }, 10000);

  // ---- Автоматический запуск способности Маньяка («28 число Юки») каждые 40 секунд ----
  if (room.manSkillAutoTimer) clearInterval(room.manSkillAutoTimer);
  room.manSkillAutoTimer = setInterval(() => {
    if (room.state !== 'game') return;
    const yanderePlayer = Object.values(room.players).find(p => p.role === 'yandere');
    broadcastAll(room, { type: 'play_sound', sound: 'yuki_loves_me.mp3' });
    broadcastAll(room, { type: 'man_skill_used', from: yanderePlayer ? yanderePlayer.id : null });
  }, 40000);
}


function triggerGlobalEvent(room) {
  const events = ['wind', 'rocket', 'phone'];
  const ev = events[Math.floor(Math.random() * events.length)];

  if (ev === 'wind') {
    broadcastAll(room, {
      type: 'global_event',
      name: 'wind',
      message: '🌪 ВЕТЕР ЛОМАЕТ АНИМЕ-ПРИЧЕСКУ! Регенерация Кавайности заблокирована на 5с!',
      duration: 5
    });
    broadcastAll(room, { type: 'play_sound', sound: 'wind_whistle.mp3' });
  } else if (ev === 'rocket') {
    broadcastAll(room, {
      type: 'global_event',
      name: 'rocket',
      message: '🚀 РАКЕТНАЯ ОПАСНОСТЬ! HUD скрыт на 10 секунд!',
      duration: 10
    });
  } else if (ev === 'phone') {
    broadcastAll(room, {
      type: 'global_event',
      name: 'phone',
      message: '📱 СООБЩЕНИЕ НА ТЕЛЕФОН!',
      duration: 5
    });
  }
}

function rotateItems(room) {
  const newItems = {};
  Object.values(room.players).forEach(p => {
    if (p.role === 'tsundere' && !p.isDead) {
      const item = getRandomItemForPlayer(p);
      newItems[p.id] = item;
      broadcastAll(room, { type: 'model_change', id: p.id, itemId: item });
    }
  });
  broadcastAll(room, { type: 'item_rotate', playerItems: newItems });
}

function getGamePlayers(room) {
  return Object.values(room.players).map(p => ({
    id: p.id,
    name: p.name,
    role: p.role,
    hp: p.hp || 3,
    maxHp: p.maxHp || 3,
    isDead: p.isDead || false,
    spawn: p.spawn
  }));
}

const wss = new WebSocketServer({ server: httpServer });

wss.on('connection', (ws) => {
  const clientId = genId();
  ws._id = clientId;

  ws.send(JSON.stringify({ type: 'connected', id: clientId }));
  sendRoomList(ws);

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }

    switch (msg.type) {

      case 'get_rooms':
        sendRoomList(ws);
        break;

      case 'create_room': {
        const roomId = genId();
        const room = {
          id: roomId,
          hostId: clientId,
          name: msg.name || `Комната #${Object.keys(rooms).length + 1}`,
          maxPlayers: msg.maxPlayers || 4,
          state: 'lobby',
          map: msg.map || 'map_erd',
          players: {},
          countdown: null,
          globalTimer: null,
          itemTimer: null,
          feminiTimer: null,
        };
        rooms[roomId] = room;
        const player = { id: clientId, ws, name: msg.playerName || 'Player', role: msg.role || 'random', ready: false, hp: 3, maxHp: 3, isDead: false, usedItems: [] };
        room.players[clientId] = player;
        playerRoomMap[clientId] = roomId;
        ws.send(JSON.stringify({ type: 'room_joined', roomId, playerId: clientId, hostId: clientId }));
        sendLobbyState(room);
        break;
      }

      case 'join_room': {
        const room = rooms[msg.roomId];
        if (!room) { ws.send(JSON.stringify({ type: 'error', msg: 'Комната не найдена' })); break; }
        if (room.state !== 'lobby') { ws.send(JSON.stringify({ type: 'error', msg: 'Игра уже началась' })); break; }
        if (Object.keys(room.players).length >= room.maxPlayers) { ws.send(JSON.stringify({ type: 'error', msg: 'Комната полная' })); break; }
        const player = { id: clientId, ws, name: msg.playerName || 'Player', role: msg.role || 'tsundere', ready: false, hp: 3, maxHp: 3, isDead: false, usedItems: [] };
        room.players[clientId] = player;
        playerRoomMap[clientId] = msg.roomId;
        ws.send(JSON.stringify({ type: 'room_joined', roomId: msg.roomId, playerId: clientId, hostId: room.hostId }));
        sendLobbyState(room);
        break;
      }

      case 'set_role': {
        const rid = playerRoomMap[clientId];
        if (!rid || !rooms[rid]) break;
        const room = rooms[rid];
        const player = room.players[clientId];
        if (!player || room.state !== 'lobby') break;
        if (msg.role === 'yandere') {
          const existing = Object.values(room.players).find(p => p.role === 'yandere' && p.id !== clientId);
          if (existing) { ws.send(JSON.stringify({ type: 'error', msg: 'Яндере уже выбран!' })); break; }
        }
        player.role = msg.role;
        sendLobbyState(room);
        break;
      }

      case 'set_map': {
        const rid = playerRoomMap[clientId];
        if (!rid || !rooms[rid]) break;
        const room = rooms[rid];
        if (!room.players[clientId] || room.state !== 'lobby') break;
        room.map = msg.map || 'map_erd';
        sendLobbyState(room);
        break;
      }

      case 'set_match_duration': {
        const rid = playerRoomMap[clientId];
        if (!rid || !rooms[rid]) break;
        const room = rooms[rid];
        if (!room.players[clientId] || room.state !== 'lobby') break;
        room.matchDuration = parseInt(msg.duration) || 180;
        sendLobbyState(room);
        break;
      }

      case 'set_chase_music': {
        const rid = playerRoomMap[clientId];
        if (!rid || !rooms[rid]) break;
        const room = rooms[rid];
        if (!room.players[clientId] || room.state !== 'lobby') break;
        room.chaseMusic = msg.music || '624_-MATADORA.mp3';
        sendLobbyState(room);
        break;
      }

      case 'set_item_rotate_time': {
        const rid = playerRoomMap[clientId];
        if (!rid || !rooms[rid]) break;
        const room = rooms[rid];
        if (!room.players[clientId] || room.state !== 'lobby') break;
        room.itemRotateInterval = parseInt(msg.interval);
        sendLobbyState(room);
        break;
      }

      case 'set_ready': {
        const rid = playerRoomMap[clientId];
        if (!rid || !rooms[rid]) break;
        const room = rooms[rid];
        if (!room.players[clientId] || room.state !== 'lobby') break;
        room.players[clientId].ready = msg.ready;
        sendLobbyState(room);
        checkAndStartCountdown(room);
        break;
      }

      case 'cancel_ready': {
        const rid = playerRoomMap[clientId];
        if (!rid || !rooms[rid]) break;
        const room = rooms[rid];
        if (!room.players[clientId]) break;
        room.players[clientId].ready = false;
        if (room.state === 'countdown') {
          clearInterval(room.countdown);
          room.state = 'lobby';
          broadcastAll(room, { type: 'countdown_cancelled' });
        }
        sendLobbyState(room);
        break;
      }

      case 'player_move': {
        const rid = playerRoomMap[clientId];
        if (!rid || !rooms[rid]) break;
        const room = rooms[rid];
        broadcast(room, {
          type: 'player_move',
          id: clientId,
          pos: msg.pos,
          vel: msg.vel,
          rot: msg.rot,
          anim: msg.anim
        }, clientId);
        break;
      }

      case 'model_change': {
        const rid = playerRoomMap[clientId];
        if (!rid || !rooms[rid]) break;
        broadcastAll(rooms[rid], { type: 'model_change', id: clientId, itemId: msg.itemId });
        break;
      }

      case 'player_attack': {
        const rid = playerRoomMap[clientId];
        if (!rid || !rooms[rid]) break;
        const room = rooms[rid];
        const attacker = room.players[clientId];
        if (!attacker) break;

        broadcast(room, { type: 'player_attack', id: clientId, target: msg.target }, clientId);

        if (msg.target && room.players[msg.target]) {
          const victim = room.players[msg.target];
          if (victim.isDead) break;

          // ВЫЖИВШИМ НЕЛЬЗЯ БИТЬ СЛЕНДРИНУ/МАНЬЯКА! (Даже с Глазом Гуля — он действует только на союзиков!)
          if (attacker.role === 'tsundere' && victim.role === 'yandere') {
            break;
          }

          if (attacker.role === 'yandere') {
            broadcastAll(room, { type: 'play_sound', sound: 'ai_baka.mp3' });
          }
          broadcastAll(room, { type: 'play_sound', sound: 'whip.mp3' });

          const dmg = msg.amount || 1;
          victim.hp = Math.max(0, victim.hp - dmg);

          if (msg.isGhoulEye) {
            attacker.hp += dmg;
            broadcastAll(room, { type: 'player_hp_update', id: attacker.id, hp: attacker.hp, maxHp: attacker.maxHp });
          }

          broadcastAll(room, { type: 'player_hp_update', id: victim.id, hp: victim.hp, maxHp: victim.maxHp });

          if (victim.hp <= 0 && !victim.isDead) {
            victim.isDead = true;

            const aliveTsunderes = Object.values(room.players).filter(p => p.role === 'tsundere' && !p.isDead);
            const isFinalKill = (aliveTsunderes.length === 0);

            // СКРИМЕР: отправляем только жертве при наступлении СМЕРТИ (передаём флаг последнего убийства)
            if (attacker.role === 'yandere' && victim.ws && victim.ws.readyState === 1) {
              victim.ws.send(JSON.stringify({ type: 'scream_event', isFinalKill }));
            }

            broadcastAll(room, {
              type: 'killfeed',
              text: `Вороны Итачи забрали ${victim.name}`
            });
            broadcastAll(room, { type: 'player_eliminated', id: victim.id, name: victim.name });

            if (isFinalKill) {
              // КИНЕМАТИКА ПОБЕДЫ МАНЬЯКА — глушим таймеры и ждём 1.0с чтобы у жертвы успел отыграть скример!
              if (room.globalTimer) clearInterval(room.globalTimer);
              if (room.itemTimer) clearInterval(room.itemTimer);
              if (room.feminiTimer) clearInterval(room.feminiTimer);
              if (room.manSkillAutoTimer) clearInterval(room.manSkillAutoTimer);

              setTimeout(() => {
                broadcastAll(room, { type: 'yandere_wins_cinematic' });
                setTimeout(() => {
                  broadcastAll(room, { type: 'game_over', winner: 'yandere' });
                  stopMatch(room);
                }, 8000); // 8 секунд — дать видео поиграть
              }, 1000);
            }
          }
        }
        break;
      }

      case 'model_change': {
        const rid = playerRoomMap[clientId];
        if (!rid || !rooms[rid]) break;
        broadcastAll(rooms[rid], { type: 'model_change', id: clientId, itemId: msg.itemId });
        break;
      }

      case 'throw_glue': {
        const rid = playerRoomMap[clientId];
        if (!rid || !rooms[rid]) break;
        broadcast(rooms[rid], { type: 'throw_glue', id: clientId, pos: msg.pos, dir: msg.dir }, clientId);
        break;
      }

      case 'glue_hit': {
        const rid = playerRoomMap[clientId];
        if (!rid || !rooms[rid]) break;
        broadcastAll(rooms[rid], { type: 'glue_hit', pos: msg.pos, from: clientId });
        break;
      }

      case 'man_skill': {
        const rid = playerRoomMap[clientId];
        if (!rid || !rooms[rid]) break;
        broadcastAll(rooms[rid], { type: 'play_sound', sound: 'yuki_loves_me.mp3' });
        broadcastAll(rooms[rid], { type: 'man_skill_used', from: clientId });
        break;
      }

      case 'yarik_skill': {
        const rid = playerRoomMap[clientId];
        if (!rid || !rooms[rid]) break;
        const room = rooms[rid];
        broadcastAll(room, { type: 'yarik_text', text: 'вы знаете кто такой Барт симсон' });
        setTimeout(() => {
          if (room.state !== 'game') return;
          broadcastAll(room, { type: 'yarik_event_trigger' });
        }, 5000);
        break;
      }

      case 'player_eliminated': {
        const rid = playerRoomMap[clientId];
        if (!rid || !rooms[rid]) break;
        const room = rooms[rid];
        const victim = room.players[msg.victimId];
        if (victim && !victim.isDead) {
          victim.isDead = true;
          broadcastAll(room, { type: 'killfeed', text: `Вороны Итачи забрали ${victim.name}` });
          broadcastAll(room, { type: 'player_eliminated', id: msg.victimId, name: victim.name });
        }
        const aliveTsunderes = Object.values(room.players).filter(p => p.role === 'tsundere' && !p.isDead);
        if (aliveTsunderes.length === 0) {
          broadcastAll(room, { type: 'game_over', winner: 'yandere' });
          stopMatch(room);
        }
        break;
      }

      default:
        break;
    }
  });

  ws.on('close', () => {
    const rid = playerRoomMap[clientId];
    if (rid && rooms[rid]) {
      const room = rooms[rid];
      delete room.players[clientId];
      delete playerRoomMap[clientId];

      if (room.hostId === clientId) {
        const remainingIds = Object.keys(room.players);
        if (remainingIds.length > 0) {
          room.hostId = remainingIds[0];
        }
      }

      broadcast(room, { type: 'player_left', id: clientId, newHostId: room.hostId });
      if (Object.keys(room.players).length === 0) {
        stopMatch(room);
        delete rooms[rid];
      } else {
        sendLobbyState(room);
      }
    }
  });
});

function stopMatch(room) {
  if (room.globalTimer) clearInterval(room.globalTimer);
  if (room.itemTimer) clearInterval(room.itemTimer);
  if (room.feminiTimer) clearInterval(room.feminiTimer);
  if (room.manSkillAutoTimer) clearInterval(room.manSkillAutoTimer);
  if (room.matchTimerInterval) clearInterval(room.matchTimerInterval);
  room.state = 'lobby';
  Object.values(room.players).forEach(p => { p.ready = false; });
}

const os = require('os');

function getLocalIPs() {
  const ips = [];
  const ifaces = os.networkInterfaces();
  for (const name of Object.keys(ifaces)) {
    for (const iface of ifaces[name]) {
      if (iface.family === 'IPv4' && !iface.internal) ips.push(iface.address);
    }
  }
  return ips;
}

// Слушаем на ВСЕХ интерфейсах (0.0.0.0) — иначе с других ПК в локальной сети не подключиться!
httpServer.listen(PORT, '0.0.0.0', () => {
  console.log(`\n🎮  Yandere vs Tsunderes — сервер запущен!`);
  console.log(`🌐  Твой браузер:     http://localhost:${PORT}`);
  const ips = getLocalIPs();
  if (ips.length > 0) {
    console.log(`🔗  Другие игроки в локальной сети вводят в поле "Адрес сервера":`);
    ips.forEach(ip => console.log(`      ${ip}:${PORT}`));
  }
  console.log(`\n⚠   Для игры через интернет: используй Hamachi, ZeroTier или проброс порта ${PORT}.\n`);
});




