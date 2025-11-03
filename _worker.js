// Telegram 双向机器人 — 完整代码（含 5 次错判封禁、24h 无交互删话题、管理员私聊面板、可读剩余时间）
let BOT_TOKEN;
let GROUP_ID;
let MAX_MESSAGES_PER_MINUTE;

let lastCleanupTime = 0;
const CLEANUP_INTERVAL = 24 * 60 * 60 * 1000; // 24 小时
let isInitialized = false;
const processedMessages = new Set();
const processedCallbacks = new Set();

const topicCreationLocks = new Map();

const settingsCache = new Map([
  ['verification_enabled', null],
  ['user_raw_enabled', null]
]);

class LRUCache {
  constructor(maxSize) {
    this.maxSize = maxSize;
    this.cache = new Map();
  }
  get(key) {
    const value = this.cache.get(key);
    if (value !== undefined) {
      this.cache.delete(key);
      this.cache.set(key, value);
    }
    return value;
  }
  set(key, value) {
    if (this.cache.size >= this.maxSize) {
      const firstKey = this.cache.keys().next().value;
      this.cache.delete(firstKey);
    }
    this.cache.set(key, value);
  }
  clear() {
    this.cache.clear();
  }
}

const userInfoCache = new LRUCache(1000);
const topicIdCache = new LRUCache(1000);
const userStateCache = new LRUCache(1000);
const messageRateCache = new LRUCache(1000);

// ---------- 时间与日志工具 ----------
function formatShanghai(date = new Date()) {
  const df = new Intl.DateTimeFormat('zh-CN', {
    timeZone: 'Asia/Shanghai',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: false
  });
  const parts = df.formatToParts(date);
  const map = {};
  parts.forEach(p => { if (p.type !== 'literal') map[p.type] = p.value; });
  return `${map.year}-${map.month}-${map.day} ${map.hour}:${map.minute}:${map.second}`;
}

/**
 * 将剩余秒数格式化为中文友好表达（精确到秒）
 * examples:
 *  - 3663 -> "还剩 1 小时 1 分钟 3 秒"
 *  - 3600 -> "还剩 1 小时"
 *  - 125  -> "还剩 2 分钟 5 秒"
 *  - 30   -> "还剩 30 秒"
 *  - 0    -> "已到期"
 */
function formatRemainingTime(seconds) {
  if (!seconds || seconds <= 0) return '已到期';
  const hrs = Math.floor(seconds / 3600);
  const mins = Math.floor((seconds % 3600) / 60);
  const secs = Math.floor(seconds % 60);
  const parts = [];
  if (hrs > 0) parts.push(`${hrs} 小时`);
  if (mins > 0) parts.push(`${mins} 分钟`);
  if (secs > 0) parts.push(`${secs} 秒`);
  if (parts.length === 0) return '还剩不到1秒';
  return `还剩 ${parts.join(' ')}`;
}

function logger(level, message, meta) {
  const ts = formatShanghai();
  const metaStr = meta ? ` | ${typeof meta === 'string' ? meta : JSON.stringify(meta)}` : '';
  if (level === 'ERROR') {
    console.error(`[${ts}] [${level}] ${message}${metaStr}`);
  } else if (level === 'WARN') {
    console.warn(`[${ts}] [${level}] ${message}${metaStr}`);
  } else {
    console.log(`[${ts}] [${level}] ${message}${metaStr}`);
  }
}
// ---------- end 时间与日志 ----------

export default {
  async fetch(request, env) {
    BOT_TOKEN = env.BOT_TOKEN_ENV || null;
    GROUP_ID = env.GROUP_ID_ENV || null;
    MAX_MESSAGES_PER_MINUTE = env.MAX_MESSAGES_PER_MINUTE_ENV ? parseInt(env.MAX_MESSAGES_PER_MINUTE_ENV) : 40;

    if (!env.D1) {
      return new Response('Server configuration error: D1 database is not bound', { status: 500 });
    }

    if (!isInitialized) {
      await initialize(env.D1, request);
      isInitialized = true;
    }

    async function handleRequest(request) {
      if (!BOT_TOKEN || !GROUP_ID) {
        return new Response('Server configuration error: Missing required environment variables', { status: 500 });
      }

      const url = new URL(request.url);
      if (url.pathname === '/webhook') {
        try {
          const update = await request.json();
          await handleUpdate(update);
          return new Response('OK');
        } catch (error) {
          logger('ERROR', 'Invalid webhook request', error.message);
          return new Response('Bad Request', { status: 400 });
        }
      } else if (url.pathname === '/registerWebhook') {
        return await registerWebhook(request);
      } else if (url.pathname === '/unRegisterWebhook') {
        return await unRegisterWebhook();
      } else if (url.pathname === '/checkTables') {
        await checkAndRepairTables(env.D1);
        await Promise.all([cleanExpiredVerificationCodes(env.D1), cleanInactiveTopics(env.D1)]);
        return new Response('Database tables checked and repaired', { status: 200 });
      }
      return new Response('Not Found', { status: 404 });
    }

    async function initialize(d1, request) {
      await Promise.all([
        checkAndRepairTables(d1),
        autoRegisterWebhook(request),
        checkBotPermissions(),
        cleanExpiredVerificationCodes(d1),
        cleanInactiveTopics(d1)
      ]);
      logger('INFO', 'Initialization complete');
    }

    async function autoRegisterWebhook(request) {
      const webhookUrl = `${new URL(request.url).origin}/webhook`;
      try {
        await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/setWebhook`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ url: webhookUrl }),
        });
        logger('INFO', 'Auto-registered webhook', webhookUrl);
      } catch (e) {
        logger('WARN', 'Failed to auto-register webhook', e.message);
      }
    }

    async function checkBotPermissions() {
      const response = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/getChat`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ chat_id: GROUP_ID })
      });
      const data = await response.json();
      if (!data.ok) {
        throw new Error(`Failed to access group: ${data.description}`);
      }

      const memberResponse = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/getChatMember`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          chat_id: GROUP_ID,
          user_id: (await getBotId())
        })
      });
      const memberData = await memberResponse.json();
      if (!memberData.ok) {
        throw new Error(`Failed to get bot member status: ${memberData.description}`);
      }
      logger('INFO', 'Bot permissions checked');
    }

    async function getBotId() {
      const response = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/getMe`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({})
      });
      const data = await response.json();
      if (!data.ok) throw new Error(`Failed to get bot ID: ${data.description}`);
      return data.result.id;
    }

    async function checkAndRepairTables(d1) {
      const expectedTables = {
        user_states: {
          columns: {
            chat_id: 'TEXT PRIMARY KEY',
            is_blocked: 'BOOLEAN DEFAULT FALSE',
            is_verified: 'BOOLEAN DEFAULT FALSE',
            verified_expiry: 'INTEGER',
            verification_code: 'TEXT',
            code_expiry: 'INTEGER',
            last_verification_message_id: 'TEXT',
            is_first_verification: 'BOOLEAN DEFAULT TRUE',
            is_rate_limited: 'BOOLEAN DEFAULT FALSE',
            is_verifying: 'BOOLEAN DEFAULT FALSE',
            verification_fail_count: 'INTEGER DEFAULT 0',
            verification_fail_block_until: 'INTEGER'
          }
        },
        message_rates: {
          columns: {
            chat_id: 'TEXT PRIMARY KEY',
            message_count: 'INTEGER DEFAULT 0',
            window_start: 'INTEGER',
            start_count: 'INTEGER DEFAULT 0',
            start_window_start: 'INTEGER'
          }
        },
        chat_topic_mappings: {
          columns: {
            chat_id: 'TEXT PRIMARY KEY',
            topic_id: 'TEXT NOT NULL',
            created_at: 'INTEGER',
            last_interaction: 'INTEGER'
          }
        },
        settings: {
          columns: {
            key: 'TEXT PRIMARY KEY',
            value: 'TEXT'
          }
        }
      };

      for (const [tableName, structure] of Object.entries(expectedTables)) {
        const tableInfo = await d1.prepare(
          `SELECT sql FROM sqlite_master WHERE type='table' AND name=?`
        ).bind(tableName).first();

        if (!tableInfo) {
          await createTable(d1, tableName, structure);
          logger('INFO', `Created missing table ${tableName}`);
          continue;
        }

        const columnsResult = await d1.prepare(
          `PRAGMA table_info(${tableName})`
        ).all();

        const currentColumns = new Map(
          columnsResult.results.map(col => [col.name, {
            type: col.type,
            notnull: col.notnull,
            dflt_value: col.dflt_value
          }])
        );

        for (const [colName, colDef] of Object.entries(structure.columns)) {
          if (!currentColumns.has(colName)) {
            try {
              const addColumnSQL = `ALTER TABLE ${tableName} ADD COLUMN ${colName} ${colDef}`;
              await d1.exec(addColumnSQL);
              logger('INFO', `Added column ${colName} to ${tableName}`);
            } catch (err) {
              logger('WARN', `Failed to add column ${colName} to ${tableName}`, err.message);
            }
          }
        }

        if (tableName === 'settings') {
          await d1.exec('CREATE INDEX IF NOT EXISTS idx_settings_key ON settings (key)');
        }
      }

      await Promise.all([
        d1.prepare('INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)')
          .bind('verification_enabled', 'true').run(),
        d1.prepare('INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)')
          .bind('user_raw_enabled', 'true').run()
      ]);

      settingsCache.set('verification_enabled', (await getSetting('verification_enabled', d1)) === 'true');
      settingsCache.set('user_raw_enabled', (await getSetting('user_raw_enabled', d1)) === 'true');

      logger('INFO', 'checkAndRepairTables finished');
    }

    async function createTable(d1, tableName, structure) {
      const columnsDef = Object.entries(structure.columns)
        .map(([name, def]) => `${name} ${def}`)
        .join(', ');
      const createSQL = `CREATE TABLE ${tableName} (${columnsDef})`;
      await d1.exec(createSQL);
    }

    async function cleanExpiredVerificationCodes(d1) {
      const now = Date.now();
      if (now - lastCleanupTime < CLEANUP_INTERVAL) {
        return;
      }

      const nowSeconds = Math.floor(now / 1000);
      const expiredCodes = await d1.prepare(
        'SELECT chat_id FROM user_states WHERE code_expiry IS NOT NULL AND code_expiry < ?'
      ).bind(nowSeconds).all();

      if (expiredCodes.results.length > 0) {
        await d1.batch(
          expiredCodes.results.map(({ chat_id }) =>
            d1.prepare(
              'UPDATE user_states SET verification_code = NULL, code_expiry = NULL, is_verifying = FALSE WHERE chat_id = ?'
            ).bind(chat_id)
          )
        );
        logger('INFO', `Cleaned ${expiredCodes.results.length} expired verification codes`);
      }
      lastCleanupTime = now;
    }

    async function cleanInactiveTopics(d1) {
      try {
        const now = Date.now();
        const cutoff = now - 24 * 60 * 60 * 1000;
        const rows = await d1.prepare('SELECT chat_id, topic_id, created_at, last_interaction FROM chat_topic_mappings WHERE created_at IS NOT NULL').all();
        const toDelete = rows.results.filter(r => r.created_at && r.last_interaction && (r.created_at <= cutoff) && (r.last_interaction === r.created_at));
        if (toDelete.length === 0) return;
        logger('INFO', `Found ${toDelete.length} inactive topics to delete`);

        for (const row of toDelete) {
          const topicId = row.topic_id;
          const chatId = row.chat_id;
          try {
            const delResp = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/deleteForumTopic`, {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({
                chat_id: GROUP_ID,
                message_thread_id: Number(topicId)
              })
            });
            const delData = await delResp.json().catch(() => ({ ok: false, description: 'invalid json' }));
            if (!delData.ok) {
              logger('WARN', `Failed to delete forum topic ${topicId}`, delData.description || JSON.stringify(delData));
            } else {
              logger('INFO', `Deleted inactive topic ${topicId}`);
            }
          } catch (err) {
            logger('WARN', `deleteForumTopic error for ${topicId}`, err.message);
          } finally {
            await d1.prepare('DELETE FROM chat_topic_mappings WHERE chat_id = ?').bind(chatId).run();
            topicIdCache.set(chatId, undefined);
            logger('INFO', `Cleaned DB mapping for chat ${chatId}`);
          }
        }
      } catch (err) {
        logger('ERROR', 'cleanInactiveTopics error', err.message);
      }
    }

    async function handleUpdate(update) {
      await Promise.all([cleanExpiredVerificationCodes(env.D1), cleanInactiveTopics(env.D1)]).catch((e) => {
        logger('WARN', 'Periodic cleanup error', e.message);
      });

      if (update.message) {
        const messageId = update.message.message_id.toString();
        const chatId = update.message.chat.id.toString();
        const messageKey = `${chatId}:${messageId}`;

        if (processedMessages.has(messageKey)) {
          return;
        }
        processedMessages.add(messageKey);

        if (processedMessages.size > 10000) {
          processedMessages.clear();
          logger('DEBUG', 'processedMessages cache cleared to free memory');
        }

        await onMessage(update.message);
      } else if (update.callback_query) {
        await onCallbackQuery(update.callback_query);
      }
    }

    async function onMessage(message) {
      const chatId = message.chat.id.toString();
      const text = message.text || '';
      const messageId = message.message_id;

      if (chatId === GROUP_ID) {
        const topicId = message.message_thread_id;
        if (topicId) {
          const privateChatId = await getPrivateChatId(topicId);
          if (privateChatId && text === '/admin') {
            await sendAdminPanel(chatId, topicId, privateChatId, messageId);
            return;
          }
          if (privateChatId && text.startsWith('/reset_user')) {
            await handleResetUser(chatId, topicId, text);
            return;
          }
          if (privateChatId) {
            await forwardMessageToPrivateChat(privateChatId, message);
            return;
          }
        }
        return;
      }

      let userState = userStateCache.get(chatId);
      if (userState === undefined) {
        userState = await env.D1.prepare('SELECT is_blocked, is_first_verification, is_verified, verified_expiry, is_verifying, verification_fail_count, verification_fail_block_until FROM user_states WHERE chat_id = ?')
          .bind(chatId)
          .first();
        if (!userState) {
          userState = { is_blocked: false, is_first_verification: true, is_verified: false, verified_expiry: null, is_verifying: false, verification_fail_count: 0, verification_fail_block_until: null };
          await env.D1.prepare('INSERT INTO user_states (chat_id, is_blocked, is_first_verification, is_verified, is_verifying, verification_fail_count) VALUES (?, ?, ?, ?, ?, ?)')
            .bind(chatId, false, true, false, false, 0)
            .run();
        }
        userStateCache.set(chatId, userState);
      }

      if (userState.is_blocked) {
        await sendMessageToUser(chatId, "您已被拉黑，无法发送消息！");
        return;
      }

      let isAdmin = false;
      try {
        if (message.chat && message.chat.type === 'private') {
          isAdmin = await checkIfAdmin(Number(chatId));
        }
      } catch (err) {
        logger('WARN', 'checkIfAdmin failed', err.message);
      }

      if (text === '/admin' && message.chat && message.chat.type === 'private' && isAdmin) {
        await sendPrivateAdminPanel(chatId);
        return;
      }

      const verificationEnabled = (await getSetting('verification_enabled', env.D1)) === 'true';

      if (!isAdmin) {
        if (!verificationEnabled) {
          // 验证关闭，直接通过
        } else {
          const nowSeconds = Math.floor(Date.now() / 1000);
          const isVerified = userState.is_verified && userState.verified_expiry && nowSeconds < userState.verified_expiry;
          const isFirstVerification = userState.is_first_verification;
          const isRateLimited = await checkMessageRate(chatId);
          const isVerifying = userState.is_verifying || false;

          if (userState.verification_fail_block_until && nowSeconds < userState.verification_fail_block_until) {
            const remainingSec = userState.verification_fail_block_until - nowSeconds;
            const remainingFriendly = formatRemainingTime(remainingSec);
            const untilStr = formatShanghai(new Date(userState.verification_fail_block_until * 1000));
            await sendMessageToUser(chatId, `由于连续多次验证失败，您已被限制验证。${remainingFriendly}，解禁时间：${untilStr}`);
            return;
          }

          if (!isVerified || (isRateLimited && !isFirstVerification)) {
            if (isVerifying) {
              const storedCode = await env.D1.prepare('SELECT verification_code, code_expiry FROM user_states WHERE chat_id = ?')
                .bind(chatId)
                .first();

              const nowSeconds2 = Math.floor(Date.now() / 1000);
              const isCodeExpired = !storedCode?.verification_code || !storedCode?.code_expiry || nowSeconds2 > storedCode.code_expiry;

              if (isCodeExpired) {
                await sendMessageToUser(chatId, '验证码已过期，正在为您发送新的验证码...');
                await env.D1.prepare('UPDATE user_states SET verification_code = NULL, code_expiry = NULL, is_verifying = FALSE WHERE chat_id = ?')
                  .bind(chatId)
                  .run();
                userStateCache.set(chatId, { ...userState, verification_code: null, code_expiry: null, is_verifying: false });

                try {
                  const lastVerification = await env.D1.prepare('SELECT last_verification_message_id FROM user_states WHERE chat_id = ?')
                    .bind(chatId)
                    .first();

                  if (lastVerification?.last_verification_message_id) {
                    try {
                      await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/deleteMessage`, {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({
                          chat_id: chatId,
                          message_id: lastVerification.last_verification_message_id
                        })
                      });
                    } catch (deleteError) {
                      logger('WARN', 'Failed to delete old verification message', deleteError.message);
                    }

                    await env.D1.prepare('UPDATE user_states SET last_verification_message_id = NULL WHERE chat_id = ?')
                      .bind(chatId)
                      .run();
                  }
                } catch (error) {
                  logger('WARN', 'Failed to query last verification message', error.message);
                }

                try {
                  await handleVerification(chatId, 0);
                } catch (verificationError) {
                  logger('ERROR', 'Failed to send new verification after expiry', verificationError.message);
                  setTimeout(async () => {
                    try { await handleVerification(chatId, 0); } catch (retryError) {
                      logger('ERROR', 'Retry send verification failed', retryError.message);
                      await sendMessageToUser(chatId, '发送验证码失败，请发送任意消息重试');
                    }
                  }, 1000);
                }
                return;
              } else {
                await sendMessageToUser(chatId, `请完成验证后发送消息"${text || '您的具体信息'}"。`);
              }
              return;
            }
            await sendMessageToUser(chatId, `请完成验证后发送消息"${text || '您的具体信息'}"。`);
            await handleVerification(chatId, messageId);
            return;
          }
        }
      }

      if (text === '/start') {
        if (!isAdmin && await checkStartCommandRate(chatId)) {
          await sendMessageToUser(chatId, "您发送 /start 命令过于频繁，请稍后再试！");
          return;
        }

        const successMessage = await getVerificationSuccessMessage();
        await sendMessageToUser(chatId, `${successMessage}\n你可以用这个机器人跟我对话。写下您想要发送的消息（图片、视频），我会尽快回复您！`);
        if (!isAdmin) {
          const userInfo = await getUserInfo(chatId);
          await ensureUserTopic(chatId, userInfo);
        }
        return;
      }

      const userInfo = await getUserInfo(chatId);
      if (!userInfo) {
        await sendMessageToUser(chatId, "无法获取用户信息，请稍后再试或联系管理员。");
        return;
      }

      let topicId = null;
      if (!isAdmin) {
        topicId = await ensureUserTopic(chatId, userInfo);
        if (!topicId) {
          await sendMessageToUser(chatId, "无法创建话题，请稍后再试或联系管理员。");
          return;
        }

        const isTopicValid = await validateTopic(topicId);
        if (!isTopicValid) {
          await env.D1.prepare('DELETE FROM chat_topic_mappings WHERE chat_id = ?').bind(chatId).run();
          topicIdCache.set(chatId, undefined);
          topicId = await ensureUserTopic(chatId, userInfo);
          if (!topicId) {
            await sendMessageToUser(chatId, "无法重新创建话题，请稍后再试或联系管理员。");
            return;
          }
        }
      }

      const userName = userInfo.username || `User_${chatId}`;
      const nickname = userInfo.nickname || userName;

      if (text) {
        const formattedMessage = `${nickname}:\n${text}`;
        if (topicId) {
          await sendMessageToTopic(topicId, formattedMessage);
        } else {
          await sendMessageToUser(chatId, "管理员消息已接收（私聊模式），如需转发请使用群组内功能。");
        }
      } else {
        if (topicId) {
          await copyMessageToTopic(topicId, message);
        } else {
          await sendMessageToUser(chatId, "无法处理该消息类型（管理员私聊未创建话题）。");
        }
      }
    }

    async function validateTopic(topicId) {
      try {
        const response = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/sendMessage`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            chat_id: GROUP_ID,
            message_thread_id: topicId,
            text: "您有新消息！",
            disable_notification: true
          })
        });
        const data = await response.json();
        if (data.ok) {
          await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/deleteMessage`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
              chat_id: GROUP_ID,
              message_id: data.result.message_id
            })
          });
          return true;
        }
        return false;
      } catch (error) {
        return false;
      }
    }

    async function ensureUserTopic(chatId, userInfo) {
      let lock = topicCreationLocks.get(chatId);
      if (!lock) {
        lock = Promise.resolve();
        topicCreationLocks.set(chatId, lock);
      }

      try {
        await lock;

        let topicId = await getExistingTopicId(chatId);
        if (topicId) {
          return topicId;
        }

        const newLock = (async () => {
          const userName = userInfo.username || `User_${chatId}`;
          const nickname = userInfo.nickname || userName;
          const topicIdInner = await createForumTopic(nickname, userName, nickname, userInfo.id || chatId);
          await saveTopicId(chatId, topicIdInner, Date.now(), Date.now());
          return topicIdInner;
        })();

        topicCreationLocks.set(chatId, newLock);
        return await newLock;
      } finally {
        if (topicCreationLocks.get(chatId) === lock) {
          topicCreationLocks.delete(chatId);
        }
      }
    }

    async function handleResetUser(chatId, topicId, text) {
      const senderId = chatId;
      const isAdmin = await checkIfAdmin(senderId);
      if (!isAdmin) {
        await sendMessageToTopic(topicId, '只有管理员可以使用此功能。');
        return;
      }

      const parts = text.split(' ');
      if (parts.length !== 2) {
        await sendMessageToTopic(topicId, '用法：/reset_user <chat_id>');
        return;
      }

      const targetChatId = parts[1];
      await env.D1.batch([
        env.D1.prepare('DELETE FROM user_states WHERE chat_id = ?').bind(targetChatId),
        env.D1.prepare('DELETE FROM message_rates WHERE chat_id = ?').bind(targetChatId),
        env.D1.prepare('DELETE FROM chat_topic_mappings WHERE chat_id = ?').bind(targetChatId)
      ]);
      userStateCache.set(targetChatId, undefined);
      messageRateCache.set(targetChatId, undefined);
      topicIdCache.set(targetChatId, undefined);
      await sendMessageToTopic(topicId, `用户 ${targetChatId} 的状态已重置。`);
    }

    async function sendAdminPanel(chatId, topicId, privateChatId, messageId) {
      const verificationEnabled = (await getSetting('verification_enabled', env.D1)) === 'true';
      const userRawEnabled = (await getSetting('user_raw_enabled', env.D1)) === 'true';

      const buttons = [
        [
          { text: '拉黑用户', callback_data: `block_${privateChatId}` },
          { text: '解除拉黑', callback_data: `unblock_${privateChatId}` }
        ],
        [
          { text: verificationEnabled ? '关闭验证码' : '开启验证码', callback_data: `toggle_verification_${privateChatId}` },
          { text: '查询黑名单', callback_data: `check_blocklist_${privateChatId}` }
        ],
        [
          { text: '删除会话', callback_data: `delete_user_${privateChatId}` }
        ]
      ];

      const adminMessage = '管理员面板：请选择操作';
      await Promise.all([
        fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/sendMessage`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            chat_id: chatId,
            message_thread_id: topicId,
            text: adminMessage,
            reply_markup: { inline_keyboard: buttons }
          })
        }),
        fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/deleteMessage`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            chat_id: chatId,
            message_id: messageId
          })
        })
      ]);
    }

    async function sendPrivateAdminPanel(chatId) {
      const blockedUsers = await env.D1.prepare('SELECT chat_id FROM user_states WHERE is_blocked = ?')
        .bind(true)
        .all();

      const rows = blockedUsers.results || [];
      const inline_keyboard = [];

      if (rows.length === 0) {
        inline_keyboard.push([{ text: '当前没有被拉黑的用户', callback_data: `noop` }]);
      } else {
        for (const row of rows) {
          inline_keyboard.push([{ text: `解除 ${row.chat_id}`, callback_data: `private_unblock_${row.chat_id}` }]);
        }
      }

      inline_keyboard.push([{ text: (await getSetting('verification_enabled', env.D1)) === 'true' ? '关闭验证码' : '开启验证码', callback_data: 'toggle_verification_private' }]);

      await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/sendMessage`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          chat_id: chatId,
          text: '私聊管理员面板：',
          reply_markup: { inline_keyboard }
        })
      });
    }

    async function getVerificationSuccessMessage() {
      const userRawEnabled = (await getSetting('user_raw_enabled', env.D1)) === 'true';
      if (!userRawEnabled) return '验证成功。';

      try {
        const response = await fetch('https://raw.githubusercontent.com/wuyangdaily/ctt/refs/heads/main/CFTeleTrans/start.md');
        if (!response.ok) return '验证成功。';
        const message = await response.text();
        return message.trim() || '验证成功。';
      } catch (e) {
        return '验证成功。';
      }
    }

    async function getNotificationContent() {
      try {
        const response = await fetch('https://raw.githubusercontent.com/wuyangdaily/ctt/refs/heads/main/CFTeleTrans/notification.md');
        if (!response.ok) return '';
        const content = await response.text();
        return content.trim() || '';
      } catch (e) {
        return '';
      }
    }

    async function checkStartCommandRate(chatId) {
      const now = Date.now();
      const window = 5 * 60 * 1000;
      const maxStartsPerWindow = 1;

      let data = messageRateCache.get(chatId);
      if (data === undefined) {
        data = await env.D1.prepare('SELECT start_count, start_window_start FROM message_rates WHERE chat_id = ?')
          .bind(chatId)
          .first();
        if (!data) {
          data = { start_count: 0, start_window_start: now };
          await env.D1.prepare('INSERT INTO message_rates (chat_id, start_count, start_window_start) VALUES (?, ?, ?)')
            .bind(chatId, data.start_count, data.start_window_start)
            .run();
        }
        messageRateCache.set(chatId, data);
      }

      if (now - data.start_window_start > window) {
        data.start_count = 1;
        data.start_window_start = now;
        await env.D1.prepare('UPDATE message_rates SET start_count = ?, start_window_start = ? WHERE chat_id = ?')
          .bind(data.start_count, data.start_window_start, chatId)
          .run();
      } else {
        data.start_count += 1;
        await env.D1.prepare('UPDATE message_rates SET start_count = ? WHERE chat_id = ?')
          .bind(data.start_count, chatId)
          .run();
      }

      messageRateCache.set(chatId, data);
      return data.start_count > maxStartsPerWindow;
    }

    async function checkMessageRate(chatId) {
      const now = Date.now();
      const window = 60 * 1000;

      let data = messageRateCache.get(chatId);
      if (data === undefined) {
        data = await env.D1.prepare('SELECT message_count, window_start FROM message_rates WHERE chat_id = ?')
          .bind(chatId)
          .first();
        if (!data) {
          data = { message_count: 0, window_start: now };
          await env.D1.prepare('INSERT INTO message_rates (chat_id, message_count, window_start) VALUES (?, ?, ?)')
            .bind(chatId, data.message_count, data.window_start)
            .run();
        }
        messageRateCache.set(chatId, data);
      }

      if (now - data.window_start > window) {
        data.message_count = 1;
        data.window_start = now;
      } else {
        data.message_count += 1;
      }

      messageRateCache.set(chatId, data);
      await env.D1.prepare('UPDATE message_rates SET message_count = ?, window_start = ? WHERE chat_id = ?')
        .bind(data.message_count, data.window_start, chatId)
        .run();
      return data.message_count > MAX_MESSAGES_PER_MINUTE;
    }

    async function getSetting(key, d1) {
      const result = await d1.prepare('SELECT value FROM settings WHERE key = ?')
        .bind(key)
        .first();
      return result?.value || null;
    }

    async function setSetting(key, value) {
      await env.D1.prepare('INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)')
        .bind(key, value)
        .run();
      if (key === 'verification_enabled') {
        settingsCache.set('verification_enabled', value === 'true');
        if (value === 'false') {
          const nowSeconds = Math.floor(Date.now() / 1000);
          const verifiedExpiry = nowSeconds + 3600 * 24;
          await env.D1.prepare('UPDATE user_states SET is_verified = ?, verified_expiry = ?, is_verifying = ?, verification_code = NULL, code_expiry = NULL, is_first_verification = ? WHERE chat_id NOT IN (SELECT chat_id FROM user_states WHERE is_blocked = TRUE)')
            .bind(true, verifiedExpiry, false, false)
            .run();
          userStateCache.clear();
        }
      } else if (key === 'user_raw_enabled') {
        settingsCache.set('user_raw_enabled', value === 'true');
      }
      logger('INFO', `Setting changed ${key} = ${value}`);
    }

    async function onCallbackQuery(callbackQuery) {
      const chatId = callbackQuery.message.chat.id.toString();
      const topicId = callbackQuery.message.message_thread_id;
      const data = callbackQuery.data;
      const messageId = callbackQuery.message.message_id;
      const callbackKey = `${chatId}:${callbackQuery.id}`;

      if (processedCallbacks.has(callbackKey)) {
        return;
      }
      processedCallbacks.add(callbackKey);

      const parts = data.split('_');
      let action;
      let privateChatId;

      if (data.startsWith('verify_')) {
        action = 'verify';
        privateChatId = parts[1];
      } else if (data.startsWith('toggle_verification_')) {
        action = 'toggle_verification';
        privateChatId = parts.slice(2).join('_');
      } else if (data.startsWith('toggle_user_raw_')) {
        action = 'toggle_user_raw';
        privateChatId = parts.slice(3).join('_');
      } else if (data.startsWith('check_blocklist_')) {
        action = 'check_blocklist';
        privateChatId = parts.slice(2).join('_');
      } else if (data.startsWith('block_')) {
        action = 'block';
        privateChatId = parts.slice(1).join('_');
      } else if (data.startsWith('unblock_')) {
        action = 'unblock';
        privateChatId = parts.slice(1).join('_');
      } else if (data.startsWith('delete_user_')) {
        action = 'delete_user';
        privateChatId = parts.slice(2).join('_');
      } else if (data.startsWith('private_unblock_')) {
        action = 'private_unblock';
        privateChatId = parts.slice(1).join('_');
      } else if (data === 'toggle_verification_private') {
        action = 'toggle_verification_private';
      } else if (data === 'noop') {
        action = 'noop';
      } else {
        action = data;
        privateChatId = '';
      }

      if (action === 'verify') {
        const [, userChatId, selectedAnswer, result] = data.split('_');
        if (userChatId !== chatId) {
          return;
        }

        let verificationState = userStateCache.get(chatId);
        if (verificationState === undefined) {
          verificationState = await env.D1.prepare('SELECT verification_code, code_expiry, is_verifying, verification_fail_count, verification_fail_block_until FROM user_states WHERE chat_id = ?')
            .bind(chatId)
            .first();
          if (!verificationState) {
            verificationState = { verification_code: null, code_expiry: null, is_verifying: false, verification_fail_count: 0, verification_fail_block_until: null };
          }
          userStateCache.set(chatId, verificationState);
        }

        const storedCode = verificationState.verification_code;
        const codeExpiry = verificationState.code_expiry;
        const nowSeconds = Math.floor(Date.now() / 1000);

        if (verificationState.verification_fail_block_until && nowSeconds < verificationState.verification_fail_block_until) {
          const remainingSec = verificationState.verification_fail_block_until - nowSeconds;
          const remainingFriendly = formatRemainingTime(remainingSec);
          const untilStr = formatShanghai(new Date(verificationState.verification_fail_block_until * 1000));
          await sendMessageToUser(chatId, `由于连续多次验证失败，您已被限制验证。${remainingFriendly}，解禁时间：${untilStr}`);
          await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/deleteMessage`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
              chat_id: chatId,
              message_id: messageId
            })
          }).catch(()=>{});
          return;
        }

        if (!storedCode || (codeExpiry && nowSeconds > codeExpiry)) {
          await sendMessageToUser(chatId, '验证码已过期，正在为您发送新的验证码...');
          await env.D1.prepare('UPDATE user_states SET verification_code = NULL, code_expiry = NULL, is_verifying = FALSE WHERE chat_id = ?')
            .bind(chatId)
            .run();
          userStateCache.set(chatId, { ...verificationState, verification_code: null, code_expiry: null, is_verifying: false });

          try {
            await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/deleteMessage`, {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({
                chat_id: chatId,
                message_id: messageId
              })
            });
          } catch (error) {
            logger('WARN', 'Failed to delete expired verification button', error.message);
          }

          try {
            await handleVerification(chatId, 0);
          } catch (verificationError) {
            logger('ERROR', 'Failed to send new verification', verificationError.message);
            setTimeout(async () => {
              try {
                await handleVerification(chatId, 0);
              } catch (retryError) {
                logger('ERROR', 'Retry sending verification failed', retryError.message);
                await sendMessageToUser(chatId, '发送验证码失败，请发送任意消息重试');
              }
            }, 1000);
          }
          return;
        }

        if (result === 'correct') {
          const verifiedExpiry = nowSeconds + 3600 * 24;
          await env.D1.prepare('UPDATE user_states SET is_verified = ?, verified_expiry = ?, verification_code = NULL, code_expiry = NULL, last_verification_message_id = NULL, is_first_verification = ?, is_verifying = ?, verification_fail_count = ?, verification_fail_block_until = NULL WHERE chat_id = ?')
            .bind(true, verifiedExpiry, false, false, 0, chatId)
            .run();
          const verificationStateNew = await env.D1.prepare('SELECT is_verified, verified_expiry, verification_code, code_expiry, last_verification_message_id, is_first_verification, is_verifying, verification_fail_count, verification_fail_block_until FROM user_states WHERE chat_id = ?')
            .bind(chatId)
            .first();
          userStateCache.set(chatId, verificationStateNew);

          let rateData = await env.D1.prepare('SELECT message_count, window_start FROM message_rates WHERE chat_id = ?')
            .bind(chatId)
            .first() || { message_count: 0, window_start: nowSeconds * 1000 };
          rateData.message_count = 0;
          rateData.window_start = nowSeconds * 1000;
          messageRateCache.set(chatId, rateData);
          await env.D1.prepare('UPDATE message_rates SET message_count = ?, window_start = ? WHERE chat_id = ?')
            .bind(0, nowSeconds * 1000, chatId)
            .run();

          const successMessage = await getVerificationSuccessMessage();
          await sendMessageToUser(chatId, `${successMessage}\n你可以用这个机器人跟我对话。写下您想要发送的消息（图片、视频），我会尽快回复您！`);
          const userInfo = await getUserInfo(chatId);
          await ensureUserTopic(chatId, userInfo);
        } else {
          // 验证失败处理：计数，达到 5 次则短时封禁 1 小时
          let failCount = verificationState.verification_fail_count || 0;
          failCount += 1;
          const nowSeconds2 = Math.floor(Date.now() / 1000);
          if (failCount >= 5) {
            const blockUntil = nowSeconds2 + 3600; // 1 hour
            await env.D1.prepare('UPDATE user_states SET verification_fail_count = ?, verification_fail_block_until = ? WHERE chat_id = ?')
              .bind(failCount, blockUntil, chatId)
              .run();
            userStateCache.set(chatId, { ...verificationState, verification_fail_count: failCount, verification_fail_block_until: blockUntil });
            const remainingSec = blockUntil - nowSeconds2;
            const remainingFriendly = formatRemainingTime(remainingSec);
            const untilStr = formatShanghai(new Date(blockUntil * 1000));
            await sendMessageToUser(chatId, `验证失败次数已达到 ${failCount} 次，您被限制再次验证。${remainingFriendly}，解禁时间：${untilStr}`);
            await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/deleteMessage`, {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({
                chat_id: chatId,
                message_id: messageId
              })
            }).catch(()=>{});
            logger('INFO', `User ${chatId} locked for failed verifications until ${blockUntil}`);
            return;
          } else {
            await env.D1.prepare('UPDATE user_states SET verification_fail_count = ? WHERE chat_id = ?')
              .bind(failCount, chatId)
              .run();
            userStateCache.set(chatId, { ...verificationState, verification_fail_count: failCount });
            await sendMessageToUser(chatId, '验证失败，请重新尝试。');
            await handleVerification(chatId, messageId);
          }
        }

        await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/deleteMessage`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            chat_id: chatId,
            message_id: messageId
          })
        });
      } else {
        // 非 verify 回调：管理员操作
        const senderId = callbackQuery.from.id.toString();
        const isAdmin = await checkIfAdmin(senderId);
        if (!isAdmin) {
          await sendMessageToTopic(topicId, '只有管理员可以使用此功能。');
          await sendAdminPanel(chatId, topicId, privateChatId, messageId);
          return;
        }

        if (action === 'block') {
          let state = userStateCache.get(privateChatId);
          if (state === undefined) {
            state = await env.D1.prepare('SELECT is_blocked FROM user_states WHERE chat_id = ?')
              .bind(privateChatId)
              .first() || { is_blocked: false };
          }
          state.is_blocked = true;
          userStateCache.set(privateChatId, state);
          await env.D1.prepare('INSERT OR REPLACE INTO user_states (chat_id, is_blocked) VALUES (?, ?)')
            .bind(privateChatId, true)
            .run();
          await sendMessageToTopic(topicId, `用户 ${privateChatId} 已被拉黑，消息将不再转发。`);
        } else if (action === 'unblock' || action === 'private_unblock') {
          let state = userStateCache.get(privateChatId);
          if (state === undefined) {
            state = await env.D1.prepare('SELECT is_blocked, is_first_verification FROM user_states WHERE chat_id = ?')
              .bind(privateChatId)
              .first() || { is_blocked: false, is_first_verification: true };
          }
          state.is_blocked = false;
          state.is_first_verification = true;
          await env.D1.prepare('INSERT OR REPLACE INTO user_states (chat_id, is_blocked, is_first_verification, verification_fail_count, verification_fail_block_until) VALUES (?, ?, ?, ?, ?)')
            .bind(privateChatId, false, true, 0, null)
            .run();
          userStateCache.set(privateChatId, { ...state, verification_fail_count: 0, verification_fail_block_until: null });
          if (action === 'private_unblock') {
            await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/sendMessage`, {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({
                chat_id: senderId,
                text: `用户 ${privateChatId} 已解除拉黑。`
              })
            });
          } else {
            await sendMessageToTopic(topicId, `用户 ${privateChatId} 已解除拉黑，消息将继续转发。`);
            await sendAdminPanel(chatId, topicId, privateChatId, messageId);
          }
        } else if (action === 'toggle_verification') {
          const currentState = (await getSetting('verification_enabled', env.D1)) === 'true';
          const newState = !currentState;
          await setSetting('verification_enabled', newState.toString());
          await sendMessageToTopic(topicId, `验证码功能已${newState ? '开启' : '关闭'}。`);
        } else if (action === 'toggle_verification_private') {
          const currentState = (await getSetting('verification_enabled', env.D1)) === 'true';
          const newState = !currentState;
          await setSetting('verification_enabled', newState.toString());
          await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/sendMessage`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
              chat_id: senderId,
              text: `验证码功能已${newState ? '开启' : '关闭'}。`
            })
          });
        } else if (action === 'check_blocklist') {
          const blockedUsers = await env.D1.prepare('SELECT chat_id FROM user_states WHERE is_blocked = ?')
            .bind(true)
            .all();
          const blockList = blockedUsers.results.length > 0
            ? blockedUsers.results.map(row => row.chat_id).join('\n')
            : '当前没有被拉黑的用户。';
          await sendMessageToTopic(topicId, `黑名单列表：\n${blockList}`);
        } else if (action === 'toggle_user_raw') {
          const currentState = (await getSetting('user_raw_enabled', env.D1)) === 'true';
          const newState = !currentState;
          await setSetting('user_raw_enabled', newState.toString());
          await sendMessageToTopic(topicId, `用户端 Raw 链接已${newState ? '开启' : '关闭'}。`);
        } else if (action === 'delete_user') {
          try {
            const userTopicId = await getExistingTopicId(privateChatId);
            if (userTopicId) {
              try {
                const delResp = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/deleteForumTopic`, {
                  method: 'POST',
                  headers: { 'Content-Type': 'application/json' },
                  body: JSON.stringify({
                    chat_id: GROUP_ID,
                    message_thread_id: Number(userTopicId)
                  })
                });
                const delData = await delResp.json().catch(() => ({ ok: false, description: 'invalid json' }));
                if (!delData.ok) {
                  logger('WARN', `Failed to delete topic ${userTopicId}`, delData.description || JSON.stringify(delData));
                } else {
                  logger('INFO', `Deleted topic ${userTopicId} for user ${privateChatId}`);
                }
              } catch (err) {
                logger('WARN', `deleteForumTopic error for ${userTopicId}`, err.message);
              }
            } else {
              logger('INFO', `No topic mapping found for ${privateChatId}`);
            }
          } catch (err) {
            logger('WARN', 'Error finding topicId to delete', err.message);
          }

          userStateCache.set(privateChatId, undefined);
          messageRateCache.set(privateChatId, undefined);
          topicIdCache.set(privateChatId, undefined);

          await env.D1.batch([
            env.D1.prepare('DELETE FROM user_states WHERE chat_id = ?').bind(privateChatId),
            env.D1.prepare('DELETE FROM message_rates WHERE chat_id = ?').bind(privateChatId),
            env.D1.prepare('DELETE FROM chat_topic_mappings WHERE chat_id = ?').bind(privateChatId)
          ]);

          await sendMessageToTopic(topicId, `用户 ${privateChatId} 的状态、消息记录和话题映射已删除，用户需重新发起会话。`);
        } else {
          await sendMessageToTopic(topicId, `未知操作：${action}`);
        }

        await sendAdminPanel(chatId, topicId, privateChatId, messageId);
      }

      await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/answerCallbackQuery`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          callback_query_id: callbackQuery.id
        })
      });
    }

    async function handleVerification(chatId, messageId) {
      try {
        let userState = userStateCache.get(chatId);
        if (userState === undefined) {
          userState = await env.D1.prepare('SELECT is_blocked, is_first_verification, is_verified, verified_expiry, is_verifying, verification_fail_block_until FROM user_states WHERE chat_id = ?')
            .bind(chatId)
            .first();
          if (!userState) {
            userState = { is_blocked: false, is_first_verification: true, is_verified: false, verified_expiry: null, is_verifying: false, verification_fail_block_until: null };
          }
          userStateCache.set(chatId, userState);
        }

        const nowSeconds = Math.floor(Date.now() / 1000);
        if (userState.verification_fail_block_until && nowSeconds < userState.verification_fail_block_until) {
          const remainingSec = userState.verification_fail_block_until - nowSeconds;
          const remainingFriendly = formatRemainingTime(remainingSec);
          const untilStr = formatShanghai(new Date(userState.verification_fail_block_until * 1000));
          await sendMessageToUser(chatId, `由于连续多次验证失败，您已被限制验证。${remainingFriendly}，解禁时间：${untilStr}`);
          return;
        }

        userState.verification_code = null;
        userState.code_expiry = null;
        userState.is_verifying = true;
        userStateCache.set(chatId, userState);
        await env.D1.prepare('UPDATE user_states SET verification_code = NULL, code_expiry = NULL, is_verifying = ? WHERE chat_id = ?')
          .bind(true, chatId)
          .run();

        const lastVerification = userState.last_verification_message_id || (await env.D1.prepare('SELECT last_verification_message_id FROM user_states WHERE chat_id = ?')
          .bind(chatId)
          .first())?.last_verification_message_id;

        if (lastVerification) {
          try {
            await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/deleteMessage`, {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({
                chat_id: chatId,
                message_id: lastVerification
              })
            });
          } catch (deleteError) {
            logger('WARN', 'Failed to delete previous verification message', deleteError.message);
          }

          userState.last_verification_message_id = null;
          userStateCache.set(chatId, userState);
          await env.D1.prepare('UPDATE user_states SET last_verification_message_id = NULL WHERE chat_id = ?')
            .bind(chatId)
            .run();
        }

        await sendVerification(chatId);
      } catch (error) {
        logger('ERROR', 'handleVerification failed', error.message);
        try {
          await env.D1.prepare('UPDATE user_states SET is_verifying = FALSE WHERE chat_id = ?')
            .bind(chatId)
            .run();
          let currentState = userStateCache.get(chatId);
          if (currentState) {
            currentState.is_verifying = false;
            userStateCache.set(chatId, currentState);
          }
        } catch (resetError) {
          logger('ERROR', 'Failed to reset user verifying flag', resetError.message);
        }
        throw error;
      }
    }

    async function sendVerification(chatId) {
      try {
        const st = await env.D1.prepare('SELECT verification_fail_block_until FROM user_states WHERE chat_id = ?').bind(chatId).first();
        const nowSec = Math.floor(Date.now() / 1000);
        if (st?.verification_fail_block_until && nowSec < st.verification_fail_block_until) {
          const remainingSec = st.verification_fail_block_until - nowSec;
          const remainingFriendly = formatRemainingTime(remainingSec);
          const untilStr = formatShanghai(new Date(st.verification_fail_block_until * 1000));
          await sendMessageToUser(chatId, `由于连续多次验证失败，您已被限制验证。${remainingFriendly}，解禁时间：${untilStr}`);
          return;
        }

        const num1 = Math.floor(Math.random() * 10);
        const num2 = Math.floor(Math.random() * 10);
        const operation = Math.random() > 0.5 ? '+' : '-';
        const correctResult = operation === '+' ? num1 + num2 : num1 - num2;

        const options = new Set([correctResult]);
        while (options.size < 4) {
          const wrongResult = correctResult + Math.floor(Math.random() * 5) - 2;
          if (wrongResult !== correctResult) options.add(wrongResult);
        }
        const optionArray = Array.from(options).sort(() => Math.random() - 0.5);

        const buttons = optionArray.map(option => ({
          text: `(${option})`,
          callback_data: `verify_${chatId}_${option}_${option === correctResult ? 'correct' : 'wrong'}`
        }));

        const question = `请计算：${num1} ${operation} ${num2} = ?（点击下方按钮完成验证）`;
        const nowSeconds = Math.floor(Date.now() / 1000);
        const codeExpiry = nowSeconds + 300;

        let userState = userStateCache.get(chatId);
        if (userState === undefined) {
          userState = { verification_code: correctResult.toString(), code_expiry: codeExpiry, last_verification_message_id: null, is_verifying: true };
        } else {
          userState.verification_code = correctResult.toString();
          userState.code_expiry = codeExpiry;
          userState.last_verification_message_id = null;
          userState.is_verifying = true;
        }
        userStateCache.set(chatId, userState);

        const response = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/sendMessage`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            chat_id: chatId,
            text: question,
            reply_markup: { inline_keyboard: [buttons] }
          })
        });
        const data = await response.json();
        if (data.ok) {
          userState.last_verification_message_id = data.result.message_id.toString();
          userStateCache.set(chatId, userState);
          await env.D1.prepare('UPDATE user_states SET verification_code = ?, code_expiry = ?, last_verification_message_id = ?, is_verifying = ? WHERE chat_id = ?')
            .bind(correctResult.toString(), codeExpiry, data.result.message_id.toString(), true, chatId)
            .run();
          logger('INFO', `Sent verification to ${chatId}`);
        } else {
          throw new Error(`Telegram API 返回错误: ${data.description || '未知错误'}`);
        }
      } catch (error) {
        logger('ERROR', 'sendVerification failed', error.message);
        throw error;
      }
    }

    async function checkIfAdmin(userId) {
      try {
        const response = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/getChatMember`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            chat_id: GROUP_ID,
            user_id: userId
          })
        });
        const data = await response.json();
        return data.ok && (data.result.status === 'administrator' || data.result.status === 'creator');
      } catch (err) {
        logger('WARN', 'checkIfAdmin request failed', err.message);
        return false;
      }
    }

    async function getUserInfo(chatId) {
      let userInfo = userInfoCache.get(chatId);
      if (userInfo !== undefined) {
        return userInfo;
      }

      const response = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/getChat`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ chat_id: chatId })
      });
      const data = await response.json();
      if (!data.ok) {
        userInfo = {
          id: chatId,
          username: `User_${chatId}`,
          nickname: `User_${chatId}`
        };
      } else {
        const result = data.result;
        const nickname = result.first_name
          ? `${result.first_name}${result.last_name ? ` ${result.last_name}` : ''}`.trim()
          : result.username || `User_${chatId}`;
        userInfo = {
          id: result.id || chatId,
          username: result.username || `User_${chatId}`,
          nickname: nickname
        };
      }

      userInfoCache.set(chatId, userInfo);
      return userInfo;
    }

    async function getExistingTopicId(chatId) {
      let topicId = topicIdCache.get(chatId);
      if (topicId !== undefined) {
        return topicId;
      }

      const result = await env.D1.prepare('SELECT topic_id FROM chat_topic_mappings WHERE chat_id = ?')
        .bind(chatId)
        .first();
      topicId = result?.topic_id || null;
      if (topicId) {
        topicIdCache.set(chatId, topicId);
      }
      return topicId;
    }

    async function createForumTopic(topicName, userName, nickname, userId) {
      const response = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/createForumTopic`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ chat_id: GROUP_ID, name: `${nickname}` })
      });
      const data = await response.json();
      if (!data.ok) throw new Error(`Failed to create forum topic: ${data.description}`);
      const topicId = data.result.message_thread_id;

      try {
        const formattedTime = formatShanghai();
        const notificationContent = await getNotificationContent();
        const pinnedMessage = `昵称: ${nickname}\n用户名: @${userName}\nUserID: ${userId}\n发起时间: ${formattedTime}\n\n${notificationContent}`;
        const messageResponse = await sendMessageToTopic(topicId, pinnedMessage);
        const messageId = messageResponse.result.message_id;
        await pinMessage(topicId, messageId);
      } catch (err) {
        logger('WARN', 'Failed to send/pin pinned message for new topic', err.message);
      }

      return topicId;
    }

    async function saveTopicId(chatId, topicId, createdAt = null, lastInteraction = null) {
      if (createdAt === null) createdAt = Date.now();
      if (lastInteraction === null) lastInteraction = createdAt;
      await env.D1.prepare('INSERT OR REPLACE INTO chat_topic_mappings (chat_id, topic_id, created_at, last_interaction) VALUES (?, ?, ?, ?)')
        .bind(chatId, topicId, createdAt, lastInteraction)
        .run();
      topicIdCache.set(chatId, topicId);
      logger('INFO', `Saved topic mapping ${chatId} -> ${topicId}`);
    }

    async function getPrivateChatId(topicId) {
      for (const [chatId, tid] of topicIdCache.cache) if (tid === topicId) return chatId;
      const mapping = await env.D1.prepare('SELECT chat_id FROM chat_topic_mappings WHERE topic_id = ?')
        .bind(topicId)
        .first();
      return mapping?.chat_id || null;
    }

    async function sendMessageToTopic(topicId, text) {
      if (!text || !text.trim()) {
        throw new Error('Message text is empty');
      }

      const requestBody = {
        chat_id: GROUP_ID,
        text: text,
        message_thread_id: topicId
      };
      const response = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/sendMessage`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(requestBody)
      });
      const data = await response.json();
      if (!data.ok) {
        throw new Error(`Failed to send message to topic ${topicId}: ${data.description}`);
      }

      try {
        const mapping = await env.D1.prepare('SELECT chat_id FROM chat_topic_mappings WHERE topic_id = ?').bind(topicId).first();
        if (mapping?.chat_id) {
          const now = Date.now();
          await env.D1.prepare('UPDATE chat_topic_mappings SET last_interaction = ? WHERE chat_id = ?').bind(now, mapping.chat_id).run();
          topicIdCache.set(mapping.chat_id, topicId);
        }
      } catch (e) {
        logger('WARN', 'Failed to update last_interaction for topic', e.message);
      }

      return data;
    }

    async function copyMessageToTopic(topicId, message) {
      const requestBody = {
        chat_id: GROUP_ID,
        from_chat_id: message.chat.id,
        message_id: message.message_id,
        message_thread_id: topicId,
        disable_notification: true
      };
      const response = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/copyMessage`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(requestBody)
      });
      const data = await response.json();
      if (!data.ok) {
        throw new Error(`Failed to copy message to topic ${topicId}: ${data.description}`);
      }

      try {
        const mapping = await env.D1.prepare('SELECT chat_id FROM chat_topic_mappings WHERE topic_id = ?').bind(topicId).first();
        if (mapping?.chat_id) {
          const now = Date.now();
          await env.D1.prepare('UPDATE chat_topic_mappings SET last_interaction = ? WHERE chat_id = ?').bind(now, mapping.chat_id).run();
        }
      } catch (e) {
        logger('WARN', 'Failed to update last_interaction after copyMessage', e.message);
      }
    }

    async function pinMessage(topicId, messageId) {
      const requestBody = {
        chat_id: GROUP_ID,
        message_id: messageId,
        message_thread_id: topicId
      };
      const response = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/pinChatMessage`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(requestBody)
      });
      const data = await response.json();
      if (!data.ok) {
        throw new Error(`Failed to pin message: ${data.description}`);
      }
    }

    async function forwardMessageToPrivateChat(privateChatId, message) {
      const requestBody = {
        chat_id: privateChatId,
        from_chat_id: message.chat.id,
        message_id: message.message_id,
        disable_notification: true
      };
      const response = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/copyMessage`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(requestBody)
      });
      const data = await response.json();
      if (!data.ok) {
        throw new Error(`Failed to forward message to private chat: ${data.description}`);
      }

      try {
        const now = Date.now();
        await env.D1.prepare('UPDATE chat_topic_mappings SET last_interaction = ? WHERE chat_id = ?').bind(now, privateChatId).run();
      } catch (e) {
        logger('WARN', 'Failed to update last_interaction when forwarding message to private chat', e.message);
      }
    }

    async function sendMessageToUser(chatId, text) {
      const requestBody = { chat_id: chatId, text: text };
      const response = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/sendMessage`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(requestBody)
      });
      const data = await response.json();
      if (!data.ok) {
        throw new Error(`Failed to send message to user: ${data.description}`);
      }
    }

    async function fetchWithRetry(url, options, retries = 3, backoff = 1000) {
      for (let i = 0; i < retries; i++) {
        try {
          const controller = new AbortController();
          const timeoutId = setTimeout(() => controller.abort(), 5000);
          const response = await fetch(url, { ...options, signal: controller.signal });
          clearTimeout(timeoutId);

          if (response.ok) {
            return response;
          }
          if (response.status === 429) {
            const retryAfter = response.headers.get('Retry-After') || 5;
            const delay = parseInt(retryAfter) * 1000;
            await new Promise(resolve => setTimeout(resolve, delay));
            continue;
          }
          throw new Error(`Request failed with status ${response.status}: ${await response.text()}`);
        } catch (error) {
          if (i === retries - 1) throw error;
          await new Promise(resolve => setTimeout(resolve, backoff * Math.pow(2, i)));
        }
      }
      throw new Error(`Failed to fetch ${url} after ${retries} retries`);
    }

    async function registerWebhook(request) {
      const webhookUrl = `${new URL(request.url).origin}/webhook`;
      const response = await fetch(`https://api.telegram.org/bot${BOT_TOKEN}/setWebhook`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ url: webhookUrl })
      }).then(r => r.json());
      return new Response(response.ok ? 'Webhook set successfully' : JSON.stringify(response, null, 2));
    }

    async function unRegisterWebhook() {
      const response = await fetch(`https://api.telegram.org/bot${BOT_TOKEN}/setWebhook`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ url: '' })
      }).then(r => r.json());
      return new Response(response.ok ? 'Webhook removed' : JSON.stringify(response, null, 2));
    }

    try {
      return await handleRequest(request);
    } catch (error) {
      logger('ERROR', 'Unhandled error in fetch', error.message);
      return new Response('Internal Server Error', { status: 500 });
    }
  }
};
