// ============================================================
// Telegram 双向机器人 - 环境变量版（阈值、封禁时长从环境变量读取）
// 包含：验证失败封禁、全局管理员面板（解封）、日志
// ============================================================

let BOT_TOKEN;
let GROUP_ID;
let MAX_MESSAGES_PER_MINUTE;
let GENERAL_TOPIC_ID;

let lastCleanupTime = 0;
const CLEANUP_INTERVAL = 24 * 60 * 60 * 1000; // 24 小时
let isInitialized = false;
const processedMessages = new Set();
const processedCallbacks = new Set();

const topicCreationLocks = new Map();
const adminPanelMessages = new Map();

// 缓存设置（仅用于 verification_enabled 和 user_raw_enabled，阈值和封禁时长直接从 env 读取）
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

// 将UTC时间转换为北京时间（UTC+8）
function formatBeijingTime(utcDate) {
  const beijingTime = new Date(utcDate.getTime() + 8 * 60 * 60 * 1000);
  const year = beijingTime.getUTCFullYear();
  const month = String(beijingTime.getUTCMonth() + 1).padStart(2, '0');
  const day = String(beijingTime.getUTCDate()).padStart(2, '0');
  const hours = String(beijingTime.getUTCHours()).padStart(2, '0');
  const minutes = String(beijingTime.getUTCMinutes()).padStart(2, '0');
  const seconds = String(beijingTime.getUTCSeconds()).padStart(2, '0');
  return `${year}-${month}-${day} ${hours}:${minutes}:${seconds}`;
}

// 格式化剩余时间
function formatRemaining(seconds) {
  if (seconds < 0) seconds = 0;
  const h = Math.floor(seconds / 3600);
  const m = Math.floor((seconds % 3600) / 60);
  const s = seconds % 60;
  let parts = [];
  if (h > 0) parts.push(`${h}小时`);
  if (m > 0) parts.push(`${m}分钟`);
  if (s > 0 || parts.length === 0) parts.push(`${s}秒`);
  return parts.join('');
}

export default {
  async fetch(request, env) {
    BOT_TOKEN = env.BOT_TOKEN_ENV || null;
    GROUP_ID = env.GROUP_ID_ENV || null;
    MAX_MESSAGES_PER_MINUTE = env.MAX_MESSAGES_PER_MINUTE_ENV ? parseInt(env.MAX_MESSAGES_PER_MINUTE_ENV) : 40;
    GENERAL_TOPIC_ID = env.GENERAL_TOPIC_ID ? parseInt(env.GENERAL_TOPIC_ID) : 1;

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
          return new Response('Bad Request', { status: 400 });
        }
      } else if (url.pathname === '/registerWebhook') {
        return await registerWebhook(request);
      } else if (url.pathname === '/unRegisterWebhook') {
        return await unRegisterWebhook();
      } else if (url.pathname === '/checkTables') {
        await checkAndRepairTables(env.D1);
        return new Response('Database tables checked and repaired', { status: 200 });
      }
      return new Response('Not Found', { status: 404 });
    }

    async function initialize(d1, request) {
      await Promise.all([
        checkAndRepairTables(d1),
        autoRegisterWebhook(request),
        checkBotPermissions(),
        cleanExpiredVerificationCodes(d1)
      ]);
    }

    async function autoRegisterWebhook(request) {
      const webhookUrl = `${new URL(request.url).origin}/webhook`;
      await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/setWebhook`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ url: webhookUrl }),
      });
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
            failed_attempts: 'INTEGER DEFAULT 0',
            ban_expiry: 'INTEGER',
            last_verification_at: 'INTEGER'
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
            topic_id: 'TEXT NOT NULL'
          }
        },
        settings: {
          columns: {
            key: 'TEXT PRIMARY KEY',
            value: 'TEXT'
          }
        },
        admin_logs: {
          columns: {
            id: 'INTEGER PRIMARY KEY AUTOINCREMENT',
            admin_id: 'TEXT',
            action: 'TEXT',
            target_id: 'TEXT',
            details: 'TEXT',
            created_at: 'INTEGER'
          }
        }
      };

      for (const [tableName, structure] of Object.entries(expectedTables)) {
        const tableInfo = await d1.prepare(
          `SELECT sql FROM sqlite_master WHERE type='table' AND name=?`
        ).bind(tableName).first();

        if (!tableInfo) {
          await createTable(d1, tableName, structure);
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
            const columnParts = colDef.split(' ');
            const addColumnSQL = `ALTER TABLE ${tableName} ADD COLUMN ${colName} ${columnParts.slice(1).join(' ')}`;
            await d1.exec(addColumnSQL);
          }
        }

        if (tableName === 'settings') {
          await d1.exec('CREATE INDEX IF NOT EXISTS idx_settings_key ON settings (key)');
        }
        if (tableName === 'admin_logs') {
          await d1.exec('CREATE INDEX IF NOT EXISTS idx_admin_logs_created_at ON admin_logs (created_at)');
        }
      }

      // 插入默认设置（只保留可动态开关的两个）
      await Promise.all([
        d1.prepare('INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)').bind('verification_enabled', 'true').run(),
        d1.prepare('INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)').bind('user_raw_enabled', 'true').run()
      ]);

      // 刷新缓存
      settingsCache.set('verification_enabled', (await getSetting('verification_enabled', d1)) === 'true');
      settingsCache.set('user_raw_enabled', (await getSetting('user_raw_enabled', d1)) === 'true');
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
      }
      lastCleanupTime = now;
    }

    async function handleUpdate(update) {
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

      // 处理 /admin 命令
      if (chatId === GROUP_ID && text.trim() === '/admin') {
        const topicId = message.message_thread_id;
        if (!topicId || topicId === GENERAL_TOPIC_ID) {
          await handleAdminCommand(message);
          return;
        }
      }

      if (chatId === GROUP_ID) {
        const topicId = message.message_thread_id;
        if (topicId) {
          const privateChatId = await getPrivateChatId(topicId);
          if (privateChatId && text.startsWith('/reset_user')) {
            await handleResetUser(chatId, topicId, text);
            return;
          }
          if (privateChatId) {
            await forwardMessageToPrivateChat(privateChatId, message);
          }
        }
        return;
      }

      // 检查是否是管理员私聊
      const isAdmin = await checkIfAdmin(message.from.id);
      if (isAdmin) {
        if (text === '/start') {
          await sendMessageToUser(chatId, "尊敬的管理员，您好！您无需验证可以直接与机器人对话。");
        } else {
          await sendMessageToUser(chatId, "尊敬的管理员，您好！您无需验证可以直接与机器人对话。");
        }
        return;
      }

      let userState = userStateCache.get(chatId);
      if (userState === undefined) {
        userState = await env.D1.prepare('SELECT is_blocked, is_first_verification, is_verified, verified_expiry, is_verifying, failed_attempts, ban_expiry FROM user_states WHERE chat_id = ?')
          .bind(chatId)
          .first();
        if (!userState) {
          userState = { is_blocked: false, is_first_verification: true, is_verified: false, verified_expiry: null, is_verifying: false, failed_attempts: 0, ban_expiry: null };
          await env.D1.prepare('INSERT INTO user_states (chat_id, is_blocked, is_first_verification, is_verified, is_verifying, failed_attempts, ban_expiry) VALUES (?, ?, ?, ?, ?, ?, ?)')
            .bind(chatId, false, true, false, false, 0, null)
            .run();
        }
        userStateCache.set(chatId, userState);
      }

      // 检查是否被封禁（拉黑或验证失败封禁）
      const nowSeconds = Math.floor(Date.now() / 1000);
      
      // 强制从数据库获取最新封禁状态，避免缓存问题
      const dbBan = await env.D1.prepare('SELECT ban_expiry FROM user_states WHERE chat_id = ?').bind(chatId).first();
      if (dbBan && dbBan.ban_expiry) {
        userState.ban_expiry = dbBan.ban_expiry;
        userStateCache.set(chatId, userState); // 更新缓存
      }

      if (userState.is_blocked) {
        await sendMessageToUser(chatId, "您已被拉黑，无法发送消息。");
        return;
      }
      if (userState.ban_expiry && userState.ban_expiry > nowSeconds) {
        const remaining = userState.ban_expiry - nowSeconds;
        const banExpiryDate = new Date(userState.ban_expiry * 1000);
        const banExpiryStr = formatBeijingTime(banExpiryDate);
        const remainingStr = formatRemaining(remaining);
        await sendMessageToUser(chatId, `由于连续多次验证失败，您已被限制验证。还剩${remainingStr} 解封时间 ${banExpiryStr} 后再试`);
        return;
      }

      const verificationEnabled = (await getSetting('verification_enabled', env.D1)) === 'true';

      if (!verificationEnabled) {
        // 验证码关闭，直接放行
      } else {
        const isVerified = userState.is_verified && userState.verified_expiry && nowSeconds < userState.verified_expiry;
        const isFirstVerification = userState.is_first_verification;
        const isRateLimited = await checkMessageRate(chatId);
        const isVerifying = userState.is_verifying || false;

        if (!isVerified || (isRateLimited && !isFirstVerification)) {
         const verificationEnabled = (await getSetting('verification_enabled', env.D1)) === 'true';

      if (!verificationEnabled) {
        // 验证码关闭，直接放行（后面继续处理消息）
      } else {
        const isVerified = userState.is_verified && userState.verified_expiry && nowSeconds < userState.verified_expiry;
        const isFirstVerification = userState.is_first_verification;
        const isRateLimited = await checkMessageRate(chatId);
        const isVerifying = userState.is_verifying || false;

        if (!isVerified || (isRateLimited && !isFirstVerification)) {
          if (isVerifying) {
            // 用户正在验证中，检查验证码是否过期
            const storedCode = await env.D1.prepare('SELECT verification_code, code_expiry FROM user_states WHERE chat_id = ?')
              .bind(chatId)
              .first();
            const nowSeconds = Math.floor(Date.now() / 1000);
            const isCodeExpired = !storedCode?.verification_code || !storedCode?.code_expiry || nowSeconds > storedCode.code_expiry;

            if (isCodeExpired) {
              // 验证码过期，发送新验证码
              await sendMessageToUser(chatId, '验证码已过期，正在为您发送新的验证码...');
              await env.D1.prepare('UPDATE user_states SET verification_code = NULL, code_expiry = NULL, is_verifying = FALSE WHERE chat_id = ?')
                .bind(chatId)
                .run();
              userStateCache.set(chatId, { ...userState, verification_code: null, code_expiry: null, is_verifying: false });

              // 删除旧的验证消息（如果存在）
              try {
                const lastVerification = await env.D1.prepare('SELECT last_verification_message_id FROM user_states WHERE chat_id = ?')
                  .bind(chatId)
                  .first();
                if (lastVerification?.last_verification_message_id) {
                  await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/deleteMessage`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                      chat_id: chatId,
                      message_id: lastVerification.last_verification_message_id
                    })
                  }).catch(() => {});
                  await env.D1.prepare('UPDATE user_states SET last_verification_message_id = NULL WHERE chat_id = ?')
                    .bind(chatId)
                    .run();
                }
              } catch (error) {}

              // 发送新验证码，并处理可能的错误
              try {
                await handleVerification(chatId, 0);
              } catch (verificationError) {
                console.error(`发送新验证码失败: ${verificationError.message}`);
                await sendMessageToUser(chatId, '发送验证码失败，请稍后重试或联系管理员。');
                // 重置状态，允许用户重新触发验证
                await env.D1.prepare('UPDATE user_states SET is_verifying = FALSE WHERE chat_id = ?').bind(chatId).run();
                userStateCache.delete(chatId);
              }
              return;
            } else {
              // 验证码未过期，提醒用户完成验证
              await sendMessageToUser(chatId, `请完成验证后发送消息"${text || '您的具体信息'}"。`);
            }
            return;
          }

          // 用户尚未验证，发送验证码
          await sendMessageToUser(chatId, `请完成验证后发送消息"${text || '您的具体信息'}"。`);
          try {
            await handleVerification(chatId, messageId);
          } catch (verificationError) {
            console.error(`发送验证码失败: ${verificationError.message}`);
            await sendMessageToUser(chatId, '发送验证码失败，请稍后重试或联系管理员。');
            // 重置状态，允许用户重新触发验证
            await env.D1.prepare('UPDATE user_states SET is_verifying = FALSE WHERE chat_id = ?').bind(chatId).run();
            userStateCache.delete(chatId);
          }
          return;
        }
      }
          await sendMessageToUser(chatId, `请完成验证后发送消息"${text || '您的具体信息'}"。`);
          await handleVerification(chatId, messageId);
          return;
        }
      }

      if (text === '/start') {
        if (await checkStartCommandRate(chatId)) {
          await sendMessageToUser(chatId, "您发送 /start 命令过于频繁，请稍后再试！");
          return;
        }

        const successMessage = await getVerificationSuccessMessage();
        await sendMessageToUser(chatId, `${successMessage}\n你可以用这个机器人跟我对话。写下您想要发送的消息（图片、视频），我会尽快回复您！`);
        const userInfo = await getUserInfo(chatId);
        await ensureUserTopic(chatId, userInfo);
        return;
      }

      const userInfo = await getUserInfo(chatId);
      if (!userInfo) {
        await sendMessageToUser(chatId, "无法获取用户信息，请稍后再试或联系管理员。");
        return;
      }

      let topicId = await ensureUserTopic(chatId, userInfo);
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

      const userName = userInfo.username || `User_${chatId}`;
      const nickname = userInfo.nickname || userName;

      if (text) {
        const formattedMessage = `${nickname}:\n${text}`;
        await sendMessageToTopic(topicId, formattedMessage);
      } else {
        await copyMessageToTopic(topicId, message);
      }
    }

    async function handleAdminCommand(message) {
      const chatId = message.chat.id.toString();
      const topicId = message.message_thread_id;
      const senderId = message.from.id;
      const messageId = message.message_id;
      
      const isAdmin = await checkIfAdmin(senderId);
      if (!isAdmin) {
        await sendMessageToTopic(topicId, '只有管理员可以使用此命令。');
        return;
      }

      try {
        const success = await sendGlobalAdminPanel(chatId, topicId, 0, senderId);
        if (success) {
          await deleteMessage(chatId, messageId);
        }
      } catch (error) {
        console.error(`处理管理员命令失败: ${error.message}`);
      }
    }

    // 获取失败阈值（从环境变量读取，默认5）
    function getFailThreshold() {
      const val = env.VERIFICATION_FAIL_THRESHOLD;
      if (val !== undefined && val !== null) {
        const num = parseInt(val);
        if (!isNaN(num) && num > 0) return num;
      }
      return 5;
    }

    // 获取封禁时长（秒）（从环境变量读取，默认3600）
    function getBanSeconds() {
      const val = env.VERIFICATION_BAN_SECONDS;
      if (val !== undefined && val !== null) {
        const num = parseInt(val);
        if (!isNaN(num) && num > 0) return num;
      }
      return 3600;
    }

    // 全局管理员面板（简化版，仅显示和解封）
    async function sendGlobalAdminPanel(chatId, topicId, page = 0, adminId = null, confirmData = null) {
      try {
        const verificationEnabled = (await getSetting('verification_enabled', env.D1)) === 'true';
        const failThreshold = getFailThreshold();
        const banSeconds = getBanSeconds();

        const nowSeconds = Math.floor(Date.now() / 1000);
        const blockedUsers = await getBlockedUsersDetailed(page, 10, nowSeconds);
        const totalBlocked = await getTotalBlockedUsers(nowSeconds);
        const totalPages = Math.ceil(totalBlocked / 10) || 1;

        let text = `🔧 *全局管理员面板*\n\n`;
        text += `✅ *验证码状态*: ${verificationEnabled ? '开启' : '关闭'}\n`;
        text += `⚙️ *失败阈值*: ${failThreshold} 次\n`;
        text += `⏳ *封禁时长*: ${banSeconds} 秒\n`;
        text += `🚫 *封禁用户数*: ${totalBlocked}\n\n`;

        if (blockedUsers.length === 0) {
          text += `📝 当前没有被封禁的用户。`;
        } else {
          text += `*被封禁用户列表 (${page + 1}/${totalPages})*:\n`;
          for (let i = 0; i < blockedUsers.length; i++) {
            const user = blockedUsers[i];
            const idx = i + 1 + page * 10;
            const userInfo = user.user_info || { nickname: '未知', username: '未知' };
            const lastVerif = user.last_verification_at ? formatBeijingTime(new Date(user.last_verification_at * 1000)) : '从未';
            const status = user.is_blocked ? '🚫 拉黑' : (user.ban_expiry ? `⏳ 封禁至 ${formatBeijingTime(new Date(user.ban_expiry * 1000))}` : '');
            text += `${idx}. [${userInfo.nickname}](tg://user?id=${user.chat_id}) \`${user.chat_id}\`\n`;
            text += `   失败: ${user.failed_attempts} 次 | 上次验证: ${lastVerif}\n`;
            text += `   状态: ${status}\n`;
          }
        }

        const buttons = [];

        if (confirmData) {
          const [action, targetId] = confirmData.split(':');
          if (action === 'confirm_unban') {
            buttons.push([
              { text: `✅ 确认解封`, callback_data: `global_confirm_unban_${targetId}_${page}` },
              { text: '❌ 取消', callback_data: `global_admin_${page}` }
            ]);
          }
        } else {
          blockedUsers.forEach(user => {
            buttons.push([{
              text: `🔓 解封 ${user.chat_id}`,
              callback_data: `global_unban_confirm_${user.chat_id}_${page}`
            }]);
          });

          if (totalPages > 1) {
            const navButtons = [];
            if (page > 0) {
              navButtons.push({ text: '⬅️ 上一页', callback_data: `global_admin_${page - 1}` });
            }
            if (page < totalPages - 1) {
              navButtons.push({ text: '下一页 ➡️', callback_data: `global_admin_${page + 1}` });
            }
            if (navButtons.length > 0) {
              buttons.push(navButtons);
            }
          }

          // 已移除“解封全部”按钮

          buttons.push([
            { text: verificationEnabled ? '🔴 关闭验证' : '🟢 开启验证', callback_data: `global_toggle_verification_${page}` },
            { text: '🔄 刷新', callback_data: `global_admin_${page}` },
            { text: '❌ 关闭面板', callback_data: `global_close_panel` }
          ]);
        }

        const replyMarkup = { inline_keyboard: buttons };

        const panelKey = `${chatId}:${topicId || 'default'}`;
        const oldPanelMessageId = adminPanelMessages.get(panelKey);
        if (oldPanelMessageId) {
          try {
            await deleteMessage(chatId, oldPanelMessageId);
          } catch (deleteError) {}
        }

        const messageBody = {
          chat_id: chatId,
          text: text,
          parse_mode: 'Markdown',
          reply_markup: replyMarkup,
          disable_web_page_preview: true
        };
        if (topicId) {
          messageBody.message_thread_id = topicId;
        }
        
        const response = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/sendMessage`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(messageBody)
        });
        
        const data = await response.json();
        if (data.ok) {
          adminPanelMessages.set(panelKey, data.result.message_id);
          return true;
        } else {
          console.error(`发送管理员面板失败: ${JSON.stringify(data)}`);
          try {
            await sendMessageToTopic(topicId, '发送管理员面板失败，请稍后重试。');
          } catch (e) {}
          return false;
        }
      } catch (error) {
        console.error(`发送全局管理员面板时出错: ${error.message}`);
        try {
          await sendMessageToTopic(topicId, '发送管理员面板时出现错误，请稍后重试。');
        } catch (e) {}
        return false;
      }
    }

    async function getBlockedUsersDetailed(page = 0, limit = 10, nowSeconds) {
      const offset = page * limit;
      const result = await env.D1.prepare(`
        SELECT chat_id, is_blocked, failed_attempts, ban_expiry, last_verification_at
        FROM user_states
        WHERE is_blocked = ? OR (ban_expiry IS NOT NULL AND ban_expiry > ?)
        ORDER BY chat_id
        LIMIT ? OFFSET ?
      `).bind(true, nowSeconds, limit, offset).all();

      const users = [];
      for (const row of result.results) {
        const userInfo = await getUserInfo(row.chat_id);
        users.push({ ...row, user_info: userInfo });
      }
      return users;
    }

    async function getTotalBlockedUsers(nowSeconds) {
      const result = await env.D1.prepare(`
        SELECT COUNT(*) as count
        FROM user_states
        WHERE is_blocked = ? OR (ban_expiry IS NOT NULL AND ban_expiry > ?)
      `).bind(true, nowSeconds).first();
      return result.count;
    }

    async function handleGlobalAdminCallback(callbackQuery) {
      const data = callbackQuery.data;
      const chatId = callbackQuery.message.chat.id.toString();
      const topicId = callbackQuery.message.message_thread_id;
      const messageId = callbackQuery.message.message_id;
      const adminId = callbackQuery.from.id.toString();

      const isAdmin = await checkIfAdmin(adminId);
      if (!isAdmin) {
        await sendMessageToTopic(topicId, '只有管理员可以使用此功能。');
        return;
      }

      try {
        if (data.startsWith('global_admin_')) {
          const page = parseInt(data.split('_')[2]);
          await sendGlobalAdminPanel(chatId, topicId, page, adminId);
        } else if (data.startsWith('global_unban_confirm_')) {
          const parts = data.split('_');
          const userChatId = parts[3];
          const page = parseInt(parts[4]);
          await sendGlobalAdminPanel(chatId, topicId, page, adminId, `confirm_unban:${userChatId}`);
        } else if (data.startsWith('global_confirm_unban_')) {
          const parts = data.split('_');
          const userChatId = parts[3];
          const page = parseInt(parts[4]);
          await env.D1.prepare(`
            UPDATE user_states 
            SET is_blocked = ?, failed_attempts = ?, ban_expiry = ? 
            WHERE chat_id = ?
          `).bind(false, 0, null, userChatId).run();
          let state = userStateCache.get(userChatId);
          if (state) {
            state.is_blocked = false;
            state.failed_attempts = 0;
            state.ban_expiry = null;
            userStateCache.set(userChatId, state);
          }
          await logAdminAction(adminId, 'unban_user', userChatId, `管理员解封用户`);
          const nowSeconds = Math.floor(Date.now() / 1000);
          const totalBlocked = await getTotalBlockedUsers(nowSeconds);
          if (totalBlocked === 0) {
            await deleteMessage(chatId, messageId);
            adminPanelMessages.delete(`${chatId}:${topicId || 'default'}`);
            await sendMessageToTopic(topicId, '✅ 所有用户已解封，管理员面板已关闭。');
          } else {
            await sendGlobalAdminPanel(chatId, topicId, page, adminId);
          }
        // 已移除处理 global_unban_all_confirm_ 和 global_confirm_unban_all_ 的分支
        } else if (data.startsWith('global_toggle_verification_')) {
          const page = parseInt(data.split('_')[3]);
          const currentState = (await getSetting('verification_enabled', env.D1)) === 'true';
          const newState = !currentState;
          await setSetting('verification_enabled', newState.toString());
          await logAdminAction(adminId, 'toggle_verification', '', `验证码状态: ${currentState} -> ${newState}`);
          await sendGlobalAdminPanel(chatId, topicId, page, adminId);
        } else if (data === 'global_close_panel') {
          await deleteMessage(chatId, messageId);
          adminPanelMessages.delete(`${chatId}:${topicId || 'default'}`);
          return;
        }
      } catch (error) {
        console.error('全局回调处理错误:', error);
        await sendMessageToTopic(topicId, `❌ 操作失败: ${error.message}`);
      }

      await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/answerCallbackQuery`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ callback_query_id: callbackQuery.id })
      });

      cleanupAdminPanelMessages();
    }

    async function logAdminAction(adminId, action, targetId, details) {
      const now = Math.floor(Date.now() / 1000);
      await env.D1.prepare(`
        INSERT INTO admin_logs (admin_id, action, target_id, details, created_at)
        VALUES (?, ?, ?, ?, ?)
      `).bind(adminId, action, targetId || null, details, now).run();
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

      if (data.startsWith('global_')) {
        await handleGlobalAdminCallback(callbackQuery);
        return;
      }

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
      } else if (data.startsWith('clear_ban_')) {
        action = 'clear_ban';
        privateChatId = parts.slice(2).join('_');
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
          verificationState = await env.D1.prepare('SELECT verification_code, code_expiry, is_verifying, failed_attempts, ban_expiry FROM user_states WHERE chat_id = ?')
            .bind(chatId)
            .first();
          if (!verificationState) {
            verificationState = { verification_code: null, code_expiry: null, is_verifying: false, failed_attempts: 0, ban_expiry: null };
          }
          userStateCache.set(chatId, verificationState);
        }

        const storedCode = verificationState.verification_code;
        const codeExpiry = verificationState.code_expiry;
        const nowSeconds = Math.floor(Date.now() / 1000);

        // 检查是否已被封禁（验证失败封禁）
        if (verificationState.ban_expiry && verificationState.ban_expiry > nowSeconds) {
          const remaining = verificationState.ban_expiry - nowSeconds;
          const banExpiryDate = new Date(verificationState.ban_expiry * 1000);
          const banExpiryStr = formatBeijingTime(banExpiryDate);
          const remainingStr = formatRemaining(remaining);
          await sendMessageToUser(chatId, `由于连续多次验证失败，您已被限制验证。还剩${remainingStr} 解封时间 ${banExpiryStr} 后再试`);
          // 删除当前验证消息
          await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/deleteMessage`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ chat_id: chatId, message_id: messageId })
          });
          return;
        }

        // 验证码过期处理
        if (!storedCode || (codeExpiry && nowSeconds > codeExpiry)) {
          await sendMessageToUser(chatId, '验证码已过期，正在为您发送新的验证码...');
          await env.D1.prepare('UPDATE user_states SET verification_code = NULL, code_expiry = NULL, is_verifying = FALSE WHERE chat_id = ?')
            .bind(chatId)
            .run();
          userStateCache.set(chatId, { ...verificationState, verification_code: null, code_expiry: null, is_verifying: false });
          
          // 删除当前验证消息
          try {
            await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/deleteMessage`, {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({ chat_id: chatId, message_id: messageId })
            });
          } catch (error) {}
          
          // 立即发送新验证码
          try {
            await handleVerification(chatId, 0);
          } catch (verificationError) {
            console.error(`发送新验证码失败: ${verificationError.message}`);
            setTimeout(async () => {
              try {
                await handleVerification(chatId, 0);
              } catch (retryError) {
                console.error(`重试发送验证码仍失败: ${retryError.message}`);
                await sendMessageToUser(chatId, '发送验证码失败，请发送任意消息重试');
              }
            }, 1000);
          }
          return;
        }

        if (result === 'correct') {
          // 验证成功
          const verifiedExpiry = nowSeconds + 3600 * 24;
          await env.D1.prepare(`
            UPDATE user_states 
            SET is_verified = ?, verified_expiry = ?, verification_code = NULL, code_expiry = NULL, 
                last_verification_message_id = NULL, is_first_verification = ?, is_verifying = ?,
                failed_attempts = ?, ban_expiry = ?
            WHERE chat_id = ?
          `).bind(true, verifiedExpiry, false, false, 0, null, chatId).run();
          verificationState = await env.D1.prepare('SELECT * FROM user_states WHERE chat_id = ?').bind(chatId).first();
          userStateCache.set(chatId, verificationState);

          // 重置消息频率计数
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
          
          // 删除验证消息
          await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/deleteMessage`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ chat_id: chatId, message_id: messageId })
          });
        } else {
          // 验证失败，增加失败计数
          const failThreshold = getFailThreshold(); // 从环境变量读取
          const banSeconds = getBanSeconds();       // 从环境变量读取
          const newAttempts = (verificationState.failed_attempts || 0) + 1;
          
          if (newAttempts >= failThreshold) {
            // 达到阈值，封禁用户
            const banExpiry = nowSeconds + banSeconds;
            await env.D1.prepare(`
              UPDATE user_states 
              SET failed_attempts = ?, ban_expiry = ?, is_verifying = FALSE
              WHERE chat_id = ?
            `).bind(newAttempts, banExpiry, chatId).run();
            
            const banExpiryDate = new Date(banExpiry * 1000);
            const banExpiryStr = formatBeijingTime(banExpiryDate);
            const remainingStr = formatRemaining(banSeconds);
            await sendMessageToUser(chatId, `验证失败次数已达到 ${failThreshold} 次，您被限制再次验证。还剩${remainingStr} 解封时间 ${banExpiryStr} 后再试`);
            
            // 转发通知到 General 话题
            const generalTopicId = GENERAL_TOPIC_ID;
            const userInfo = await getUserInfo(chatId);
            const notifyMsg = `⚠️ 用户 ${userInfo.nickname} (${chatId}) 因连续 ${failThreshold} 次验证失败，已被封禁至 ${banExpiryStr}`;
            await sendMessageToTopic(generalTopicId, notifyMsg);
            
            await logAdminAction('system', 'auto_ban', chatId, `失败次数达到阈值 ${failThreshold}`);
            
            // 更新缓存
            verificationState.failed_attempts = newAttempts;
            verificationState.ban_expiry = banExpiry;
            verificationState.is_verifying = false;
            userStateCache.set(chatId, verificationState);
            
            // 删除当前验证消息
            await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/deleteMessage`, {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({ chat_id: chatId, message_id: messageId })
            });
          } else {
            // 未达到阈值，仅增加失败次数，并发送新验证码
            await env.D1.prepare(`
              UPDATE user_states 
              SET failed_attempts = ?, is_verifying = FALSE
              WHERE chat_id = ?
            `).bind(newAttempts, chatId).run();
            
            // 更新缓存
            verificationState.failed_attempts = newAttempts;
            verificationState.is_verifying = false;
            userStateCache.set(chatId, verificationState);
            
            // 发送剩余次数提示
            await sendMessageToUser(chatId, `验证失败，剩余尝试次数: ${failThreshold - newAttempts}`);
            
            // 删除当前验证消息
            await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/deleteMessage`, {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({ chat_id: chatId, message_id: messageId })
            });
            
            // 立即发送新的验证码
            try {
              await handleVerification(chatId, 0);
            } catch (verificationError) {
              console.error(`发送新验证码失败: ${verificationError.message}`);
              setTimeout(async () => {
                try {
                  await handleVerification(chatId, 0);
                } catch (retryError) {
                  console.error(`重试发送验证码仍失败: ${retryError.message}`);
                  await sendMessageToUser(chatId, '发送验证码失败，请发送任意消息重试');
                }
              }, 1000);
            }
          }
        }

        if (result === 'correct') {
          const verifiedExpiry = nowSeconds + 3600 * 24;
          await env.D1.prepare(`
            UPDATE user_states 
            SET is_verified = ?, verified_expiry = ?, verification_code = NULL, code_expiry = NULL, 
                last_verification_message_id = NULL, is_first_verification = ?, is_verifying = ?,
                failed_attempts = ?, ban_expiry = ?
            WHERE chat_id = ?
          `).bind(true, verifiedExpiry, false, false, 0, null, chatId).run();
          verificationState = await env.D1.prepare('SELECT * FROM user_states WHERE chat_id = ?').bind(chatId).first();
          userStateCache.set(chatId, verificationState);

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
          const failThreshold = getFailThreshold();
          const banSeconds = getBanSeconds();
          const newAttempts = (verificationState.failed_attempts || 0) + 1;
          
          if (newAttempts >= failThreshold) {
            const banExpiry = nowSeconds + banSeconds;
            await env.D1.prepare(`
              UPDATE user_states 
              SET failed_attempts = ?, ban_expiry = ?, is_verifying = FALSE
              WHERE chat_id = ?
            `).bind(newAttempts, banExpiry, chatId).run();
            
            const banExpiryDate = new Date(banExpiry * 1000);
            const banExpiryStr = formatBeijingTime(banExpiryDate);
            const remainingStr = formatRemaining(banSeconds);
            await sendMessageToUser(chatId, `验证失败次数已达到 ${failThreshold} 次，您被限制再次验证。还剩${remainingStr} 解封时间 ${banExpiryStr} 后再试`);
            
            // 转发通知到 General 话题
            const generalTopicId = GENERAL_TOPIC_ID;
            const userInfo = await getUserInfo(chatId);
            const notifyMsg = `⚠️ 用户 ${userInfo.nickname} (${chatId}) 因连续 ${failThreshold} 次验证失败，已被封禁至 ${banExpiryStr}`;
            await sendMessageToTopic(generalTopicId, notifyMsg);
            
            await logAdminAction('system', 'auto_ban', chatId, `失败次数达到阈值 ${failThreshold}`);
          } else {
            await env.D1.prepare(`
              UPDATE user_states 
              SET failed_attempts = ?, is_verifying = FALSE
              WHERE chat_id = ?
            `).bind(newAttempts, chatId).run();
            await sendMessageToUser(chatId, `验证失败，剩余尝试次数: ${failThreshold - newAttempts}`);
          }
          
          verificationState.failed_attempts = newAttempts;
          verificationState.ban_expiry = (newAttempts >= failThreshold) ? (nowSeconds + banSeconds) : verificationState.ban_expiry;
          verificationState.is_verifying = false;
          userStateCache.set(chatId, verificationState);
        }

        await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/deleteMessage`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ chat_id: chatId, message_id: messageId })
        });
      } else {
        const senderId = callbackQuery.from.id.toString();
        const isAdmin = await checkIfAdmin(senderId);
        if (!isAdmin) {
          await sendMessageToTopic(topicId, '只有管理员可以使用此功能。');
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
          await logAdminAction(senderId, 'block_user', privateChatId, '管理员拉黑用户');
          await sendMessageToTopic(topicId, `用户 ${privateChatId} 已被拉黑，消息将不再转发。`);
        } else if (action === 'unblock') {
          let state = userStateCache.get(privateChatId);
          if (state === undefined) {
            state = await env.D1.prepare('SELECT is_blocked, is_first_verification FROM user_states WHERE chat_id = ?')
              .bind(privateChatId)
              .first() || { is_blocked: false, is_first_verification: true };
          }
          state.is_blocked = false;
          state.is_first_verification = true;
          userStateCache.set(privateChatId, state);
          await env.D1.prepare('INSERT OR REPLACE INTO user_states (chat_id, is_blocked, is_first_verification) VALUES (?, ?, ?)')
            .bind(privateChatId, false, true)
            .run();
          await logAdminAction(senderId, 'unblock_user', privateChatId, '管理员解除拉黑');
          await sendMessageToTopic(topicId, `用户 ${privateChatId} 已解除拉黑，消息将继续转发。`);
        } else if (action === 'clear_ban') {
          await env.D1.prepare(`
            UPDATE user_states 
            SET failed_attempts = ?, ban_expiry = ?
            WHERE chat_id = ?
          `).bind(0, null, privateChatId).run();
          let state = userStateCache.get(privateChatId);
          if (state) {
            state.failed_attempts = 0;
            state.ban_expiry = null;
            userStateCache.set(privateChatId, state);
          }
          await logAdminAction(senderId, 'clear_ban', privateChatId, '管理员解除验证封禁');
          await sendMessageToTopic(topicId, `用户 ${privateChatId} 的验证封禁已解除。`);
        } else if (action === 'toggle_verification') {
          const currentState = (await getSetting('verification_enabled', env.D1)) === 'true';
          const newState = !currentState;
          await setSetting('verification_enabled', newState.toString());
          await logAdminAction(senderId, 'toggle_verification', '', `验证码状态: ${currentState} -> ${newState}`);
          await sendMessageToTopic(topicId, `验证码功能已${newState ? '开启' : '关闭'}。`);
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
          await logAdminAction(senderId, 'toggle_user_raw', '', `Raw链接: ${currentState} -> ${newState}`);
          await sendMessageToTopic(topicId, `用户端 Raw 链接已${newState ? '开启' : '关闭'}。`);
        } else if (action === 'delete_user') {
          const userTopicId = await getExistingTopicId(privateChatId);
          if (userTopicId) {
            try {
              await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/deleteForumTopic`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ chat_id: GROUP_ID, message_thread_id: userTopicId })
              });
            } catch (error) {
              console.log(`删除话题失败: ${error.message}`);
            }
          }

          userStateCache.set(privateChatId, undefined);
          messageRateCache.set(privateChatId, undefined);
          topicIdCache.set(privateChatId, undefined);
          await env.D1.batch([
            env.D1.prepare('DELETE FROM user_states WHERE chat_id = ?').bind(privateChatId),
            env.D1.prepare('DELETE FROM message_rates WHERE chat_id = ?').bind(privateChatId),
            env.D1.prepare('DELETE FROM chat_topic_mappings WHERE chat_id = ?').bind(privateChatId)
          ]);
          await logAdminAction(senderId, 'delete_user', privateChatId, '管理员结束会话并删除话题');
          await sendMessageToTopic(topicId, `用户 ${privateChatId} 的状态、消息记录、话题映射已删除，对应的讨论话题也已删除。`);
        } else {
          await sendMessageToTopic(topicId, `未知操作：${action}`);
        }

        await updateAdminPanel(chatId, topicId, privateChatId, messageId);
      }

      await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/answerCallbackQuery`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ callback_query_id: callbackQuery.id })
      });

      cleanupAdminPanelMessages();
    }

    async function sendAdminPanelInTopic(topicId, privateChatId) {
      const verificationEnabled = (await getSetting('verification_enabled', env.D1)) === 'true';

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
          { text: '解除验证封禁', callback_data: `clear_ban_${privateChatId}` },
          { text: '结束会话', callback_data: `delete_user_${privateChatId}` }
        ]
      ];

      const adminMessage = '管理员面板：请选择操作';
      await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/sendMessage`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          chat_id: GROUP_ID,
          message_thread_id: topicId,
          text: adminMessage,
          reply_markup: { inline_keyboard: buttons }
        })
      });
    }

    async function updateAdminPanel(chatId, topicId, privateChatId, messageId) {
      const verificationEnabled = (await getSetting('verification_enabled', env.D1)) === 'true';

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
          { text: '解除验证封禁', callback_data: `clear_ban_${privateChatId}` },
          { text: '结束会话', callback_data: `delete_user_${privateChatId}` }
        ]
      ];

      const adminMessage = '管理员面板：请选择操作';
      await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/editMessageText`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          chat_id: chatId,
          message_id: messageId,
          text: adminMessage,
          reply_markup: { inline_keyboard: buttons }
        })
      });
    }

    async function handleVerification(chatId, messageId) {
      try {
        let userState = userStateCache.get(chatId);
        if (userState === undefined) {
          userState = await env.D1.prepare('SELECT is_blocked, is_first_verification, is_verified, verified_expiry, is_verifying, failed_attempts, ban_expiry FROM user_states WHERE chat_id = ?')
            .bind(chatId)
            .first();
          if (!userState) {
            userState = { is_blocked: false, is_first_verification: true, is_verified: false, verified_expiry: null, is_verifying: false, failed_attempts: 0, ban_expiry: null };
          }
          userStateCache.set(chatId, userState);
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
              body: JSON.stringify({ chat_id: chatId, message_id: lastVerification })
            });
          } catch (deleteError) {}
          
          userState.last_verification_message_id = null;
          userStateCache.set(chatId, userState);
          await env.D1.prepare('UPDATE user_states SET last_verification_message_id = NULL WHERE chat_id = ?')
            .bind(chatId)
            .run();
        }

        await sendVerification(chatId);
      } catch (error) {
        console.error(`处理验证过程失败: ${error.message}`);
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
          console.error(`重置用户验证状态失败: ${resetError.message}`);
        }
        throw error;
      }
    }

    async function sendVerification(chatId) {
      try {
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
          await env.D1.prepare(`
            UPDATE user_states 
            SET verification_code = ?, code_expiry = ?, last_verification_message_id = ?, is_verifying = ?, last_verification_at = ?
            WHERE chat_id = ?
          `).bind(correctResult.toString(), codeExpiry, data.result.message_id.toString(), true, nowSeconds, chatId).run();
        } else {
          throw new Error(`Telegram API 返回错误: ${data.description || '未知错误'}`);
        }
      } catch (error) {
        console.error(`发送验证码失败: ${error.message}`);
        throw error;
      }
    }

    async function checkIfAdmin(userId) {
      const response = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/getChatMember`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ chat_id: GROUP_ID, user_id: userId })
      });
      const data = await response.json();
      return data.ok && (data.result.status === 'administrator' || data.result.status === 'creator');
    }

    async function getUserInfo(chatId) {
      let userInfo = userInfoCache.get(chatId);
      if (userInfo !== undefined) return userInfo;

      const response = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/getChat`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ chat_id: chatId })
      });
      const data = await response.json();
      if (!data.ok) {
        userInfo = { id: chatId, username: `User_${chatId}`, nickname: `User_${chatId}` };
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
      if (topicId !== undefined) return topicId;

      const result = await env.D1.prepare('SELECT topic_id FROM chat_topic_mappings WHERE chat_id = ?')
        .bind(chatId)
        .first();
      topicId = result?.topic_id || null;
      if (topicId) topicIdCache.set(chatId, topicId);
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

      const now = new Date();
      const formattedTime = formatBeijingTime(now);
      const notificationContent = await getNotificationContent();
      const pinnedMessage = `昵称: ${nickname}\n用户名: @${userName}\nUserID: ${userId}\n发起时间: ${formattedTime}\n\n${notificationContent}`;
      const messageResponse = await sendMessageToTopic(topicId, pinnedMessage);
      const messageId = messageResponse.result.message_id;
      await pinMessage(topicId, messageId);

      await sendAdminPanelInTopic(topicId, userId);
      return topicId;
    }

    async function saveTopicId(chatId, topicId) {
      await env.D1.prepare('INSERT OR REPLACE INTO chat_topic_mappings (chat_id, topic_id) VALUES (?, ?)')
        .bind(chatId, topicId)
        .run();
      topicIdCache.set(chatId, topicId);
    }

    async function getPrivateChatId(topicId) {
      for (const [chatId, tid] of topicIdCache.cache) if (tid === topicId) return chatId;
      const mapping = await env.D1.prepare('SELECT chat_id FROM chat_topic_mappings WHERE topic_id = ?')
        .bind(topicId)
        .first();
      return mapping?.chat_id || null;
    }

    async function sendMessageToTopic(topicId, text) {
      if (!text.trim()) throw new Error('Message text is empty');
      const response = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/sendMessage`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ chat_id: GROUP_ID, text: text, message_thread_id: topicId })
      });
      const data = await response.json();
      if (!data.ok) throw new Error(`Failed to send message to topic ${topicId}: ${data.description}`);
      return data;
    }

    async function copyMessageToTopic(topicId, message) {
      const response = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/copyMessage`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          chat_id: GROUP_ID,
          from_chat_id: message.chat.id,
          message_id: message.message_id,
          message_thread_id: topicId,
          disable_notification: true
        })
      });
      const data = await response.json();
      if (!data.ok) throw new Error(`Failed to copy message to topic ${topicId}: ${data.description}`);
    }

    async function pinMessage(topicId, messageId) {
      const response = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/pinChatMessage`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ chat_id: GROUP_ID, message_id: messageId, message_thread_id: topicId })
      });
      const data = await response.json();
      if (!data.ok) throw new Error(`Failed to pin message: ${data.description}`);
    }

    async function forwardMessageToPrivateChat(privateChatId, message) {
      const response = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/copyMessage`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          chat_id: privateChatId,
          from_chat_id: message.chat.id,
          message_id: message.message_id,
          disable_notification: true
        })
      });
      const data = await response.json();
      if (!data.ok) throw new Error(`Failed to forward message to private chat: ${data.description}`);
    }

    async function sendMessageToUser(chatId, text) {
      const response = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/sendMessage`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ chat_id: chatId, text: text })
      });
      const data = await response.json();
      if (!data.ok) throw new Error(`Failed to send message to user: ${data.description}`);
    }

    async function fetchWithRetry(url, options, retries = 3, backoff = 1000) {
      for (let i = 0; i < retries; i++) {
        try {
          const controller = new AbortController();
          const timeoutId = setTimeout(() => controller.abort(), 5000);
          const response = await fetch(url, { ...options, signal: controller.signal });
          clearTimeout(timeoutId);
          if (response.ok) return response;
          if (response.status === 429) {
            const retryAfter = response.headers.get('Retry-After') || 5;
            await new Promise(resolve => setTimeout(resolve, parseInt(retryAfter) * 1000));
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

    async function deleteMessage(chatId, messageId) {
      try {
        await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/deleteMessage`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ chat_id: chatId, message_id: messageId })
        });
      } catch (error) {
        console.log(`删除消息失败: ${error.message}`);
      }
    }

    function cleanupAdminPanelMessages() {
      if (adminPanelMessages.size > 1000) {
        adminPanelMessages.clear();
      }
    }

    async function validateTopic(topicId) {
      try {
        const response = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/sendMessage`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ chat_id: GROUP_ID, message_thread_id: topicId, text: "您有新消息！", disable_notification: true })
        });
        const data = await response.json();
        if (data.ok) {
          await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/deleteMessage`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ chat_id: GROUP_ID, message_id: data.result.message_id })
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
        if (topicId) return topicId;

        const newLock = (async () => {
          const userName = userInfo.username || `User_${chatId}`;
          const nickname = userInfo.nickname || userName;
          topicId = await createForumTopic(nickname, userName, nickname, userInfo.id || chatId);
          await saveTopicId(chatId, topicId);
          return topicId;
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

    async function getVerificationSuccessMessage() {
      const userRawEnabled = (await getSetting('user_raw_enabled', env.D1)) === 'true';
      if (!userRawEnabled) return '验证成功。';

      const response = await fetch('https://raw.githubusercontent.com/wuyangdaily/ctt/refs/heads/main/CFTeleTrans/start.md');
      if (!response.ok) return '验证成功。';
      const message = await response.text();
      return message.trim() || '验证成功。';
    }

    async function getNotificationContent() {
      const response = await fetch('https://raw.githubusercontent.com/wuyangdaily/ctt/refs/heads/main/CFTeleTrans/notification.md');
      if (!response.ok) return '';
      const content = await response.text();
      return content.trim() || '';
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
      const cached = settingsCache.get(key);
      if (cached !== null && cached !== undefined) return cached.toString();
      const result = await d1.prepare('SELECT value FROM settings WHERE key = ?').bind(key).first();
      const value = result?.value || null;
      settingsCache.set(key, value);
      return value;
    }

    async function setSetting(key, value) {
      await env.D1.prepare('INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)').bind(key, value).run();
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
    }

    try {
      return await handleRequest(request);
    } catch (error) {
      console.error('Unhandled error:', error);
      return new Response('Internal Server Error', { status: 500 });
    }
  }
};
