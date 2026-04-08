// Load .env BEFORE anything else
require("dotenv").config({ path: require("path").join(__dirname, "..", ".env"), override: true });

const express = require("express");
const Anthropic = require("@anthropic-ai/sdk").default;
const {
  StreamableHTTPClientTransport,
} = require("@modelcontextprotocol/sdk/client/streamableHttp.js");
const { Client } = require("@modelcontextprotocol/sdk/client/index.js");

const app = express();
app.use(express.json({ limit: "1mb" }));

const PORT = 3001;

const ANTHROPIC_API_KEY = process.env.ANTHROPIC_API_KEY;
if (!ANTHROPIC_API_KEY) {
  console.error("ERROR: ANTHROPIC_API_KEY not set");
  process.exit(1);
}

const anthropic = new Anthropic({ apiKey: ANTHROPIC_API_KEY });

const MCP_URL = "http://34.73.107.214:8000/mcp";
const MCP_AUTH =
  "mcp_ak_7d9e859878c869751f3841a631a394ee2799ce17ef8df6c693e7e5e8f00b9d63";

const SYSTEM_PROMPT = `You are Terabot, AI assistant for Terasofts Data Center with access to real analytics data via tools.

RULES:
- Answer in the user's language. Be concise — no filler, no preamble.
- Data questions: call ONE tool, present key numbers in a short table or list, add 1-2 sentence insight. Do NOT call multiple tools unless explicitly asked to compare.
- General questions (no data needed): answer directly, do NOT call any tool.
- Max response length: ~300 words. Use bullet points and tables over paragraphs.
- Numbers: use K/M suffixes (e.g. 325K, $4.2K). Round to 2 decimals max.
- Do NOT repeat the question back. Do NOT add disclaimers or caveats.
- If tool returns empty/error, say so briefly and suggest alternatives.
- TIMEZONE: Always use UTC+7 (Asia/Ho_Chi_Minh) for ALL date/time parameters when calling tools. When displaying dates to users, use UTC+7. This ensures data consistency across all sources.`;

// ─── Persistent MCP connection pool ───
let mcpClient = null;
let mcpConnecting = false;
let mcpToolsCache = null;

async function ensureMcpClient() {
  if (mcpClient) return mcpClient;
  if (mcpConnecting) {
    // Wait for ongoing connection
    while (mcpConnecting) await new Promise((r) => setTimeout(r, 50));
    return mcpClient;
  }
  mcpConnecting = true;
  try {
    const transport = new StreamableHTTPClientTransport(new URL(MCP_URL), {
      requestInit: { headers: { Authorization: MCP_AUTH } },
    });
    const client = new Client({ name: "terabot", version: "1.0.0" });
    await client.connect(transport);
    mcpClient = client;

    // Pre-cache tools on connect
    const result = await client.listTools();
    mcpToolsCache = result.tools.map((t) => ({
      name: t.name,
      description: t.description || "",
      input_schema: t.inputSchema || { type: "object", properties: {} },
    }));

    console.log(`[MCP] Connected, ${mcpToolsCache.length} tools cached`);
    return client;
  } catch (err) {
    mcpClient = null;
    throw err;
  } finally {
    mcpConnecting = false;
  }
}

async function resetMcpClient() {
  if (mcpClient) {
    await mcpClient.close().catch(() => {});
    mcpClient = null;
  }
}

async function getMcpTools() {
  if (mcpToolsCache) return mcpToolsCache;
  await ensureMcpClient();
  return mcpToolsCache;
}

async function callMcpTool(toolName, toolInput) {
  const client = await ensureMcpClient();
  try {
    const result = await client.callTool({
      name: toolName,
      arguments: toolInput,
    });
    if (result.content && Array.isArray(result.content)) {
      return result.content
        .map((c) => (c.type === "text" ? c.text : JSON.stringify(c)))
        .join("\n");
    }
    return JSON.stringify(result);
  } catch (err) {
    // Connection broken, reset and retry once
    if (err.message && (err.message.includes("closed") || err.message.includes("ECONNR"))) {
      console.log("[MCP] Connection lost, reconnecting...");
      await resetMcpClient();
      const client2 = await ensureMcpClient();
      const result = await client2.callTool({ name: toolName, arguments: toolInput });
      if (result.content && Array.isArray(result.content)) {
        return result.content.map((c) => (c.type === "text" ? c.text : JSON.stringify(c))).join("\n");
      }
      return JSON.stringify(result);
    }
    throw err;
  }
}

// ─── Call multiple tools in parallel ───
async function callMcpToolsParallel(toolBlocks) {
  const results = await Promise.allSettled(
    toolBlocks.map(async (tb) => {
      console.log(`[MCP] Calling: ${tb.name}`);
      const t0 = Date.now();
      const result = await callMcpTool(tb.name, tb.input);
      console.log(`[MCP] ${tb.name} done in ${Date.now() - t0}ms`);
      return { id: tb.id, content: result };
    })
  );

  return results.map((r, i) => {
    if (r.status === "fulfilled") {
      return { type: "tool_result", tool_use_id: r.value.id, content: r.value.content };
    }
    console.error(`[MCP] Tool error (${toolBlocks[i].name}):`, r.reason?.message);
    return { type: "tool_result", tool_use_id: toolBlocks[i].id, content: `Error: ${r.reason?.message}`, is_error: true };
  });
}

// ─── Cost optimization helpers ───

// Truncate tool results to avoid sending huge payloads back to Claude
const MAX_TOOL_RESULT_CHARS = 4000;
function truncateToolResult(text) {
  if (text.length <= MAX_TOOL_RESULT_CHARS) return text;
  return text.slice(0, MAX_TOOL_RESULT_CHARS) + "\n...[truncated]";
}

// Summarize old assistant messages to reduce input tokens
function compressHistory(messages) {
  if (messages.length <= 4) return messages;
  // Keep last 4 messages as-is, summarize older ones
  const old = messages.slice(0, -4);
  const recent = messages.slice(-4);

  // Compress old messages into a single context line
  const summary = old
    .map((m) => {
      const text = String(m.content);
      if (m.role === "user") return `Q: ${text.slice(0, 80)}`;
      // Truncate long assistant responses in history
      return `A: ${text.slice(0, 120)}...`;
    })
    .join(" | ");

  return [
    { role: "user", content: `[Previous context: ${summary}]` },
    { role: "assistant", content: "Understood, I have the context." },
    ...recent,
  ];
}

// Detect if question needs data tools or is just general chat
const DATA_KEYWORDS = /\b(dau|mau|wau|revenue|install|cost|roas|ecpi|session|retention|arpu|arppu|ltv|churn|crash|metric|report|data|analytics|app version|marketing|campaign|country|cohort|product_health|product_metric|pdf1|aip016|gpt1|apl125)\b/i;

function needsTools(userMsg) {
  return DATA_KEYWORDS.test(userMsg);
}

// Choose model based on complexity
const MODEL_FAST = "claude-haiku-4-5-20251001";
const MODEL_SMART = "claude-sonnet-4-20250514";

function chooseModel(userMsg, withTools) {
  // Use Haiku for simple greetings, general chat without tools
  if (!withTools) {
    const simple = /^(hi|hello|hey|xin chao|chao|thanks|cam on|ok|bye|help|giup)\b/i;
    if (simple.test(userMsg.trim()) || userMsg.trim().length < 20) {
      return MODEL_FAST;
    }
  }
  // Use Sonnet for data queries (needs tool calling)
  return MODEL_SMART;
}

// ─── Chat endpoint ───
app.post("/chat", async (req, res) => {
  const { messages } = req.body;

  if (!messages || !Array.isArray(messages) || messages.length === 0) {
    return res.status(400).json({ error: "No messages provided" });
  }

  const t0 = Date.now();

  try {
    // Clean: only user/assistant text, last 6 messages
    const cleaned = messages
      .filter((m) => m.role === "user" || m.role === "assistant")
      .map((m) => ({ role: m.role, content: String(m.content) }));
    const trimmed = cleaned.length > 6 ? cleaned.slice(-6) : cleaned;
    const startIdx = trimmed.findIndex((m) => m.role === "user");
    const safeMessages = startIdx > 0 ? trimmed.slice(startIdx) : trimmed;

    // Compress old history to save tokens
    const compressed = compressHistory(safeMessages);

    // Get the latest user message
    const lastUserMsg = [...compressed].reverse().find((m) => m.role === "user")?.content || "";
    const wantsData = needsTools(lastUserMsg);

    // Choose model and tools
    const model = chooseModel(lastUserMsg, wantsData);
    const tools = wantsData ? await getMcpTools() : [];
    const maxTokens = wantsData ? 1500 : 800;

    console.log(`[CHAT] model=${model} tools=${wantsData} tokens=${maxTokens}`);

    let currentMessages = [...compressed];
    const MAX_TURNS = 3;
    let lastReply = "";

    for (let turn = 0; turn < MAX_TURNS; turn++) {
      const params = {
        model: model,
        max_tokens: maxTokens,
        system: SYSTEM_PROMPT,
        messages: currentMessages,
      };
      if (tools.length > 0) params.tools = tools;

      const response = await anthropic.messages.create(params);

      let turnText = "";
      for (const block of response.content) {
        if (block.type === "text") turnText += block.text;
      }
      if (turnText) lastReply = turnText;

      const toolUseBlocks = response.content.filter((b) => b.type === "tool_use");

      if (toolUseBlocks.length === 0) {
        console.log(`[CHAT] Done in ${Date.now() - t0}ms (${turn + 1} turns, ${model})`);
        return res.json({ reply: lastReply || "No response." });
      }

      // Execute tool calls in parallel, truncate results
      currentMessages.push({ role: "assistant", content: response.content });
      const toolResults = await callMcpToolsParallel(toolUseBlocks);

      // Truncate large tool results to save tokens on next turn
      const truncatedResults = toolResults.map((r) => ({
        ...r,
        content: typeof r.content === "string" ? truncateToolResult(r.content) : r.content,
      }));

      currentMessages.push({ role: "user", content: truncatedResults });
    }

    console.log(`[CHAT] Max turns in ${Date.now() - t0}ms`);
    res.json({ reply: lastReply || "Reached maximum processing steps." });
  } catch (err) {
    console.error("Error:", err.message);
    res.status(500).json({ error: err.message });
  }
});

app.get("/health", (req, res) => {
  res.json({ status: "ok", mcp_connected: !!mcpClient, tools: mcpToolsCache?.length || 0 });
});

// Pre-connect MCP on startup
ensureMcpClient().catch((e) => console.error("[MCP] Startup connect failed:", e.message));

app.listen(PORT, () => {
  console.log(`Terabot proxy running on port ${PORT}`);
});
