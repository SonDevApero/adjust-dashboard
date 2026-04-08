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

const SYSTEM_PROMPT = `You are Terabot, an AI assistant for Terasofts Data Center. You have access to Data Gateway tools that provide real app analytics data (installs, revenue, DAU, sessions, ROAS, cost, marketing metrics, etc.).

When users ask about app data, metrics, or performance:
1. Use the available tools to fetch real data
2. Present the data clearly and concisely
3. Provide brief insights

Answer in the same language the user uses. Be concise. Avoid unnecessary verbose explanations.`;

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

// ─── Chat endpoint ───
app.post("/chat", async (req, res) => {
  const { messages } = req.body;

  if (!messages || !Array.isArray(messages) || messages.length === 0) {
    return res.status(400).json({ error: "No messages provided" });
  }

  const t0 = Date.now();

  try {
    const tools = await getMcpTools();

    // Clean & trim: only user/assistant text, last 6 messages to reduce tokens
    const cleaned = messages
      .filter((m) => m.role === "user" || m.role === "assistant")
      .map((m) => ({ role: m.role, content: String(m.content) }));
    const trimmed = cleaned.length > 6 ? cleaned.slice(-6) : cleaned;
    const startIdx = trimmed.findIndex((m) => m.role === "user");
    const safeMessages = startIdx > 0 ? trimmed.slice(startIdx) : trimmed;

    let currentMessages = [...safeMessages];
    const MAX_TURNS = 5;
    let lastReply = "";

    for (let turn = 0; turn < MAX_TURNS; turn++) {
      const response = await anthropic.messages.create({
        model: "claude-sonnet-4-20250514",
        max_tokens: 2048,
        system: SYSTEM_PROMPT,
        messages: currentMessages,
        tools: tools,
      });

      let turnText = "";
      for (const block of response.content) {
        if (block.type === "text") turnText += block.text;
      }
      if (turnText) lastReply = turnText;

      const toolUseBlocks = response.content.filter((b) => b.type === "tool_use");

      if (toolUseBlocks.length === 0) {
        console.log(`[CHAT] Done in ${Date.now() - t0}ms (${turn + 1} turns)`);
        return res.json({ reply: lastReply || "No response." });
      }

      // Execute all tool calls in parallel
      currentMessages.push({ role: "assistant", content: response.content });
      const toolResults = await callMcpToolsParallel(toolUseBlocks);
      currentMessages.push({ role: "user", content: toolResults });
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
