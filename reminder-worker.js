// reminder-worker.js
// Drains ReminderQueue_v1 (Ordered Data Store) and fires "come back"
// Experience Notifications for every entry that is due (value <= now).

const API_KEY     = process.env.ROBLOX_API_KEY;
const UNIVERSE_ID = process.env.UNIVERSE_ID || "6033574667";
const ODS_NAME    = process.env.ODS_NAME    || "ReminderQueue_v1";
const MESSAGE_ID  = process.env.MESSAGE_ID  || "7f7141f1-d906-504e-85f5-e235b5b91f25";

const PAGE_SIZE   = 100;    // entries per list page
const MAX_PER_RUN = 800;    // drain up to this many per run (raised from 200 to clear backlog)
const REQ_TIMEOUT = 10000;  // ms; abort any single request that stalls (prevents multi-min hangs)

const ODS_BASE = "https://apis.roblox.com/ordered-data-stores/v1";

if (!API_KEY) {
  console.error("ROBLOX_API_KEY is not set");
  process.exit(1);
}

// fetch with a hard per-request timeout so one stalled call can't hang the whole run.
function fetchWithTimeout(url, opts = {}) {
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), REQ_TIMEOUT);
  return fetch(url, { ...opts, signal: ctrl.signal }).finally(() => clearTimeout(t));
}

// ---------------------------------------------------------------------------
// List due entries. Ascending by value means due entries come first, so we can
// stop the moment we hit one in the future. THROWS on any non-2xx so a bad
// request can never masquerade as "empty".
// ---------------------------------------------------------------------------
async function fetchDueEntries(now) {
  const due = [];
  let pageToken = null;

  while (due.length < MAX_PER_RUN) {
    const url = new URL(
      `${ODS_BASE}/universes/${UNIVERSE_ID}/orderedDataStores/${ODS_NAME}/scopes/global/entries`
    );
    url.searchParams.set("max_page_size", String(PAGE_SIZE));
    url.searchParams.set("order_by", "asc"); // this endpoint wants asc/desc, NOT "value"
    if (pageToken) url.searchParams.set("page_token", pageToken);

    const res = await fetchWithTimeout(url, { headers: { "x-api-key": API_KEY } });
    if (!res.ok) {
      const body = await res.text();
      throw new Error(`List failed: HTTP ${res.status} ${body}`);
    }

    const data = await res.json();
    const entries = data.entries || [];
    if (entries.length === 0) break; // empty page -> nothing more to do

    for (const e of entries) {
      const value = parseInt(e.value, 10); // value comes back as a STRING
      if (Number.isNaN(value)) continue;
      if (value <= now) {
        due.push(e); // { path, id, value }
      } else {
        return due;  // ascending -> everything after this is in the future
      }
      if (due.length >= MAX_PER_RUN) break;
    }

    pageToken = data.nextPageToken;
    if (!pageToken) break;
  }

  return due;
}

// Send one notification. Returns "sent" | "skip" | "ratelimited".
// entry id is "u<userId>" -> strip the "u" for the users/{id} endpoint.
async function sendNotification(entryId) {
  const userId = entryId.replace(/^u/, "");
  const res = await fetchWithTimeout(
    `https://apis.roblox.com/cloud/v2/users/${userId}/notifications`,
    {
      method: "POST",
      headers: { "x-api-key": API_KEY, "Content-Type": "application/json" },
      body: JSON.stringify({
        source: { universe: `universes/${UNIVERSE_ID}` },
        payload: { type: "MOMENT", messageId: MESSAGE_ID },
      }),
    }
  );

  if (res.status === 429) return "ratelimited";
  if (!res.ok) {
    // 400/403/404 are expected for users who opted out / can't be prompted.
    const body = await res.text();
    console.warn(`send ${userId}: HTTP ${res.status} ${body}`);
    return "skip";
  }
  return "sent";
}

// entry.path is the full resource path the API handed us -> delete straight off it.
async function deleteEntry(path) {
  const res = await fetchWithTimeout(`${ODS_BASE}/${path}`, {
    method: "DELETE",
    headers: { "x-api-key": API_KEY },
  });
  if (!res.ok && res.status !== 404) {
    const body = await res.text();
    console.warn(`delete ${path}: HTTP ${res.status} ${body}`);
  }
}

// ---------------------------------------------------------------------------
async function main() {
  const now = Math.floor(Date.now() / 1000); // Unix SECONDS (queue stores seconds)
  const due = await fetchDueEntries(now);
  console.log(`Found ${due.length} due entr${due.length === 1 ? "y" : "ies"} at ${now}`);

  let sent = 0, skipped = 0;
  for (const e of due) {
    const result = await sendNotification(e.id);

    if (result === "ratelimited") {
      console.warn("Rate limited -> stopping this run; remaining entries stay queued for retry.");
      break;
    }

    await deleteEntry(e.path); // sent or skip -> drain it either way
    if (result === "sent") sent++; else skipped++;
  }

  console.log(`Done. sent=${sent} skipped=${skipped} remaining_in_batch=${due.length - sent - skipped}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1); // fail loudly -> GitHub Actions marks the run red
});
