// DAILY NEW-PLAYER DISCOVERY — external, ZERO game-side load.
//
// Problem: a brand-new player must land in ReminderQueue_v1 fast (so the daily
// Cloudflare worker pings them the next UTC day), but we refuse to add any
// in-game DataStore writes (they caused a join-stampede lag incident before),
// and Roblox's notification rate limit makes re-probing all ~445k players daily
// impossible (~24h/full pass). We also can't ask Open Cloud for "keys created
// today" -- the List Entries endpoint has NO ordering and NO createTime filter
// (verified: only id.startsWith + pagination).
//
// Insight: LISTING keys is cheap/fast (~1h for the whole store, no sends). Only
// PROBING (send-to-discover-opt-in) is slow. So we don't re-probe everyone --
// we snapshot-diff:
//   1. Keep a persisted set of player keys we've already seen (known-keys.txt,
//      restored/saved via actions/cache between daily runs).
//   2. List ALL DailyRewards_v2 keys; any key NOT in the set is NEW since the
//      last run (a player who first got reward data since yesterday).
//   3. Probe ONLY the new keys. That daily delta is small (hundreds/thousands),
//      so it finishes in minutes and never hits the rate-limit wall.
//   4. Opted-in new players are upserted into ReminderQueue_v1 at the next day
//      boundary -> the every-minute worker notifies them next UTC day.
//
// This catches new PLAYERS fast. It does NOT catch existing players who opt in
// LATER (their key isn't new) -- the slow full rescan (backfill.js) remains the
// catch-all for opt-in-status changes on the long tail.
//
// COLD START (empty/missing snapshot): we DON'T probe the whole ~445k backlog
// (that's the slow rescan's job and would blow the CI time budget). We just
// learn the current keyspace into the snapshot and begin diffing from the next
// run. Same behavior if the cache is ever evicted -- a safe re-snapshot, with
// the slow rescan covering anyone missed during the gap.
//
// USAGE:
//   node discover-new.js            -> DRY RUN: lists + reports the new-key count, sends/writes nothing.
//   node discover-new.js --write    -> probes new keys, seeds opted-in ones, updates the snapshot.

const fs   = require("fs");
const path = require("path");

const API_KEY     = process.env.ROBLOX_API_KEY;
const UNIVERSE_ID = process.env.UNIVERSE_ID || "6033574667";
const DAILY_STORE = "DailyRewards_v2";
const ODS_NAME    = "ReminderQueue_v1";
const MESSAGE_ID  = process.env.MESSAGE_ID || "7f7141f1-d906-504e-85f5-e235b5b91f25";
const RESET_OFFSET_HOURS = parseInt(process.env.RESET_OFFSET_HOURS || "0", 10);

const DAY_SECONDS = 86400;
const WRITE = process.argv.includes("--write");
const SNAPSHOT_FILE = path.join(__dirname, "known-keys.txt");

// Safety cap: probe at most this many NEW keys per run. Deferred keys are NOT
// added to the snapshot, so they are re-tried next run instead of dropped.
//
// RAISED 5,000 -> 50,000 (2026-07-21). 5k was far too low: this game gains
// ~10k new players/day, so the cap deferred ~5k MORE than it cleared each run
// and the backlog grew without bound (observed: new=37,568 probed=5,000
// deferred=32,568). Since ~15% of NEW keys are opted-in (vs 5.7% overall),
// that stranded ~4,900 opted-in players who should have been getting pinged.
// The cap was never the real constraint -- TIME is, bounded separately by
// DISCOVER_MAX_MINUTES. At 60ms/probe, 50k probes = ~50min on top of the
// ~84min list pass, comfortably inside the 300min budget.
const MAX_NEW_PROBE = parseInt(process.env.MAX_NEW_PROBE || "50000", 10);

// Wall-clock budget (minutes); 0 = unlimited (local). In CI set below the job
// timeout so we save the snapshot + exit 0 gracefully (a timeout-kill would be a
// cancellation and skip actions/cache's post-save).
const MAX_RUNTIME_MS = parseInt(process.env.DISCOVER_MAX_MINUTES || "0", 10) * 60_000;

const SEND_DELAY_MS = 60;   // notification endpoint tolerated high throughput in testing
const LIST_DELAY_MS = 250;  // list endpoint 429'd with no pacing; 250ms was safe

const DS_BASE   = "https://apis.roblox.com/cloud/v2";
const ODS_BASE  = "https://apis.roblox.com/ordered-data-stores/v1";
const NOTIF_URL = (userId) => `https://apis.roblox.com/cloud/v2/users/${userId}/notifications`;

if (!API_KEY) { console.error("ROBLOX_API_KEY is not set"); process.exit(1); }

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// Snapshot is a newline-delimited list of keys (e.g. "u12345"). ~5MB at 445k.
function loadSnapshot() {
	if (!fs.existsSync(SNAPSHOT_FILE)) return new Set();
	try {
		const txt = fs.readFileSync(SNAPSHOT_FILE, "utf8");
		return new Set(txt.split("\n").filter(Boolean));
	} catch { return new Set(); }
}
function saveSnapshot(set) {
	// Write to a temp file then rename, so a crash mid-write can't corrupt the snapshot.
	const tmp = SNAPSHOT_FILE + ".tmp";
	fs.writeFileSync(tmp, Array.from(set).join("\n"));
	fs.renameSync(tmp, SNAPSHOT_FILE);
}

// fetch that backs off on 429 / 5xx and retries the SAME request.
async function fetchRetry(url, opts = {}, label = "req") {
	for (let attempt = 0; ; attempt++) {
		const res = await fetch(url, opts);
		if (res.status !== 429 && res.status < 500) return res;
		if (attempt >= 8) return res;
		const wait = Math.min(30000, 1000 * 2 ** attempt);
		console.log(`${label}: HTTP ${res.status} -> backoff ${wait}ms (attempt ${attempt + 1})`);
		await sleep(wait);
	}
}

function nextBoundary(now) {
	const offset = RESET_OFFSET_HOURS * 3600;
	return (Math.floor((now - offset) / DAY_SECONDS) + 1) * DAY_SECONDS + offset;
}

// Real send attempt -- the only way to discover opt-in. Mirrors backfill.js:
// dailycap (opted in, capped today) counts as opted-in; other non-2xx = not
// opted in; 429 RESOURCE_EXHAUSTED = daily cap (no retry), else short backoff.
async function sendNotification(userId, notifBody) {
	for (let attempt = 0; ; attempt++) {
		const res = await fetch(NOTIF_URL(userId), {
			method: "POST",
			headers: { "x-api-key": API_KEY, "Content-Type": "application/json" },
			body: notifBody,
		});
		if (res.status === 429) {
			let code = null;
			try { code = JSON.parse(await res.text()).code; } catch {}
			if (code === "RESOURCE_EXHAUSTED") return "dailycap";
			if (attempt >= 5) return "ratelimited";
			const wait = Math.min(15000, 500 * 2 ** attempt);
			console.log(`send ${userId}: throttled -> backoff ${wait}ms (attempt ${attempt + 1})`);
			await sleep(wait);
			continue;
		}
		if (!res.ok) return "notoptedin";
		return "sent";
	}
}

async function upsert(entryId, value) {
	const res = await fetchRetry(
		`${ODS_BASE}/universes/${UNIVERSE_ID}/orderedDataStores/${ODS_NAME}/scopes/global/entries/${entryId}?allow_missing=true`,
		{
			method: "PATCH",
			headers: { "x-api-key": API_KEY, "Content-Type": "application/json" },
			body: JSON.stringify({ value }),
		},
		`upsert ${entryId}`
	);
	if (!res.ok) { console.warn(`upsert ${entryId}: HTTP ${res.status}`); return false; }
	return true;
}

async function main() {
	const startMs = Date.now();
	const now = Math.floor(Date.now() / 1000);
	// Schedule newcomers a FULL day out, not at the next boundary. The probe below
	// IS their first notification (that is how opt-in gets discovered), so seeding
	// them at the next midnight-UTC would double-notify anyone discovered shortly
	// before it -- and those land on different UTC days, so Roblox's 1/day cap
	// would not suppress the second. Matters now that this job runs at 20:30 UTC
	// (only ~3.5h before the boundary) instead of the old 05:00.
	const dueValue = nextBoundary(now + DAY_SECONDS);
	const notifBody = JSON.stringify({
		source: { universe: `universes/${UNIVERSE_ID}` },
		payload: { type: "MOMENT", messageId: MESSAGE_ID },
	});

	const known = loadSnapshot();
	const coldStart = known.size === 0;
	console.log(
		`${WRITE ? "WRITE" : "DRY RUN"} — due value = ${dueValue}. ` +
		(coldStart
			? "COLD START: no snapshot -> learning the keyspace, NOT probing the backlog (the full rescan owns that)."
			: `Snapshot has ${known.size} known keys.`)
	);

	let listed = 0, newFound = 0, probed = 0, optedIn = 0, notOptedIn = 0, deferred = 0;
	let pageToken = null, pages = 0;

	do {
		const url = new URL(`${DS_BASE}/universes/${UNIVERSE_ID}/data-stores/${DAILY_STORE}/entries`);
		url.searchParams.set("maxPageSize", "256");
		if (pageToken) url.searchParams.set("pageToken", pageToken);

		const res = await fetchRetry(url, { headers: { "x-api-key": API_KEY } }, `list p${pages}`);
		if (!res.ok) {
			console.error(`List failed: HTTP ${res.status} ${await res.text()}`);
			if (res.status === 403) console.error("-> Key is missing 'Data Stores: Read/list' permission.");
			// Save what we've learned so far so the run wasn't wasted, then fail.
			saveSnapshot(known);
			process.exit(1);
		}

		const data = await res.json();
		const entries = data.dataStoreEntries || data.entries || data.keys || [];

		for (const e of entries) {
			let id = typeof e === "string" ? e : (e.id || e.key || e.path || "");
			id = id.split("/").pop(); // strip "global/" scope prefix
			if (!/^u\d+$/.test(id)) continue;
			listed++;

			if (known.has(id)) continue; // already seen -> not new
			newFound++;

			// Cold start (or DRY RUN): just learn the key, don't send.
			if (coldStart || !WRITE) { if (coldStart) known.add(id); continue; }

			// Safety cap: leave extras for tomorrow (do NOT add to snapshot).
			if (probed >= MAX_NEW_PROBE) { deferred++; continue; }

			const userId = id.slice(1);
			const result = await sendNotification(userId, notifBody);
			if (result === "sent" || result === "dailycap") {
				await upsert(id, dueValue);
				optedIn++;
				known.add(id);
			} else if (result === "notoptedin") {
				notOptedIn++;
				known.add(id); // learned; a later opt-in is the slow rescan's job
			} else {
				deferred++; // ratelimited -> don't learn, re-probe next run
			}
			probed++;
			await sleep(SEND_DELAY_MS);
		}

		pageToken = data.nextPageToken;
		pages++;
		if (pages % 25 === 0) {
			console.log(`...${pages} pages, ${listed} listed, ${newFound} new, ${probed} probed, ${optedIn} opted-in`);
			saveSnapshot(known);
		}

		if (MAX_RUNTIME_MS && pageToken && Date.now() - startMs > MAX_RUNTIME_MS) {
			saveSnapshot(known);
			const mins = Math.round((Date.now() - startMs) / 60000);
			console.log(`Time budget reached (${mins} min) at page ${pages}. Snapshot saved; next run re-lists from the top.`);
			return;
		}

		if (pageToken) await sleep(LIST_DELAY_MS);
	} while (pageToken);

	saveSnapshot(known);
	console.log(
		`Done. pages=${pages} listed=${listed} new=${newFound} probed=${probed} ` +
		`optedIn=${optedIn} notOptedIn=${notOptedIn} deferred=${deferred} snapshot=${known.size}`
	);
	if (!WRITE) console.log(`DRY RUN — nothing sent or seeded. ${newFound} keys would be probed on a --write run.`);
}

main().catch((e) => { console.error(e); process.exit(1); });
