// ONE-TIME BACKFILL v2 — reach every opted-in player, not just claimers.
//
// v1 blindly wrote every DailyRewards_v2 key into ReminderQueue_v1 and let the
// worker discover/prune non-opted-in over days. That broke down at scale:
//   - Real player count turned out to be ~170k+ (not the ~40k first assumed).
//   - The Ordered Data Store WRITE endpoint has a much stricter rate limit
//     (~1/sec) than the notification-SEND endpoint (which tolerated hundreds
//     in minutes with zero throttling in earlier testing) -- so writing all
//     170k blind was hammering the slow endpoint for ~90% users who'd just
//     get pruned later anyway.
//
// v2 flips the order: attempt a REAL notification send first (the only way to
// discover opt-in status), and only write to the queue the users who succeed
// (or hit the "already sent today" cap, which still means opted-in). That
// makes the slow write endpoint only touch the ~15k actually-opted-in users
// instead of all ~170k, and doubles as sending everyone's first reminder now.
//
// IMPORTANT: running --write sends REAL notifications immediately to every
// currently opted-in player as part of testing. This is a deliberate one-time
// "kick off the daily cycle now," not a silent seed.
//
// PREREQUISITES:
//   1. API key needs "Data Stores: Read/list" (for listing DailyRewards_v2)
//      AND "Notifications: Write" + "Ordered Data Stores: Write" (already set).
//   2. Set ROBLOX_API_KEY in the environment.
//
// USAGE:
//   node backfill.js            -> DRY RUN: counts users, sends/writes nothing.
//   node backfill.js --write    -> sends real notifications + seeds opted-in.
//   node backfill.js --write --restart   -> ignore any saved checkpoint, start over.
//
// Progress is checkpointed to backfill-progress.json every page, so an
// interrupted run (Ctrl+C, crash, closed terminal) can resume where it left
// off with the same command instead of starting over.

const fs = require("fs");

const API_KEY     = process.env.ROBLOX_API_KEY;
const UNIVERSE_ID = process.env.UNIVERSE_ID || "6033574667";
const DAILY_STORE = "DailyRewards_v2";
const ODS_NAME    = "ReminderQueue_v1";
const MESSAGE_ID  = process.env.MESSAGE_ID || "7f7141f1-d906-504e-85f5-e235b5b91f25";
const RESET_OFFSET_HOURS = parseInt(process.env.RESET_OFFSET_HOURS || "0", 10);

const DAY_SECONDS = 86400;
const WRITE   = process.argv.includes("--write");
const RESTART = process.argv.includes("--restart");
const CHECKPOINT_FILE = require("path").join(__dirname, "backfill-progress.json");

// Optional wall-clock budget (minutes). Default 0 = unlimited, so local runs are
// unaffected. In CI we set this BELOW the runner's job timeout so the script saves
// its checkpoint and exits 0 on its own -- a graceful exit lets actions/cache run
// its post-step save, whereas a hard timeout-kill (cancellation) would skip it.
const MAX_RUNTIME_MS = parseInt(process.env.BACKFILL_MAX_MINUTES || "0", 10) * 60_000;

const SEND_DELAY_MS  = 60;  // notification-send endpoint tolerated much higher throughput in testing
const WRITE_DELAY_MS = 900; // queue-write endpoint's real limit is ~1/sec -- but we now only hit this for opted-in users
const LIST_DELAY_MS  = 250;
const QUEUE_LIST_DELAY_MS = 200; // ODS list endpoint 429s if paged too fast

// ACTIVITY FILTER (2026-11-19). Every probe is a REAL notification, and ~95% of
// the ~525k keys belong to players who left long ago. So before probing, READ the
// player's DailyRewards_v2 entry (a read sends nothing) and skip anyone whose
// last write (revisionCreateTime) is older than ACTIVE_WITHIN_DAYS. Matches the
// worker's sunset: the worker drops players idle 30+ days, and this rescan is how
// they get back in once they play again. Unknown last-active (read failed) ->
// probe as before (fail-open), counted as `unknownActivity` so a missing API
// permission shows up in the log instead of silently changing behaviour.
const ACTIVE_WITHIN_DAYS = parseInt(process.env.ACTIVE_WITHIN_DAYS || "30", 10);
const READ_CONC = 8; // parallel entry reads; fetchRetry backs off on 429

const DS_BASE   = "https://apis.roblox.com/cloud/v2";
const ODS_BASE  = "https://apis.roblox.com/ordered-data-stores/v1";
const NOTIF_URL = (userId) => `https://apis.roblox.com/cloud/v2/users/${userId}/notifications`;

if (!API_KEY) { console.error("ROBLOX_API_KEY is not set"); process.exit(1); }

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function loadCheckpoint() {
	if (RESTART || !fs.existsSync(CHECKPOINT_FILE)) return null;
	try {
		const cp = JSON.parse(fs.readFileSync(CHECKPOINT_FILE, "utf8"));
		// A completed scan writes {done:true} instead of deleting the file, because
		// GitHub Actions caches can't be deleted from inside a run -- a deleted file
		// just gets re-restored stale, freezing the scan at the finish line forever
		// (every subsequent run "completes" in minutes without re-probing anyone).
		// Seeing the done-marker means: start a genuinely fresh full scan.
		if (cp && cp.done) return null;
		return cp;
	} catch { return null; }
}
function saveCheckpoint(state) {
	fs.writeFileSync(CHECKPOINT_FILE, JSON.stringify(state));
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

// Load every key currently in ReminderQueue_v1 so the scan can SKIP users who are
// already in the daily cycle.
//
// WHY (found 2026-07-26): probing a user sends them a REAL notification -- that is
// the only way to discover opt-in -- and Roblox allows just ONE notification per
// user per day. This rescan runs ~20h/day, so it was consuming queued users' daily
// quota at random hours, and the worker's scheduled prime-time send then bounced
// with RESOURCE_EXHAUSTED. Observed directly: a rescan ended 23:53 UTC, and the
// worker's 00:04 UTC run logged `sent=17 dailyCap=183` -- it was trying to notify
// people the rescan had just notified minutes earlier.
//
// Queued users never needed re-probing here: the worker already re-verifies each
// of them daily and prunes the genuinely lapsed (glitch-safe). This rescan's only
// unique job is finding opt-ins among users NOT yet in the queue.
//
// Best-effort: if the listing fails partway we proceed with whatever we collected
// (skipping fewer users just restores some interference, it never loses anyone).
async function loadQueueMembers() {
	const members = new Set();
	let pageToken = null, pages = 0;
	while (pages < 5000) {
		pages++;
		const url = new URL(
			`${ODS_BASE}/universes/${UNIVERSE_ID}/orderedDataStores/${ODS_NAME}/scopes/global/entries`
		);
		url.searchParams.set("max_page_size", "100");
		url.searchParams.set("order_by", "asc");
		if (pageToken) url.searchParams.set("page_token", pageToken);

		const res = await fetchRetry(url, { headers: { "x-api-key": API_KEY } }, `queue p${pages}`);
		if (!res.ok) {
			console.warn(`queue list: HTTP ${res.status} at page ${pages}; continuing with ${members.size} loaded`);
			break;
		}
		const data = await res.json();
		const ents = data.entries || [];
		for (const e of ents) if (e.id) members.add(e.id);
		pageToken = data.nextPageToken;
		if (!pageToken || ents.length === 0) break;
		await sleep(QUEUE_LIST_DELAY_MS);
	}
	return members;
}

// Real send attempt -- the only way to discover opt-in status. Mirrors the
// worker's classification: sent / dailycap (opted in, capped for today) /
// notoptedin (skip) / ratelimited (retry).
//
// Does NOT use the generic fetchRetry -- a 429 here can mean two very
// different things and they must be told apart on the FIRST response, not
// after blind exponential backoff:
//   RESOURCE_EXHAUSTED = "1 notification per recipient per day" already used.
//     This is a permanent-for-today outcome (the live worker's own cron has
//     been running the whole time and may have already notified this user
//     today). Retrying wastes minutes per user for a guaranteed-same result.
//   anything else = a genuine transient throttle, worth a short backoff.
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
			if (code === "RESOURCE_EXHAUSTED") return "dailycap"; // no retry -- won't change today

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

// Unix seconds of the player's last DailyRewards_v2 write, or null if unknown.
async function lastActive(entryId) {
	try {
		const res = await fetchRetry(
			`${DS_BASE}/universes/${UNIVERSE_ID}/data-stores/${DAILY_STORE}/entries/${entryId}`,
			{ headers: { "x-api-key": API_KEY } },
			`read ${entryId}`
		);
		if (!res.ok) return null;
		const d = await res.json();
		const t = Date.parse(d.revisionCreateTime || d.createTime || "");
		return Number.isNaN(t) ? null : Math.floor(t / 1000);
	} catch { return null; }
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
	const dueValue = nextBoundary(now);
	const notifBody = JSON.stringify({
		source: { universe: `universes/${UNIVERSE_ID}` },
		payload: { type: "MOMENT", messageId: MESSAGE_ID },
	});

	const cp = loadCheckpoint();
	let pageToken = cp?.pageToken ?? null;
	let pages     = cp?.pages ?? 0;
	let scanned   = cp?.scanned ?? 0;
	let optedIn   = cp?.optedIn ?? 0;
	let notOptedIn = cp?.notOptedIn ?? 0;
	let skippedQueued = cp?.skippedQueued ?? 0;
	let skippedIdle = cp?.skippedIdle ?? 0;
	let unknownActivity = cp?.unknownActivity ?? 0;

	console.log(`${WRITE ? "WRITE (sends real notifications now)" : "DRY RUN"} — due value = ${dueValue}`);
	if (cp) console.log(`Resuming from checkpoint: page ${pages}, ${scanned} scanned so far`);

	// Users already in the daily cycle are skipped (see loadQueueMembers). Rebuilt
	// each run -- it's ~1min for ~26k and keeps a resumed run correct.
	const queued = WRITE ? await loadQueueMembers() : new Set();
	if (WRITE) console.log(`Queue membership: ${queued.size} users already in the daily cycle -> will be skipped (not re-notified).`);

	do {
		const url = new URL(`${DS_BASE}/universes/${UNIVERSE_ID}/data-stores/${DAILY_STORE}/entries`);
		url.searchParams.set("maxPageSize", "256");
		if (pageToken) url.searchParams.set("pageToken", pageToken);

		const res = await fetchRetry(url, { headers: { "x-api-key": API_KEY } }, `list p${pages}`);
		if (!res.ok) {
			console.error(`List failed: HTTP ${res.status} ${await res.text()}`);
			if (res.status === 403) console.error("-> Key is missing 'Data Stores: Read/list' permission.");
			// A resumed run can fail here on a stale/expired page_token. Drop the
			// checkpoint so the NEXT run starts a clean scan instead of retrying the
			// same bad token forever (permanently stuck).
			if (fs.existsSync(CHECKPOINT_FILE)) {
				fs.unlinkSync(CHECKPOINT_FILE);
				console.error("-> Cleared checkpoint; next run will restart the scan from the top.");
			}
			process.exit(1);
		}

		const data = await res.json();
		const entries = data.dataStoreEntries || data.entries || data.keys || [];

		// Pass 1: ids on this page, minus those already in the daily cycle (the
		// worker owns them; probing would burn their 1-per-day quota, see
		// loadQueueMembers).
		const candidates = [];
		for (const e of entries) {
			let id = typeof e === "string" ? e : (e.id || e.key || "");
			id = id.split("/").pop(); // strip "global/" scope prefix
			if (!/^u\d+$/.test(id)) continue;
			scanned++;
			if (!WRITE) continue;
			if (queued.has(id)) { skippedQueued++; continue; }
			candidates.push(id);
		}

		// Pass 2: activity filter (reads only, parallel). Long-gone players are
		// skipped without a notification.
		const toProbe = [];
		const cutoff = Math.floor(Date.now() / 1000) - ACTIVE_WITHIN_DAYS * DAY_SECONDS;
		for (let i = 0; i < candidates.length; i += READ_CONC) {
			const batch = candidates.slice(i, i + READ_CONC);
			const times = await Promise.all(batch.map(lastActive));
			batch.forEach((id, k) => {
				const t = times[k];
				if (t === null) { unknownActivity++; toProbe.push(id); }
				else if (t < cutoff) skippedIdle++;
				else toProbe.push(id);
			});
		}

		// Pass 3: probe the recently-active, sequentially as before.
		for (const id of toProbe) {
			{
				const userId = id.slice(1); // strip leading "u"
				const result = await sendNotification(userId, notifBody);

				if (result === "sent" || result === "dailycap") {
					await upsert(id, dueValue);
					optedIn++;
				} else if (result === "notoptedin") {
					notOptedIn++;
				}
				// "ratelimited" already retried internally by fetchRetry; if it still
				// failed after 8 attempts it falls through uncounted -- rare, safe to
				// just skip this user this pass, they'll be caught by a future rescan.

				await sleep(SEND_DELAY_MS);
			}
		}

		pageToken = data.nextPageToken;
		pages++;
		if (pages % 5 === 0) {
			console.log(`...${pages} pages, ${scanned} scanned, ${optedIn} opted-in, ${notOptedIn} not opted-in, ${skippedQueued} skipped(already queued), ${skippedIdle} skipped(idle >${ACTIVE_WITHIN_DAYS}d), ${unknownActivity} unknown-activity`);
			saveCheckpoint({ pageToken, pages, scanned, optedIn, notOptedIn, skippedQueued, skippedIdle, unknownActivity });
		}

		// Wall-clock budget: stop BEFORE the CI job timeout so we exit cleanly with
		// an up-to-date checkpoint. Only bail when there's more to do (pageToken set)
		// -- on the final page we'd rather just finish and clear the checkpoint below.
		if (MAX_RUNTIME_MS && pageToken && Date.now() - startMs > MAX_RUNTIME_MS) {
			saveCheckpoint({ pageToken, pages, scanned, optedIn, notOptedIn, skippedQueued, skippedIdle, unknownActivity });
			const mins = Math.round((Date.now() - startMs) / 60000);
			console.log(`Time budget reached (${mins} min). Saved checkpoint at page ${pages}, ${scanned} scanned. Exiting 0 to resume next run.`);
			return; // graceful -> do NOT delete the checkpoint; the next run resumes here
		}

		if (pageToken) await sleep(LIST_DELAY_MS);
	} while (pageToken);

	console.log(`Done. pages=${pages} scanned=${scanned} optedIn=${optedIn} notOptedIn=${notOptedIn} skippedQueued=${skippedQueued} skippedIdle=${skippedIdle} unknownActivity=${unknownActivity}`);
	// Mark completion (do NOT just delete -- the Actions cache would re-restore the
	// old checkpoint and freeze the scan). {done:true} tells the next run to restart.
	saveCheckpoint({ done: true });

	if (!WRITE) {
		console.log(`\nDRY RUN complete — nothing was sent or written.`);
		console.log(`Re-run with --write to send real notifications and seed the ${scanned} candidates`);
		console.log(`(only those who are actually opted in get written to the queue).`);
	}
}

main().catch((e) => { console.error(e); process.exit(1); });
