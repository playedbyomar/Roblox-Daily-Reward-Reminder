// reminder-worker.js
// Checks the "ReminderQueue_v1" OrderedDataStore for players whose 24h
// daily-reward cooldown has just expired, and sends them an Experience
// Notification via Open Cloud. Designed to be run on a schedule
// (GitHub Actions, every ~15 minutes) — not as a long-running process.

const UNIVERSE_ID = process.env.UNIVERSE_ID;
const NOTIFICATION_ASSET_ID = process.env.NOTIFICATION_ASSET_ID;
const API_KEY = process.env.ROBLOX_API_KEY;

const BASE_URL = "https://apis.roblox.com/cloud/v2";
const ORDERED_STORE_ID = "ReminderQueue_v1";
const SCOPE = "global";

if (!UNIVERSE_ID || !NOTIFICATION_ASSET_ID || !API_KEY) {
	console.error("Missing required env vars: UNIVERSE_ID, NOTIFICATION_ASSET_ID, ROBLOX_API_KEY");
	process.exit(1);
}

function headers() {
	return { "x-api-key": API_KEY, "Content-Type": "application/json" };
}

// Pull entries sorted ascending by due-time (the value we stored),
// so the earliest-due players show up first. We page through and stop
// once we reach an entry that isn't due yet (everything after it in
// ascending order won't be due either).
async function getDueEntries() {
	const now = Math.floor(Date.now() / 1000);
	const due = [];
	let pageToken = "";

	do {
		const url = new URL(
			`${BASE_URL}/universes/${UNIVERSE_ID}/ordered-data-stores/${ORDERED_STORE_ID}/scopes/${SCOPE}/entries`
		);
		url.searchParams.set("max_page_size", "100");
		url.searchParams.set("order_by", "value");
		if (pageToken) url.searchParams.set("page_token", pageToken);

		const res = await fetch(url, { headers: headers() });
		if (!res.ok) {
			console.error("Failed to list entries:", res.status, await res.text());
			break;
		}
		const body = await res.json();
		const entries = body.entries || [];

		for (const entry of entries) {
			const value = Number(entry.value);
			if (value <= now) {
				due.push(entry);
			} else {
				// Ascending order: nothing further in this page (or later
				// pages) can be due yet either. Stop entirely.
				pageToken = null;
				break;
			}
		}

		pageToken = pageToken === null ? null : body.nextPageToken || null;
	} while (pageToken);

	return due;
}

async function sendReminder(userId) {
	const res = await fetch(`https://apis.roblox.com/cloud/v2/users/${userId}/notifications`, {
		method: "POST",
		headers: headers(),
		body: JSON.stringify({
			source: { universe: `universes/${UNIVERSE_ID}` },
			payload: { message_id: NOTIFICATION_ASSET_ID, type: "MOMENT" },
		}),
	});
	if (!res.ok) {
		console.warn(`Notification send failed for user ${userId}:`, res.status, await res.text());
	}
	return res.ok;
}

async function deleteEntry(entryId) {
	// entryId as returned by List Entries may look like "global/u123..." —
	// use it exactly as returned rather than reconstructing it.
	const url = `${BASE_URL}/universes/${UNIVERSE_ID}/ordered-data-stores/${ORDERED_STORE_ID}/scopes/${SCOPE}/entries/${encodeURIComponent(
		entryId.split("/").pop()
	)}`;
	const res = await fetch(url, { method: "DELETE", headers: headers() });
	if (!res.ok) {
		console.warn(`Failed to delete queue entry ${entryId}:`, res.status, await res.text());
	}
}

async function main() {
	const due = await getDueEntries();
	console.log(`Found ${due.length} due reminder(s).`);

	for (const entry of due) {
		const rawId = entry.id.split("/").pop(); // e.g. "u12345678"
		const userId = rawId.replace(/^u/, "");

		try {
			const sent = await sendReminder(userId);
			console.log(`User ${userId}: ${sent ? "sent" : "attempted (may not be opted in)"}`);
		} catch (err) {
			console.error(`Error sending to user ${userId}:`, err);
		} finally {
			// Always clear the entry so we never re-notify for the same claim,
			// regardless of whether delivery actually happened (we can't know).
			await deleteEntry(entry.id);
		}
	}
}

main().catch((err) => {
	console.error("Worker failed:", err);
	process.exit(1);
});
