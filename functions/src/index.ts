import { onSchedule } from "firebase-functions/v2/scheduler";
import * as admin from "firebase-admin";

if (!admin.apps.length) admin.initializeApp();
const db = admin.firestore();

const ACTIVE_DAYS_ET = [4,5,6,7,11,12,13,14]; // Sep 2025 (group + playoffs)

// 07:00 ET lock (11:00 UTC in September).
function computeLockUTC(dateKey: string): Date {
  const y = Number(dateKey.slice(0, 4));
  const m = Number(dateKey.slice(4, 6)) - 1;
  const d = Number(dateKey.slice(6, 8));
  return new Date(Date.UTC(y, m, d, 11, 0, 0));
}

function todayDateKeyET(now = new Date()): string {
  // Convert "now" to ET (UTC-4 in September).
  const et = new Date(now.getTime() - 4 * 3600 * 1000);
  const y = et.getUTCFullYear();
  const m = String(et.getUTCMonth() + 1).padStart(2, "0");
  const d = String(et.getUTCDate()).padStart(2, "0");
  return `${y}${m}${d}`;
}

function isActiveDay(dateKey: string): boolean {
  const y = Number(dateKey.slice(0,4));
  const m = Number(dateKey.slice(4,6));
  const d = Number(dateKey.slice(6,8));
  return y === 2025 && m === 9 && ACTIVE_DAYS_ET.includes(d);
}

/**
 * Locks lineups once the hour crosses the daily lock.
 * Runs hourly; safe to re-run (idempotent).
 */
export const lockLineupsHourly = onSchedule("0 * * * *", async () => {
  const dk = todayDateKeyET();
  if (!isActiveDay(dk)) return;

  const lockAt = computeLockUTC(dk).getTime();
  const now = Date.now();
  if (now < lockAt) return; // not time yet

  // Mark all docs for this tid/day as locked (merge, idempotent).
  const tid = "ti2025";
  const q = db.collection("lineups")
    .where("tid", "==", tid)
    .where("dateKey", "==", dk)
    .where("locked", "==", false);

  const snap = await q.get();
  const batch = db.batch();
  snap.forEach(s => batch.set(s.ref, {
    locked: true,
    lockedAt: admin.firestore.FieldValue.serverTimestamp()
  }, { merge: true }));
  if (!snap.empty) await batch.commit();

  // Optional: write a small marker document for visibility / debugging.
  await db.collection("locks").doc(`${tid}_${dk}`).set({
    tid, dateKey: dk, lockedAt: admin.firestore.FieldValue.serverTimestamp()
  }, { merge: true });
});
