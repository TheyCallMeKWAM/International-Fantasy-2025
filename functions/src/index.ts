// functions/src/index.ts (add alongside your existing exports)
import { onCall } from "firebase-functions/v2/https";
import { db } from "./firebaseAdmin.js";

export const submitLineup = onCall(async (req) => {
  const uid = req.auth?.uid;
  if (!uid) throw new Error("auth required");

  const { tid, dateKey, captain, cores, supports, teamCard } = (req.data ?? {});
  if (!tid || !dateKey) throw new Error("tid & dateKey required");

  // fetch tournament config
  const tSnap = await db.doc(`tournaments/${tid}`).get();
  if (!tSnap.exists) throw new Error("tournament not found");
  const cfg = tSnap.data() as any;
  const roles = cfg.roles ?? { cores: 3, supports: 2, maxFromOneOrg: 4 };

  // shape checks
  if (!Number.isInteger(captain)) throw new Error("captain steam32 required");
  if (!Array.isArray(cores) || cores.length !== roles.cores) throw new Error(`need ${roles.cores} cores`);
  if (!Array.isArray(supports) || supports.length !== roles.supports) throw new Error(`need ${roles.supports} supports`);
  if (typeof teamCard !== "string") throw new Error("teamCard orgId required");

  // OPTIONAL: enforce maxFromOneOrg if you store an org map per player (skip if you don’t have it yet)

  // respect lock: if current UTC hour >= lockHourUTC, block
  const now = new Date();
  const locked = now.getUTCHours() >= (cfg.lockHourUTC ?? 12);

  const docId = `${tid}_${uid}_${dateKey}`;
  await db.doc(`lineups/${docId}`).set({
    tid, dateKey, mid: uid,
    captain, cores, supports, teamCard,
    locked
  }, { merge: true });

  return { ok: true, locked };
});
