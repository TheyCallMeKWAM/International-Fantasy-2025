/* International Fantasy 2025 — Cloud Functions (TypeScript, v2 API)
 * Locks at 3:00 AM ET (08:00 UTC). Override per tournament via tournaments/{tid}.lockHourUTC (UTC hour).
 * Polls OpenDota every 45 minutes, caches matches in Firestore, and scores only when a match becomes COMPLETE.
 */
import admin from "firebase-admin";
import { onCall, HttpsError, type CallableRequest } from "firebase-functions/v2/https";
import { onSchedule, type ScheduledEvent } from "firebase-functions/v2/scheduler";

// ---------- Admin init ----------
const app = admin.apps.length ? admin.app() : admin.initializeApp();
const db = app.firestore();

// ---------- Types ----------
type Tid = string;
type DateKey = string; // YYYYMMDD

interface TournamentDoc {
  name?: string;
  leagueIds?: number[];
  lockHourUTC?: number; // default 08:00 UTC (3am ET)
}

// Slim player/objective shapes — only what we score on.
interface PlayerSlim {
  account_id: number;
  player_slot: number;
  kills: number;
  deaths: number;
  assists: number;
  last_hits: number;
  denies: number;
  obs_placed: number;
  camps_stacked: number;
  roshans_killed?: number;
  roshan_kills?: number;
}

type ObjectiveType = "CHAT_MESSAGE_ROSHAN_KILL" | "CHAT_MESSAGE_FIRST_BLOOD";
interface ObjectiveSlim {
  type: ObjectiveType;
  player_slot: number;
  time?: number;
}

interface MatchDoc {
  match_id: number;
  series_id: number;
  series_type: number;
  radiant_team_id: number;
  dire_team_id: number;
  radiant_win: boolean;
  duration: number;
  tower_status_radiant: number;
  tower_status_dire: number;
  barracks_status_radiant: number;
  barracks_status_dire: number;
  objectives: ObjectiveSlim[];   // SLIM
  players: PlayerSlim[];         // SLIM
  start_time: number;
  radiant_name?: string;
  dire_name?: string;
  tid: Tid;
  dateKey: DateKey;
  complete?: boolean;
  updatedAt?: admin.firestore.FieldValue;
}

interface LineupDoc {
  tid: Tid;
  dateKey: DateKey;
  ownerUid: string;
  mid: string; // (legacy) user id key
  managerName?: string | null;
  captain: string;
  cores: string[];
  supports: string[];
  teamCard: string;
  locked?: boolean;
  updatedAt?: admin.firestore.FieldValue;
}

/** NEW: Roster breakdown for leaderboard */
type RosterItem =
  | { role: "Captain"; steam32: string; points: number }
  | { role: "Core";    steam32: string; points: number }
  | { role: "Support"; steam32: string; points: number }
  | { role: "Team";    teamId: string;  name?: string; points: number };

interface LeaderboardEntry { mid: string; managerName?: string | null; totalPoints: number; roster: RosterItem[]; }
interface LeaderboardDoc { entries: LeaderboardEntry[]; }

// ---------- Scoring config ----------
const SCORING = {
  players: {
    kill: 3, assist: 2, death: -1,
    lastHits: 0.02, denies: 0.02,
    wardsPlaced: 0.2, campsStacked: 0.5,
    winUnder25: 15, kaOver20: 2, winGame: 15, sweep: 30,
  },
  team: { towers: 1, barracks: 1, roshans: 3, firstBlood: 2, teamWin: 2, sweep: 15 },
  captainMultiplier: 1.5,
};

// Default lock is 3:00 AM ET = 08:00 UTC
const DEFAULT_LOCK_HOUR_UTC = 8;

// ---------- Utils ----------
const toInt = (v: unknown) => Number.parseInt(String(v ?? 0), 10) || 0;
const yyyymmddFromUTC = (d: Date): DateKey =>
  `${d.getUTCFullYear()}${String(d.getUTCMonth() + 1).padStart(2, "0")}${String(d.getUTCDate()).padStart(2, "0")}`;

async function getTournament(tid: Tid): Promise<{ leagueIds: number[]; lockHourUTC: number; name: string }> {
  const snap = await db.collection("tournaments").doc(tid).get();
  const t = (snap.exists ? (snap.data() as TournamentDoc) : {}) || {};
  const leagueIds = Array.isArray(t.leagueIds) ? t.leagueIds.map(n => Number(n)) : [];
  const lockHourUTC = Number.isFinite(t.lockHourUTC) ? Number(t.lockHourUTC) : DEFAULT_LOCK_HOUR_UTC;
  const name = String(t.name || tid);
  return { leagueIds, lockHourUTC, name };
}

function lockTimestamp(dateKey: DateKey, lockHourUTC: number = DEFAULT_LOCK_HOUR_UTC): number {
  const y = Number(dateKey.slice(0, 4));
  const m = Number(dateKey.slice(4, 6)) - 1;
  const d = Number(dateKey.slice(6, 8));
  return Date.UTC(y, m, d, lockHourUTC, 0, 0);
}

const bitCount = (n: number, bits = 32) => { n = toInt(n); let c = 0; for (let i = 0; i < bits; i++) if ((n >> i) & 1) c++; return c; };
const towersDestroyedByRadiant = (m: any) => 11 - bitCount(m.tower_status_dire, 11);
const towersDestroyedByDire    = (m: any) => 11 - bitCount(m.tower_status_radiant, 11);
const barracksDestroyedByRadiant = (m: any) => 6 - bitCount(m.barracks_status_dire, 6);
const barracksDestroyedByDire    = (m: any) => 6 - bitCount(m.barracks_status_radiant, 6);

function roshansByTeam(m: any) {
  const out = { radiant: 0, dire: 0 };

  const objs: any[] = Array.isArray(m?.objectives) ? m.objectives : [];
  let sawObjective = false;
  for (const o of objs) {
    if (o?.type === "CHAT_MESSAGE_ROSHAN_KILL") {
      const slot = toInt(o?.player_slot);
      if (slot < 128) out.radiant += 1;
      else out.dire += 1;
      sawObjective = true;
    }
  }
  if (sawObjective) return out;

  const players: any[] = Array.isArray(m?.players) ? m.players : [];
  for (const p of players) {
    const rk = toInt(p?.roshans_killed ?? p?.roshan_kills ?? 0);
    if (rk > 0) {
      const slot = toInt(p?.player_slot);
      if (slot < 128) out.radiant += rk;
      else out.dire += rk;
    }
  }
  return out;
}

function firstBloodTeam(m: any): "radiant" | "dire" | null {
  const objs: ObjectiveSlim[] = Array.isArray(m.objectives) ? m.objectives : [];
  const fb = objs.find(o => o?.type === "CHAT_MESSAGE_FIRST_BLOOD");
  if (!fb) return null;
  return toInt(fb.player_slot) < 128 ? "radiant" : "dire";
}

function summarizePlayers(m: any) {
  const arr: PlayerSlim[] = Array.isArray(m.players) ? m.players : [];
  const map = new Map<string, any>();
  for (const p of arr) {
    const id = String(p?.account_id ?? "");
    if (!id) continue;
    map.set(id, {
      id,
      kills: toInt(p.kills), deaths: toInt(p.deaths), assists: toInt(p.assists),
      last_hits: toInt(p.last_hits), denies: toInt(p.denies),
      obs_placed: toInt(p.obs_placed), camps_stacked: toInt(p.camps_stacked),
      roshans_killed: toInt((p as any).roshans_killed ?? (p as any).roshan_kills ?? 0),
      player_slot: toInt(p.player_slot), isRadiant: toInt(p.player_slot) < 128,
    });
  }
  return map;
}
function seriesWinsForDay(matches: any[]) {
  const map = new Map<number, { radiant: { team_id: number; wins: number }, dire: { team_id: number; wins: number } }>();
  for (const m of matches) {
    const sid = m.series_id || m.match_id;
    if (!map.has(sid)) map.set(sid, { radiant: { team_id: m.radiant_team_id, wins: 0 }, dire: { team_id: m.dire_team_id, wins: 0 } });
    if (m.radiant_win) map.get(sid)!.radiant.wins++; else map.get(sid)!.dire.wins++;
  }
  return map;
}
const teamKey = (x: unknown) => String(x ?? "").trim().toLowerCase().replace(/\s+/g, "_");
function isRadiantForTeam(match: any, teamSel: string | number | null | undefined): boolean | null {
  if (!teamSel) return null;
  if (/^\d+$/.test(String(teamSel))) {
    const id = toInt(teamSel);
    if (toInt(match.radiant_team_id) === id) return true;
    if (toInt(match.dire_team_id) === id) return false;
  }
  const rName = teamKey(match.radiant_name || ""), dName = teamKey(match.dire_name || ""), sel = teamKey(teamSel);
  if (rName && sel && rName === sel) return true;
  if (dName && sel && dName === sel) return false;
  return null;
}

// ---------- OpenDota ----------
async function fetchJSON(url: string): Promise<any> {
  const res = await (globalThis as any).fetch(url, { headers: { "User-Agent": "international-fantasy-2025" } });
  if (!res?.ok) throw new Error(`HTTP ${res?.status} for ${url}`);
  return res.json();
}
const fetchLeagueMatches = (id: number) => fetchJSON(`https://api.opendota.com/api/leagues/${id}/matches`);
const fetchMatchDetail   = (id: number) => fetchJSON(`https://api.opendota.com/api/matches/${id}`);

// ---------- Cache helpers ----------
const sleep = (ms: number) => new Promise(res => setTimeout(res, ms));

function looksComplete(m: any): boolean {
  if (!m) return false;
  const hasPlayers = Array.isArray(m.players) && m.players.length === 10;
  const hasDuration = toInt(m.duration) > 0;
  // OpenDota sometimes omits objectives but players/duration suffice for fantasy stats
  return !!(hasPlayers && hasDuration);
}

// NEW: Project OpenDota match -> slim shape (only what we score on)
function toSlimMatch(tid: Tid, match: any): MatchDoc {
  const startSecs = toInt(match.start_time ?? match.startTime);
  const dateKey = yyyymmddFromUTC(new Date(startSecs * 1000));

  const objectives: ObjectiveSlim[] = Array.isArray(match.objectives)
    ? match.objectives
        .filter((o: any) => o && (o.type === "CHAT_MESSAGE_ROSHAN_KILL" || o.type === "CHAT_MESSAGE_FIRST_BLOOD"))
        .map((o: any) => ({
          type: o.type as ObjectiveType,
          player_slot: toInt(o.player_slot),
          time: toInt(o.time),
        }))
    : [];

  const players: PlayerSlim[] = Array.isArray(match.players)
    ? match.players.map((p: any) => ({
        account_id: toInt(p.account_id),
        player_slot: toInt(p.player_slot),
        kills: toInt(p.kills),
        deaths: toInt(p.deaths),
        assists: toInt(p.assists),
        last_hits: toInt(p.last_hits),
        denies: toInt(p.denies),
        obs_placed: toInt(p.obs_placed),
        camps_stacked: toInt(p.camps_stacked),
      }))
    : [];

  return {
    match_id: toInt(match.match_id ?? match.matchId),
    series_id: toInt(match.series_id ?? match.seriesId),
    series_type: toInt(match.series_type ?? match.seriesType),
    radiant_team_id: toInt(match.radiant_team_id),
    dire_team_id: toInt(match.dire_team_id),
    radiant_win: !!match.radiant_win,
    duration: toInt(match.duration),
    tower_status_radiant: toInt(match.tower_status_radiant),
    tower_status_dire: toInt(match.tower_status_dire),
    barracks_status_radiant: toInt(match.barracks_status_radiant),
    barracks_status_dire: toInt(match.barracks_status_dire),
    objectives,
    players,
    start_time: startSecs,
    radiant_name: String(match.radiant_name || ""),
    dire_name: String(match.dire_name || ""),
    tid,
    dateKey,
    complete: looksComplete({ players, duration: toInt(match.duration) }),
    updatedAt: admin.firestore.FieldValue.serverTimestamp(),
  };
}

// EDITED: Upsert uses slim projection
async function upsertMatchDoc(tid: Tid, match: any) {
  const doc = toSlimMatch(tid, match);
  await db.collection("matches").doc(String(doc.match_id)).set(doc, { merge: true });
  return { dateKey: doc.dateKey, match_id: doc.match_id, complete: !!doc.complete };
}

// ---------- Scoring ----------
function scorePlayerInMatch(steam32: string | number, m: any): number {
  const row = summarizePlayers(m).get(String(steam32));
  if (!row) return 0;
  const isWin = (row.isRadiant && m.radiant_win) || (!row.isRadiant && !m.radiant_win);
  let pts = 0;
  pts += row.kills * SCORING.players.kill + row.assists * SCORING.players.assist + row.deaths * SCORING.players.death;
  pts += row.last_hits * SCORING.players.lastHits + row.denies * SCORING.players.denies;
  pts += row.obs_placed * SCORING.players.wardsPlaced + row.camps_stacked * SCORING.players.campsStacked;
  if (isWin && toInt(m.duration) < 25 * 60) pts += SCORING.players.winUnder25;
  if (row.kills + row.assists >= 20) pts += SCORING.players.kaOver20;
  if (isWin) pts += SCORING.players.winGame;
  return pts;
}
function scoreTeamCardInMatch(teamId: string | number, m: any): number {
  const side = isRadiantForTeam(m, teamId);
  if (side == null) return 0;
  let pts = 0;
  pts += (side ? towersDestroyedByRadiant(m) : towersDestroyedByDire(m)) * SCORING.team.towers;
  pts += (side ? barracksDestroyedByRadiant(m) : barracksDestroyedByDire(m)) * SCORING.team.barracks;
  const ros = roshansByTeam(m);
  pts += (side ? ros.radiant : ros.dire) * SCORING.team.roshans;
  const fb = firstBloodTeam(m);
  if ((fb === "radiant" && side) || (fb === "dire" && !side)) pts += SCORING.team.firstBlood;
  if ((side && m.radiant_win) || (!side && !m.dire_win)) /* m.dire_win not in slim; use !radiant_win */ { /* noop */ }
  if ((side && m.radiant_win) || (!side && !m.radiant_win)) pts += SCORING.team.teamWin;
  return pts;
}

function addSweepBonuses(lineup: LineupDoc, matches: any[]) {
  const series = seriesWinsForDay(matches);
  const teamId = lineup.teamCard ? toInt(lineup.teamCard) : null;
  const bySeries = new Map<number, any[]>();
  for (const m of matches) {
    const sid = m.series_id || m.match_id;
    if (!bySeries.has(sid)) bySeries.set(sid, []);
    bySeries.get(sid)!.push(m);
  }
  const playerSweeps = new Map<string, number>(); let teamSweep = 0;
  for (const [sid, pack] of bySeries.entries()) {
    const w = series.get(sid); if (!w) continue;
    let sweepTeamId: number | null = null;
    if (w.radiant.wins >= 2 && w.dire.wins === 0) sweepTeamId = w.radiant.team_id;
    if (w.dire.wins >= 2 && w.radiant.wins === 0) sweepTeamId = w.dire.team_id;
    if (!sweepTeamId) continue;

    if (teamId && toInt(teamId) === toInt(sweepTeamId)) teamSweep += SCORING.team.sweep;

    for (const pid of [lineup.captain, ...(lineup.cores || []), ...(lineup.supports || [])].map(String)) {
      const playedSweep = pack.some((m: any) => {
        const row = summarizePlayers(m).get(pid); if (!row) return false;
        const tid = row.isRadiant ? toInt(m.radiant_team_id) : toInt(m.dire_team_id);
        return toInt(tid) === toInt(sweepTeamId);
      });
      if (playedSweep) playerSweeps.set(pid, (playerSweeps.get(pid) || 0) + SCORING.players.sweep);
    }
  }
  return { playerSweeps, teamSweep };
}

/** NEW: resolve pretty team name from teams/{teamId} */
async function resolveTeamPrettyName(teamId: string): Promise<string | undefined> {
  try {
    const snap = await db.collection("teams").doc(teamId).get();
    if (!snap.exists) return undefined;
    const d = snap.data() || {};
    const name = d.name || d.displayName || d.tag;
    return typeof name === "string" && name.trim() ? String(name) : undefined;
  } catch {
    return undefined;
  }
}

// EDITED: read only completed matches to reduce memory
async function scoreDay(tid: Tid, dateKey: DateKey) {
  const mSnap = await db
    .collection("matches")
    .where("tid", "==", tid)
    .where("dateKey", "==", dateKey)
    .where("complete", "==", true)
    .get();

  const matches: any[] = [];
  mSnap.forEach((d: any) => matches.push(d.data()));

  const lSnap = await db.collection("lineups").where("tid", "==", tid).where("dateKey", "==", dateKey).get();
  const lineups: (LineupDoc & { id: string })[] = []; lSnap.forEach((d: any) => lineups.push({ id: d.id, ...(d.data() as LineupDoc) }));

  const entries: LeaderboardEntry[] = [];
  for (const L of lineups) {
    const cap = String(L.captain || "");
    const cores = Array.isArray(L.cores) ? L.cores.map(String) : [];
    const sups  = Array.isArray(L.supports) ? L.supports.map(String) : [];
    const teamCard = String(L.teamCard || "");

    const { playerSweeps, teamSweep } = addSweepBonuses(L, matches);

    // per-slot raw points across matches
    const capRaw = matches.reduce((sum, m) => sum + scorePlayerInMatch(cap, m), 0) + (playerSweeps.get(cap) || 0);
    const coreRaw: number[] = cores.map(pid =>
      matches.reduce((sum, m) => sum + scorePlayerInMatch(pid, m), 0) + (playerSweeps.get(String(pid)) || 0)
    );
    const supRaw: number[] = sups.map(pid =>
      matches.reduce((sum, m) => sum + scorePlayerInMatch(pid, m), 0) + (playerSweeps.get(String(pid)) || 0)
    );
    const teamRaw = matches.reduce((sum, m) => sum + scoreTeamCardInMatch(teamCard, m), 0) + teamSweep;

    // apply captain multiplier to captain slot only
    const capPts = capRaw * (SCORING.captainMultiplier || 1);

    // Build roster breakdown in fixed order (Captain → Cores → Supports → Team)
    const roster: RosterItem[] = [];
    roster.push({ role: "Captain", steam32: cap, points: Number(capPts.toFixed(2)) });
    cores.forEach((pid, i) => roster.push({ role: "Core", steam32: pid, points: Number(coreRaw[i].toFixed(2)) }));
    sups.forEach((pid, i)  => roster.push({ role: "Support", steam32: pid, points: Number(supRaw[i].toFixed(2)) }));
    if (teamCard) {
      const pretty = await resolveTeamPrettyName(teamCard);
      roster.push({ role: "Team", teamId: teamCard, name: pretty, points: Number(teamRaw.toFixed(2)) });
    }

    const total = roster.reduce((sum, r) => sum + (r.points || 0), 0);

    entries.push({
      mid: L.ownerUid || L.mid || "unknown",
      managerName: L.managerName ?? null,
      totalPoints: Number(total.toFixed(2)),
      roster,
    });
  }

  // sort & write
  entries.sort((a, b) => b.totalPoints - a.totalPoints);

  const lbId = `${tid}_${dateKey}`;
  await db.collection("leaderboards").doc(lbId).set({ entries } as LeaderboardDoc, { merge: true });
  return { count: entries.length, id: lbId, entries };
}

// ---------- Callables (v2) ----------
export const submitLineup = onCall(async (request: CallableRequest) => {
  const auth = request.auth; if (!auth) throw new HttpsError("unauthenticated", "Sign in first.");
  const uid = auth.uid!;
  const { tid, dateKey, captain, cores, supports, teamCard, managerName } =
    (request.data || {}) as Partial<LineupDoc> & { tid: Tid; dateKey: DateKey };
  if (!tid || !/^\d{8}$/.test(String(dateKey || ""))) throw new HttpsError("invalid-argument", "Missing tid/dateKey.");

  const t = await getTournament(tid);
  if (Date.now() >= lockTimestamp(String(dateKey), t.lockHourUTC))
    throw new HttpsError("failed-precondition", "Lineups for this day are locked.");

  const docId = `${tid}_${dateKey}_${uid}`;
  const payload: LineupDoc = {
    tid, dateKey, ownerUid: uid, mid: uid,
    managerName: managerName ?? null,
    captain: String(captain || ""),
    cores: Array.isArray(cores) ? cores.map(String) : [],
    supports: Array.isArray(supports) ? supports.map(String) : [],
    teamCard: String(teamCard || ""),
    locked: false,
    updatedAt: admin.firestore.FieldValue.serverTimestamp(),
  };
  await db.collection("lineups").doc(docId).set(payload, { merge: true });
  return { ok: true, id: docId };
});

function assertAdmin(auth: any) {
  const isAdmin = !!auth?.token?.admin;
  if (!isAdmin) throw new HttpsError("permission-denied", "Admins only.");
}

export const adminGetLineup = onCall(async (request: CallableRequest) => {
  assertAdmin(request.auth);
  const { tid, dateKey, user } = request.data as { tid: Tid; dateKey: DateKey; user: string };
  if (!tid || !/^\d{8}$/.test(String(dateKey || "")) || !user)
    throw new HttpsError("invalid-argument", "Provide tid, dateKey, user.");

  let uid: string | null = null, email: string | null = null;
  if (String(user).includes("@")) { try { const rec = await admin.auth().getUserByEmail(String(user)); uid = rec.uid; email = rec.email || null; } catch {} }
  else { uid = String(user); try { const rec = await admin.auth().getUser(uid); email = rec.email || null; } catch {} }
  if (!uid) return { uid: null };

  const docId = `${tid}_${dateKey}_${uid}`;
  const s = await db.collection("lineups").doc(docId).get();
  const L: Partial<LineupDoc> = s.exists ? (s.data() as Partial<LineupDoc>) : {};
  const t = await getTournament(tid);
  const locked = Date.now() >= lockTimestamp(String(dateKey), t.lockHourUTC) || !!(L.locked);

  return { uid, email, locked, captain: L.captain || "", cores: L.cores || [], supports: L.supports || [], teamCard: L.teamCard || "" };
});

export const adminSetLineup = onCall(async (request: CallableRequest) => {
  assertAdmin(request.auth);
  const { tid, dateKey, uid, captain, cores, supports, teamCard, overrideLock } =
    request.data as { tid: Tid; dateKey: DateKey; uid: string; captain: string; cores: string[]; supports: string[]; teamCard: string; overrideLock?: boolean; };

  if (!tid || !/^\d{8}$/.test(String(dateKey || "")) || !uid)
    throw new HttpsError("invalid-argument", "Provide tid, dateKey, uid.");

  const t = await getTournament(tid);
  const nowLocked = Date.now() >= lockTimestamp(String(dateKey), t.lockHourUTC);
  if (nowLocked && !overrideLock) throw new HttpsError("failed-precondition", "Locked; check Override.");

  const docId = `${tid}_${dateKey}_${uid}`;
  const payload: LineupDoc = {
    tid, dateKey, ownerUid: uid, mid: uid,
    captain: String(captain || ""),
    cores: Array.isArray(cores) ? cores.map(String) : [],
    supports: Array.isArray(supports) ? supports.map(String) : [],
    teamCard: String(teamCard || ""),
    locked: !!nowLocked,
    updatedAt: admin.firestore.FieldValue.serverTimestamp(),
  };
  await db.collection("lineups").doc(docId).set(payload, { merge: true });
  return { ok: true, message: nowLocked ? "Saved to a locked day." : "Saved." };
});

export const adminRescoreDay = onCall(async (request: CallableRequest) => {
  assertAdmin(request.auth);
  const { tid, dateKey } = request.data as { tid: Tid; dateKey: DateKey };
  if (!tid || !/^\d{8}$/.test(String(dateKey || "")))
    throw new HttpsError("invalid-argument", "Provide tid/dateKey.");
  const out = await scoreDay(tid, String(dateKey));
  return { ok: true, count: out.count };
});

export const adminExportScores = onCall(async (request: CallableRequest) => {
  assertAdmin(request.auth);
  const { tid, dateKey } = request.data as { tid: Tid; dateKey: DateKey };
  if (!tid || !/^\d{8}$/.test(String(dateKey || "")))
    throw new HttpsError("invalid-argument", "Provide tid/dateKey.");
  const id = `${tid}_${dateKey}`;
  const s = await db.collection("leaderboards").doc(id).get();
  const d = (s.exists ? (s.data() as LeaderboardDoc) : { entries: [] }) as LeaderboardDoc;
  return d;
});

// ---------- Scheduler (v2) with caching & scoring-on-complete ----------
export const pollOpenDota = onSchedule({ schedule: "every 45 minutes" }, async (_evt: ScheduledEvent): Promise<void> => {
  const tids: Tid[] = ["ti2025"]; // add more tournaments if needed
  const completedDatesByTid = new Map<Tid, Set<DateKey>>();

  for (const tid of tids) {
    const t = await getTournament(tid);
    const leagues = t.leagueIds || [];
    if (!leagues.length) continue;

    for (const lid of leagues) {
      let list: any[] = [];
      try {
        list = await fetchLeagueMatches(lid);
      } catch (err: any) {
        console.error("league list error", { tid, lid, err: err?.message || err });
        continue;
      }

      // newest first
      list.sort((a, b) => toInt(b.start_time) - toInt(a.start_time));

      for (const row of list) {
        const matchId = toInt(row.match_id || row.matchId);
        if (!matchId) continue;

        const ref = db.collection("matches").doc(String(matchId));
        const snap = await ref.get();

        // if we already have a complete match, skip
        if (snap.exists) {
          const existing = snap.data() as Partial<MatchDoc>;
          if (existing?.complete === true || looksComplete(existing)) continue;

          // if very recent start, skip re-pull to avoid half-filled data
          const startSec = toInt(existing?.start_time || row.start_time);
          if (Date.now() - startSec * 1000 < 15 * 60 * 1000) continue;
        } else {
          // if we don't have any doc and it's extremely fresh, skip for now
          const startSec = toInt(row.start_time);
          if (Date.now() - startSec * 1000 < 10 * 60 * 1000) continue;
        }

        // polite throttle for OpenDota
        await sleep(800);

        try {
          // Fetch detail once; Firestore is our cache
          const det = await fetchMatchDetail(matchId);
          const beforeComplete = snap.exists ? !!(snap.data() as any)?.complete : false;

          const { dateKey, complete } = await upsertMatchDoc(tid, det);

          // If this fetch made the record "complete", track date for scoring
          if (complete && !beforeComplete) {
            if (!completedDatesByTid.has(tid)) completedDatesByTid.set(tid, new Set<DateKey>());
            completedDatesByTid.get(tid)!.add(dateKey);
          }
        } catch (err: any) {
          console.error("match detail error", { tid, matchId, err: err?.message || err });
        }
      }
    }
  }

  // Score only days that had newly completed matches this run
  for (const [tid, dateSet] of completedDatesByTid.entries()) {
    for (const dateKey of dateSet) {
      try {
        await scoreDay(tid, dateKey);
      } catch (err: any) {
        console.error("scoreDay error", { tid, dateKey, err: err?.message || err });
      }
    }
  }
});

// ---------- Scheduled cleanup: purge match docs older than 2 days ----------
export const purgeOldMatches = onSchedule(
  { schedule: "every day 03:05", timeZone: "America/Toronto" },
  async (): Promise<void> => {
    // Compute UTC dateKey for (today - 2 days)
    const now = new Date();
    const cutoffUTC = Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate() - 2, 0, 0, 0);
    const cutoffKey = yyyymmddFromUTC(new Date(cutoffUTC));

    // dateKey is YYYYMMDD so string comparison works for <
    const snap = await db.collection("matches").where("dateKey", "<", cutoffKey).limit(500).get();

    if (snap.empty) return;

    const batch = db.batch();
    snap.docs.forEach((d) => batch.delete(d.ref));
    await batch.commit();

    console.log(`purgeOldMatches: deleted ${snap.size} matches older than ${cutoffKey}`);
  }
);
