import fetch from "node-fetch";
import { db } from "./firebaseAdmin.js";
import {
  scorePlayerMatch,
  scoreTeamMatch,
  sweepBonus,
  applyCaptainDailyMultiplier,
} from "./scoring.js";
import { getPlayerDisplay, nameFromMatchPlayer } from "./players.js";

function dayRangeUTC(dateKey: string) {
  const y = +dateKey.slice(0, 4), m = +dateKey.slice(4, 6) - 1, d = +dateKey.slice(6, 8);
  const start = Date.UTC(y, m, d, 0, 0, 0) / 1000;
  const end   = Date.UTC(y, m, d, 23, 59, 59) / 1000;
  return { start, end };
}

async function leagueMatchesForDay(leagueId: number, dateKey: string) {
  const res = await fetch(`https://api.opendota.com/api/leagues/${leagueId}/matches`);
  const all = (await res.json()) as any[];
  const { start, end } = dayRangeUTC(dateKey);
  return all.filter((m: any) =>
    m?.start_time >= start &&
    m?.start_time <= end &&
    m?.radiant_win !== null
  );
}

async function matchDetail(matchId: number) {
  const res = await fetch(`https://api.opendota.com/api/matches/${matchId}`);
  return (await res.json()) as any;
}

// Ensure an org doc exists; return id + name for writing on team docs.
async function mapTeamToOrg(t: { team_id?: number; tag?: string; name?: string } | undefined) {
  const teamId = t?.team_id ?? 0;
  const tag    = (t?.tag  ?? "").trim() || "Team";
  const name   = (t?.name ?? tag).trim() || tag;

  if (teamId) {
    const byTeam = await db.collection("orgs").where("dota_team_id","==",teamId).limit(1).get();
    if (!byTeam.empty) {
      const data = byTeam.docs[0].data() as any;
      return { orgId: byTeam.docs[0].id, orgName: (data?.name as string) ?? name };
    }
  }
  const byTag = await db.collection("orgs").where("tag","==",tag).limit(1).get();
  if (!byTag.empty) {
    const data = byTag.docs[0].data() as any;
    return { orgId: byTag.docs[0].id, orgName: (data?.name as string) ?? name };
  }

  const ref = db.collection("orgs").doc();
  await ref.set({ name, tag, dota_team_id: teamId || null });
  return { orgId: ref.id, orgName: name };
}

export async function scoreDay(tid: string, dateKey: string): Promise<void> {
  const tDoc = await db.doc(`tournaments/${tid}`).get();
  if (!tDoc.exists) throw new Error("Tournament not found");
  const { leagueIds } = tDoc.data() as any;

  // 1) discover matches
  const dailyMatches = (await Promise.all(
    (leagueIds as number[]).map((id) => leagueMatchesForDay(id, dateKey))
  )).flat();

  // Track per-series wins for sweep bonus
  const seriesBuckets = new Map<
    number,
    { series_type: number; radiantWins: number; direWins: number; radiantOrgId: string; direOrgId: string }
  >();

  for (const m of dailyMatches) {
    const det: any = await matchDetail(m.match_id);
    if (!det || typeof det !== "object") continue;

    // Guard: compute a safe matchId (skip if missing)
    const matchId = det?.match_id ?? m?.match_id ?? null;
    if (!matchId) continue;

    const { orgId: radiantOrgId, orgName: radiantName } = await mapTeamToOrg(det.radiant_team);
    const { orgId: direOrgId,    orgName: direName }    = await mapTeamToOrg(det.dire_team);

    // ── PLAYERS
    if (Array.isArray(det.players)) {
      for (const p of det.players as any[]) {
        const steam32 = p?.account_id as number | undefined;
        if (!steam32) continue; // we require steam32 per your schema

        // Prefer the name straight from the match; fallback to OpenDota /players/{steam32}; final fallback = steam32
        const matchName = nameFromMatchPlayer(p);
        const name = matchName ?? (await getPlayerDisplay(steam32)) ?? String(steam32);

        const side = p.isRadiant ? "radiant" : "dire";
        const orgName = side === "radiant" ? radiantName : direName;

        const points = scorePlayerMatch(
          {
            kills: p.kills, assists: p.assists, deaths: p.deaths,
            last_hits: p.last_hits, denies: p.denies,
            obs_placed: p.obs_placed, sen_placed: p.sen_placed,
            camps_stacked: p.camps_stacked, win: !!p.win,
          },
          det.duration
        );

        // EXACT fields you requested:
        await db.doc(`scores_player/${tid}_${matchId}_${steam32}`).set(
          {
            tid,
            dateKey,
            matchId,
            steam32,
            name,       // human-readable
            orgName,    // team/org that game
            points,
          },
          { merge: true }
        );
      }
    }

    // ── TEAMS
    const common = {
      tower_status_radiant: det.tower_status_radiant,
      tower_status_dire: det.tower_status_dire,
      barracks_status_radiant: det.barracks_status_radiant,
      barracks_status_dire: det.barracks_status_dire,
      objectives: det.objectives ?? [],
      radiant_win: !!det.radiant_win,
    };

    const rPoints = scoreTeamMatch({ side: "radiant", ...common });
    const dPoints = scoreTeamMatch({ side: "dire", ...common });

    await db.doc(`scores_team/${tid}_${matchId}_${radiantOrgId}`).set(
      { tid, dateKey, matchId, orgId: radiantOrgId, orgName: radiantName, points: rPoints },
      { merge: true }
    );
    await db.doc(`scores_team/${tid}_${matchId}_${direOrgId}`).set(
      { tid, dateKey, matchId, orgId: direOrgId, orgName: direName, points: dPoints },
      { merge: true }
    );

    // ── SERIES BUCKET (for sweep bonus)
    if (det.series_id != null) {
      const b =
        seriesBuckets.get(det.series_id) ??
        { series_type: det.series_type as number, radiantWins: 0, direWins: 0, radiantOrgId, direOrgId };
      if (det.radiant_win) b.radiantWins++; else b.direWins++;
      seriesBuckets.set(det.series_id, b);
    }
  }

  // 2) SWEEP BONUS rows → scores_team (marked sweep=true, has seriesId instead of matchId)
  for (const [sid, b] of seriesBuckets.entries()) {
    const bonus = sweepBonus(b.series_type, b.radiantWins, b.direWins);
    if (bonus.radiant) {
      await db.doc(`scores_team/${tid}_series_${sid}_${b.radiantOrgId}`).set(
        { tid, dateKey, seriesId: sid, orgId: b.radiantOrgId, points: bonus.radiant, sweep: true },
        { merge: true }
      );
    }
    if (bonus.dire) {
      await db.doc(`scores_team/${tid}_series_${sid}_${b.direOrgId}`).set(
        { tid, dateKey, seriesId: sid, orgId: b.direOrgId, points: bonus.dire, sweep: true },
        { merge: true }
      );
    }
  }

  // 3) AGGREGATE locked lineups for THIS date only
  const lineups = await db.collection("lineups")
    .where("tid","==",tid).where("dateKey","==",dateKey).where("locked","==",true).get();

  for (const doc of lineups.docs) {
    const lu: any = doc.data();
    const players: number[] = [lu.captain, ...lu.cores, ...lu.supports];

    // Player totals for the day
    const byId = new Map<number, number>();
    for (const steam32 of players) {
      const q = await db.collection("scores_player")
        .where("tid","==",tid).where("dateKey","==",dateKey).where("steam32","==",steam32).get();
      const sum = q.docs.reduce((s, d) => s + ((d.data() as any).points || 0), 0);
      byId.set(steam32, sum);
    }

    const captainBase = byId.get(lu.captain) ?? 0;
    const captainFinal = applyCaptainDailyMultiplier(captainBase);
    const others = players.filter(id => id !== lu.captain)
      .reduce((s, id) => s + (byId.get(id) || 0), 0);

    // Team totals for the day (include sweep rows)
    let teamPts = 0;
    const teamDocs = await db.collection("scores_team")
      .where("tid","==",tid).where("dateKey","==",dateKey).get();
    teamDocs.forEach(d => {
      const v = d.data() as any;
      if (v.orgId === lu.teamCard) teamPts += v.points || 0;
    });

    const totalPoints = captainFinal + others + teamPts;

    await db.doc(`scores_manager_day/${tid}_${lu.mid}_${dateKey}`).set(
      {
        tid, mid: lu.mid, dateKey, totalPoints,
        breakdown: {
          captain: { steam32: lu.captain, pointsBase: captainBase, pointsFinal: captainFinal, multiplier: 1.5 },
          cores: lu.cores.map((id: number) => ({ steam32: id, points: byId.get(id) || 0 })),
          supports: lu.supports.map((id: number) => ({ steam32: id, points: byId.get(id) || 0 })),
          team: { orgId: lu.teamCard, points: teamPts },
        },
      },
      { merge: true }
    );
  }

  // 4) LEADERBOARD for the day
  const daySnaps = await db.collection("scores_manager_day")
    .where("tid","==",tid).where("dateKey","==",dateKey).get();

  const entries = daySnaps.docs
    .map(d => ({ mid: (d.data() as any).mid, totalPoints: (d.data() as any).totalPoints }))
    .sort((a, b) => b.totalPoints - a.totalPoints);

  await db.doc(`leaderboards/${tid}_${dateKey}`).set({ entries }, { merge: true });
}
