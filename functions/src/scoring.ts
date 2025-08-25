// --- PLAYER ---
export function scorePlayerMatch(
  p: {
    kills: number;
    assists: number;
    deaths: number;
    last_hits: number;
    denies: number;
    obs_placed?: number;
    sen_placed?: number;
    camps_stacked?: number;
    win: boolean;
  },
  durationSeconds: number
): number {
  const wards = (p.obs_placed ?? 0) + (p.sen_placed ?? 0);

  let pts = 0;
  pts += p.kills * 3;
  pts += p.assists * 2;
  pts += p.deaths * -1;
  pts += p.last_hits * 0.02;
  pts += p.denies * 0.02;
  pts += wards * 0.2;
  pts += (p.camps_stacked ?? 0) * 0.5;

  if (p.win) {
    pts += 15;
    if (durationSeconds < 25 * 60) pts += 15; // win under 25
  }
  if (p.kills + p.assists > 20) pts += 2;

  return pts;
}

// helpers
const popcount = (n: number) => { let c = 0, x = n >>> 0; while (x) { x &= x - 1; c++; } return c; };
const towersDestroyedBy = (side: "radiant" | "dire", tr: number, td: number) =>
  11 - popcount(side === "radiant" ? tr : td);
const raxDestroyedBy = (side: "radiant" | "dire", br: number, bd: number) =>
  6 - popcount(side === "radiant" ? br : bd);

export function roshansForSide(objectives: any[] | undefined, side: "radiant" | "dire"): number {
  const t = side === "radiant" ? 2 : 3;
  return (objectives || []).filter(o => o.type === "CHAT_MESSAGE_ROSHAN_KILL" && o.team === t).length;
}

export function firstBloodForSide(objectives: any[] | undefined, side: "radiant" | "dire"): boolean {
  const t = side === "radiant" ? 2 : 3;
  return (objectives || []).some(o => o.type === "CHAT_MESSAGE_FIRSTBLOOD" && o.team === t);
}

export function scoreTeamMatch(a: {
  side: "radiant" | "dire";
  tower_status_radiant: number;
  tower_status_dire: number;
  barracks_status_radiant: number;
  barracks_status_dire: number;
  objectives?: any[];
  radiant_win: boolean;
}): number {
  const teamWon = (a.side === "radiant" && a.radiant_win) || (a.side === "dire" && !a.radiant_win);

  let pts = 0;
  pts += towersDestroyedBy(a.side, a.tower_status_radiant, a.tower_status_dire) * 1;
  pts += raxDestroyedBy(a.side, a.barracks_status_radiant, a.barracks_status_dire) * 1;
  pts += roshansForSide(a.objectives, a.side) * 3;
  if (firstBloodForSide(a.objectives, a.side)) pts += 2;
  if (teamWon) pts += 2;

  return pts;
}

// --- SERIES SWEEP BONUS (once per swept series) ---
export function sweepBonus(seriesType: number, radiantWins: number, direWins: number) {
  const out = { radiant: 0, dire: 0 };
  if (seriesType === 1 && (radiantWins === 2 || direWins === 2)) {
    radiantWins === 2 ? (out.radiant += 15) : (out.dire += 15); // Bo3 2–0
  }
  if (seriesType === 2 && (radiantWins === 3 || direWins === 3)) {
    radiantWins === 3 ? (out.radiant += 15) : (out.dire += 15); // Bo5 3–0
  }
  return out;
}

export const applyCaptainDailyMultiplier = (n: number) => n * 1.5;
