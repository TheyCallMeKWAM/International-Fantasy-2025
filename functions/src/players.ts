import { db } from "./firebaseAdmin.js";

/**
 * Resolve a Steam32 -> display name.
 * 1) Cache in /players/{steam32}
 * 2) Try OpenDota /players/{steam32}.profile.personaname
 * 3) Fallback to the steam32 string.
 */
export async function getPlayerDisplay(steam32: number): Promise<string> {
  if (!steam32) return String(steam32);

  const ref = db.collection("players").doc(String(steam32));
  const snap = await ref.get();
  if (snap.exists) {
    const d = snap.data() as any;
    if (d?.display) return d.display as string;
  }

  let display = String(steam32);
  try {
    const res = await fetch(`https://api.opendota.com/api/players/${steam32}`);
    const js = (await res.json()) as any;
    display = js?.profile?.personaname ?? js?.name ?? String(steam32);
  } catch {
    // ignore
  }

  await ref.set(
    { steam32, display, updatedAt: Date.now() },
    { merge: true }
  );

  return display;
}

/** Prefer a name straight from the match player object (pro games usually include it). */
export function nameFromMatchPlayer(p: any): string | undefined {
  return (p?.name ?? p?.personaname ?? undefined);
}
