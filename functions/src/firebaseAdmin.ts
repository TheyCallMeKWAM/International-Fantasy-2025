import { applicationDefault, getApps, initializeApp } from "firebase-admin/app";
import { getFirestore } from "firebase-admin/firestore";

const app = getApps().length
  ? getApps()[0]
  : initializeApp({ credential: applicationDefault() });

const db = getFirestore(app);

// As a safety net, drop any accidental `undefined` fields.
db.settings({ ignoreUndefinedProperties: true });

export { db };
