// /assets/auth-ui.js
// Unified Auth UI for International Fantasy (modal + header + display names)

import {
  getAuth, onAuthStateChanged, signOut,
  signInWithEmailAndPassword, createUserWithEmailAndPassword,
  setPersistence, browserLocalPersistence, browserSessionPersistence,
  updateProfile
} from "https://www.gstatic.com/firebasejs/10.13.1/firebase-auth.js";
import {
  getFirestore, doc, getDoc, setDoc
} from "https://www.gstatic.com/firebasejs/10.13.1/firebase-firestore.js";

/**
 * Setup the shared auth UI.
 * @param {Object} opts
 * @param {import('firebase/app').FirebaseApp} opts.app - your initialized Firebase app
 * @param {HTMLElement} opts.headerEl - container that shows "Sign in" or "Name + Logout"
 * @param {string} [opts.cardClass='card'] - class used for your glass card
 * @returns {{ open: (mode?:'login'|'signup')=>void, close: ()=>void, auth: import('firebase/auth').Auth, db: import('firebase/firestore').Firestore }}
 */
export function setupAuthUI({ app, headerEl, cardClass = 'card' }) {
  if (!app) throw new Error("setupAuthUI requires { app }");
  if (!headerEl) throw new Error("setupAuthUI requires { headerEl }");

  const auth = getAuth(app);
  const db = getFirestore(app);

  // ---------- inject minimal styles (readable inputs on dark bg) ----------
  ensureStyle(`
    #authModal input[type="email"],
    #authModal input[type="password"],
    #authModal input[type="text"] { background:#fff !important; color:#111827 !important; caret-color:#111827; }
    #authModal input::placeholder { color:#6b7280 !important; }
  `);

  // ---------- inject modal markup once ----------
  mountModal(cardClass);

  // refs
  const modal = byId('authModal');
  const form = byId('authForm');
  const tabLogin = byId('am_tab_login');
  const tabSignup = byId('am_tab_signup');
  const emailEl = byId('am_email');
  const passEl = byId('am_password');
  const rememberEl = byId('am_remember');
  const errorEl = byId('am_error');
  const primaryBtn = byId('am_primary');
  const togglePwBtn = byId('am_toggle_pw');
  const switchText = byId('am_switch_text');
  const switchBtn = byId('am_switch');
  const displayWrap = byId('am_display_wrap');
  const displayEl = byId('am_display');

  // ---------- UI helpers ----------
  function setMode(mode) {
    const loginOn = mode === 'login';
    tabLogin.classList.toggle('bg-white/20', loginOn);
    tabSignup.classList.toggle('bg-white/20', !loginOn);
    primaryBtn.textContent = loginOn ? 'Sign in' : 'Create account';
    switchText.textContent = loginOn ? 'No account?' : 'Already have an account?';
    switchBtn.textContent = loginOn ? 'Create one' : 'Sign in';
    form.dataset.mode = mode;
    errorEl.textContent = '';
    displayWrap.classList.toggle('hidden', loginOn); // show Display name only on signup
  }
  function open(mode='login'){ modal.classList.remove('hidden'); setMode(mode); emailEl.focus(); }
  function close(){ modal.classList.add('hidden'); }

  // wire controls
  modal.querySelectorAll('[data-close-auth]').forEach(el => el.addEventListener('click', close));
  togglePwBtn.addEventListener('click', (e) => {
    e.preventDefault();
    passEl.type = passEl.type === 'password' ? 'text' : 'password';
    togglePwBtn.textContent = passEl.type === 'text' ? 'Hide' : 'Show';
  });
  tabLogin.onclick = () => setMode('login');
  tabSignup.onclick = () => setMode('signup');
  switchBtn.onclick = () => setMode(form.dataset.mode === 'login' ? 'signup' : 'login');

  // ---------- logic ----------
  async function applyPersistence(){
    const remember = localStorage.getItem('if_remember') === '1';
    await setPersistence(auth, remember ? browserLocalPersistence : browserSessionPersistence);
  }

  async function saveDisplayName(user, name){
    const cleaned = (name || '').trim().slice(0, 32);
    if (!cleaned) throw new Error('Please enter a display name.');
    await updateProfile(user, { displayName: cleaned });
    await setDoc(doc(db, 'users', user.uid), { name: cleaned, updatedAt: Date.now() }, { merge: true });
  }

  async function getPrettyName(user){
    try{
      const s = await getDoc(doc(db, 'users', user.uid));
      const n = s.exists() ? s.data().name : '';
      return n || user.displayName || user.email || user.uid;
    }catch{
      return user.displayName || user.email || user.uid;
    }
  }

  async function renderHeader(user){
    if (!user){
      headerEl.innerHTML = `<button id="openAuth" class="px-4 py-2 rounded-xl bg-emerald-500 hover:bg-emerald-600 text-white font-semibold shadow">Sign in</button>`;
      byId('openAuth').onclick = () => open('login');
      return;
    }
    const name = await getPrettyName(user);
    headerEl.innerHTML = `
      <span class="text-sm text-gray-200 hidden md:inline drop-shadow">${name}</span>
      <button id="logoutBtn" class="px-3 py-2 rounded-xl bg-red-500 hover:bg-red-600 text-white">Logout</button>`;
    byId('logoutBtn').onclick = () => signOut(auth);
  }

  // main submit handler
  form.addEventListener('submit', async (e) => {
    e.preventDefault();
    const mode = form.dataset.mode || 'login';
    const email = (emailEl.value || '').trim();
    const pass  = passEl.value;
    const remember = !!rememberEl.checked;
    localStorage.setItem('if_remember', remember ? '1' : '0');
    errorEl.textContent = '';

    try{
      await applyPersistence();

      // Special case: if already logged-in and the display name field is visible,
      // we're just saving a name for a user who lacked one.
      if (mode === 'login' && auth.currentUser && !displayWrap.classList.contains('hidden')){
        await saveDisplayName(auth.currentUser, displayEl.value);
        close();
        await renderHeader(auth.currentUser);
        return;
      }

      if (mode === 'login'){
        await signInWithEmailAndPassword(auth, email, pass);

        // After login, if no name is set anywhere, prompt once.
        const u = auth.currentUser;
        const snap = await getDoc(doc(db, 'users', u.uid));
        const needs = !(u.displayName) && !(snap.exists() && snap.data()?.name);
        if (needs){
          displayWrap.classList.remove('hidden');
          errorEl.textContent = 'Pick a display name to show on the leaderboard.';
          primaryBtn.textContent = 'Save name';
          return; // wait for next submit to save
        }
        close();
      } else {
        const cred = await createUserWithEmailAndPassword(auth, email, pass);
        await saveDisplayName(cred.user, displayEl.value);
        close();
      }
    }catch(err){
      errorEl.textContent = (err?.message || String(err)).replace('Firebase:', '').trim();
    }
  });

  // keep header in sync
  onAuthStateChanged(auth, (u) => { renderHeader(u); });

  // public API
  return { open, close, auth, db };
}

/* ---------- helpers ---------- */
function byId(id){ return /** @type {HTMLElement} */ (document.getElementById(id)); }

function ensureStyle(cssText){
  if (document.querySelector('style[data-auth-ui]')) return;
  const s = document.createElement('style');
  s.setAttribute('data-auth-ui','true');
  s.textContent = cssText;
  document.head.appendChild(s);
}

function mountModal(cardClass){
  if (document.getElementById('authModal')) return;

  const wrap = document.createElement('div');
  wrap.id = 'authModal';
  wrap.className = 'fixed inset-0 z-50 hidden';
  wrap.innerHTML = `
    <div class="absolute inset-0 bg-black/60 backdrop-blur-sm" data-close-auth></div>
    <div class="relative mx-auto mt-24 w-[92%] max-w-md ${cardClass} rounded-2xl p-6">
      <div class="flex items-center justify-between mb-3">
        <div class="inline-flex gap-1 rounded-xl bg-white/10 p-1">
          <button id="am_tab_login"  class="px-3 py-1 rounded-lg text-sm bg-white/20">Sign in</button>
          <button id="am_tab_signup" class="px-3 py-1 rounded-lg text-sm">Create account</button>
        </div>
        <button class="px-2 py-1 text-sm bg-white/10 rounded-lg hover:bg-white/20" data-close-auth>&times;</button>
      </div>

      <form id="authForm" class="space-y-3" data-mode="login">
        <div id="am_display_wrap" class="hidden">
          <label class="block text-xs text-gray-300 mb-1">Display name</label>
          <input id="am_display" type="text" placeholder="e.g., MidOrFeed" class="w-full rounded-lg px-3 py-2" maxlength="32" />
        </div>

        <div>
          <label class="block text-xs text-gray-300 mb-1">Email</label>
          <input id="am_email" type="email" placeholder="you@example.com" class="w-full rounded-lg px-3 py-2" required />
        </div>

        <div>
          <label class="block text-xs text-gray-300 mb-1">Password</label>
          <div class="flex gap-2">
            <input id="am_password" type="password" placeholder="••••••••" class="w-full rounded-lg px-3 py-2" required />
            <button type="button" id="am_toggle_pw" class="px-3 rounded-lg bg-white/10">Show</button>
          </div>
        </div>

        <div class="flex items-center justify-between">
          <label class="text-sm text-gray-200 flex items-center gap-2">
            <input id="am_remember" type="checkbox" class="w-4 h-4" />
            Remember me
          </label>
          <span class="text-xs text-gray-300 opacity-60">Forgot?</span>
        </div>

        <div id="am_error" class="text-rose-300 text-sm min-h-5"></div>

        <div class="flex gap-2">
          <button id="am_primary" class="grow px-4 py-2 rounded-xl bg-emerald-500 hover:bg-emerald-600 text-white font-semibold">Sign in</button>
          <button type="button" class="px-4 py-2 rounded-xl bg-slate-700 hover:bg-slate-600 text-white/90" data-close-auth>Cancel</button>
        </div>

        <p class="text-xs text-gray-300">
          <span id="am_switch_text">No account?</span>
          <button type="button" id="am_switch" class="underline">Create one</button>
        </p>
      </form>
    </div>
  `;
  document.body.appendChild(wrap);
}
