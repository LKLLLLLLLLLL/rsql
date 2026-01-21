// sessionService: store credentials in sessionStorage (per-tab) so different tabs can login different accounts
function _safeGetStorage() {
  try {
    if (typeof window === 'undefined' || !window.sessionStorage) return null
    return window.sessionStorage
  } catch (e) {
    return null
  }
}

const USER_KEY = 'rsql_username'
const PASS_KEY = 'rsql_password'

function setCredentials(username, password) {
  const s = _safeGetStorage()
  if (!s) return false
  try {
    s.setItem(USER_KEY, username || '')
    s.setItem(PASS_KEY, password || '')
    return true
  } catch (e) {
    return false
  }
}

function getCredentials() {
  const s = _safeGetStorage()
  if (!s) return { username: null, password: null }
  try {
    return { username: s.getItem(USER_KEY), password: s.getItem(PASS_KEY) }
  } catch (e) {
    return { username: null, password: null }
  }
}

function clearCredentials() {
  const s = _safeGetStorage()
  if (!s) return false
  try {
    s.removeItem(USER_KEY)
    s.removeItem(PASS_KEY)
    return true
  } catch (e) {
    return false
  }
}

export { setCredentials, getCredentials, clearCredentials }
