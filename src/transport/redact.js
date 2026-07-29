/**
 * Shared secret-redaction helper for RPC/debug logging.
 *
 * token.js and http.js both log request/response bodies for debugging
 * (gated by COCO_RPC_LOG). Those bodies carry access_token/refresh_token
 * values on the auth endpoints — this strips known-sensitive field names
 * before anything is serialized to a log line, regardless of nesting depth.
 */

// Exact-match (not substring) on purpose: a substring match on /token/ or
// /secret/ would also catch access_token_expires_at/refresh_token_expires_at
// (plain timestamps, useful for debugging expiry — not secrets) and any
// unrelated field that merely contains those words in its name.
const SENSITIVE_KEYS = new Set([
  'access_token', 'refresh_token', 'id_token',
  'api_key', 'apikey',
  'client_secret', 'clientsecret',
  'password', 'secret', 'ticket',
]);

export function redactSecrets(value) {
  if (Array.isArray(value)) return value.map(redactSecrets);
  if (value && typeof value === 'object') {
    const out = {};
    for (const [k, v] of Object.entries(value)) {
      out[k] = SENSITIVE_KEYS.has(k.toLowerCase()) ? '[REDACTED]' : redactSecrets(v);
    }
    return out;
  }
  return value;
}
