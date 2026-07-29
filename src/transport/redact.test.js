import { test } from 'node:test';
import assert from 'node:assert/strict';
import { redactSecrets } from './redact.js';

test('redacts known sensitive keys at the top level', () => {
  const out = redactSecrets({ access_token: 'eyJabc', org_id: 'o1' });
  assert.equal(out.access_token, '[REDACTED]');
  assert.equal(out.org_id, 'o1');
});

test('redacts nested tokens but leaves *_expires_at timestamps intact (needed for debugging expiry)', () => {
  const out = redactSecrets({ data: { refresh_token: 'r1', access_token_expires_at: 123 } });
  assert.equal(out.data.refresh_token, '[REDACTED]');
  assert.equal(out.data.access_token_expires_at, 123);
});

test('redacts api_key, client_secret, password, ticket variants', () => {
  const out = redactSecrets({
    api_key: 'k1', apiKey: 'k2', client_secret: 's1',
    password: 'p1', ticket: 't1',
  });
  assert.equal(out.api_key, '[REDACTED]');
  assert.equal(out.apiKey, '[REDACTED]');
  assert.equal(out.client_secret, '[REDACTED]');
  assert.equal(out.password, '[REDACTED]');
  assert.equal(out.ticket, '[REDACTED]');
});

test('redacts generic "token" plus service-specific token/secret fields (core.js invitation accept, as.js upload_token)', () => {
  const out = redactSecrets({
    token: 'invite-secret', upload_token: 'u1', verify_token: 'v1', app_secret: 'a1',
  });
  assert.equal(out.token, '[REDACTED]');
  assert.equal(out.upload_token, '[REDACTED]');
  assert.equal(out.verify_token, '[REDACTED]');
  assert.equal(out.app_secret, '[REDACTED]');
});

test('does not false-positive on field names that merely contain "secret" or "token" as a substring', () => {
  const out = redactSecrets({ not_a_secret: 'v1', token_type: 'Bearer', device_id: 'd1' });
  assert.equal(out.not_a_secret, 'v1');
  assert.equal(out.token_type, 'Bearer');
  assert.equal(out.device_id, 'd1');
});

test('redacts inside arrays', () => {
  const out = redactSecrets([{ access_token: 'a' }, { org_id: 'o' }]);
  assert.equal(out[0].access_token, '[REDACTED]');
  assert.equal(out[1].org_id, 'o');
});

test('passes through primitives and strings unchanged', () => {
  assert.equal(redactSecrets('hello'), 'hello');
  assert.equal(redactSecrets(42), 42);
  assert.equal(redactSecrets(null), null);
  assert.equal(redactSecrets(undefined), undefined);
});

test('does not false-positive on ordinary field names', () => {
  const out = redactSecrets({ org_id: 'o1', member_id: 'm1', name: 'n1' });
  assert.deepEqual(out, { org_id: 'o1', member_id: 'm1', name: 'n1' });
});
