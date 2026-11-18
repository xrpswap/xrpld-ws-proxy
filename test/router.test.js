import test from 'node:test';
import assert from 'node:assert/strict';
import {
  parseMessage,
  classifyRoute,
  extractRequestId,
  buildErrorResponse,
} from '../src/router.js';

test('parseMessage handles valid JSON and edge-case IDs (0, string, missing)', () => {
  const zeroId = parseMessage(JSON.stringify({ id: 0, command: 'server_info' }));
  assert.equal(zeroId.ok, true);
  assert.equal(zeroId.idInfo.hasId, true);
  assert.equal(zeroId.idInfo.id, 0);

  const strId = parseMessage(JSON.stringify({ id: 'req-abc', command: 'fee' }));
  assert.equal(strId.ok, true);
  assert.equal(strId.idInfo.hasId, true);
  assert.equal(strId.idInfo.id, 'req-abc');

  const noId = parseMessage(JSON.stringify({ command: 'ping' }));
  assert.equal(noId.ok, true);
  assert.equal(noId.idInfo.hasId, false);
  assert.equal(noId.idInfo.id, undefined);
});

test('parseMessage returns jsonInvalid for malformed JSON or missing command while preserving id: 0', () => {
  const badJson = parseMessage('{broken json');
  assert.equal(badJson.ok, false);
  assert.equal(badJson.errorCode, 'jsonInvalid');

  const missingCmd = parseMessage(JSON.stringify({ id: 0 }));
  assert.equal(missingCmd.ok, false);
  assert.equal(missingCmd.errorCode, 'jsonInvalid');
  assert.equal(missingCmd.idInfo.hasId, true);
  assert.equal(missingCmd.idInfo.id, 0);

  const errRes = buildErrorResponse(missingCmd.idInfo, missingCmd.errorCode, missingCmd.errorMessage);
  assert.equal(errRes.id, 0);
  assert.equal(errRes.status, 'error');
  assert.equal(errRes.type, 'response');
  assert.equal(errRes.error, 'jsonInvalid');
});

test('classifyRoute assigns roles, stateful flags, and command weights accurately', () => {
  const serverInfo = classifyRoute({ command: 'server_info' });
  assert.equal(serverInfo.role, 'normal');
  assert.equal(serverInfo.isStateful, false);
  assert.equal(serverInfo.channelType, 'stateless');
  assert.equal(serverInfo.weight, 1);

  const accountTx = classifyRoute({ command: 'account_tx', account: 'rPT1Sjq2YGrBMTttX4GZHjKu9dyfzbpAYe' });
  assert.equal(accountTx.role, 'history');
  assert.equal(accountTx.isStateful, false);
  assert.equal(accountTx.weight, 5);

  const submit = classifyRoute({ command: 'submit', tx_blob: 'DEADBEEF' });
  assert.equal(submit.role, 'submit');
  assert.equal(submit.isStateful, false);
  assert.equal(submit.weight, 2);

  const pathFind = classifyRoute({ command: 'path_find', subcommand: 'create' });
  assert.equal(pathFind.role, 'path');
  assert.equal(pathFind.isStateful, true);
  assert.equal(pathFind.channelType, 'path_find');
  assert.equal(pathFind.subcommand, 'create');

  const ripplePathFind = classifyRoute({ command: 'ripple_path_find' });
  assert.equal(ripplePathFind.role, 'path');
  assert.equal(ripplePathFind.isStateful, false);
  assert.equal(ripplePathFind.channelType, 'stateless');

  const subscribe = classifyRoute({ command: 'subscribe', streams: ['ledger'] });
  assert.equal(subscribe.role, 'normal');
  assert.equal(subscribe.isStateful, true);
  assert.equal(subscribe.channelType, 'subscribe');
});

test('classifyRoute routes explicit ledger_hash or numeric ledger_index queries to history', () => {
  const currentAccount = classifyRoute({ command: 'account_info', account: 'r123', ledger_index: 'validated' });
  assert.equal(currentAccount.role, 'normal');

  const numericLedger = classifyRoute({ command: 'account_info', account: 'r123', ledger_index: 84000000 });
  assert.equal(numericLedger.role, 'history');

  const hashLedger = classifyRoute({ command: 'ledger', ledger_hash: 'ABC123' });
  assert.equal(hashLedger.role, 'history');
});

test('extractRequestId handles null and undefined properly', () => {
  assert.deepEqual(extractRequestId({ id: null }), { hasId: false, id: undefined });
  assert.deepEqual(extractRequestId({}), { hasId: false, id: undefined });
  assert.deepEqual(extractRequestId({ id: 0 }), { hasId: true, id: 0 });
  assert.deepEqual(extractRequestId({ id: '' }), { hasId: true, id: '' });
});
