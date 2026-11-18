import { config as defaultConfig } from './config.js';

/**
 * Safely extract request id without treating 0 or "" as absent.
 * @param {Record<string, unknown>} payload
 * @returns {{ hasId: boolean, id: unknown }}
 */
export function extractRequestId(payload) {
  if (!payload || typeof payload !== 'object') {
    return { hasId: false, id: undefined };
  }
  const hasId = payload.id !== undefined && payload.id !== null;
  return {
    hasId,
    id: hasId ? payload.id : undefined,
  };
}

/**
 * Build a standardized XRPL WebSocket error response.
 * @param {{ hasId?: boolean, id?: unknown } | null | undefined} idInfo
 * @param {string} errorCode
 * @param {string} errorMessage
 * @returns {Record<string, unknown>}
 */
export function buildErrorResponse(idInfo, errorCode, errorMessage) {
  const response = {
    status: 'error',
    type: 'response',
    error: errorCode,
    error_message: errorMessage,
  };
  if (idInfo && idInfo.hasId) {
    response.id = idInfo.id;
  }
  return response;
}

/**
 * Parse raw WebSocket message data into a validated XRPL JSON command object.
 * @param {import('ws').RawData | string} rawData
 */
export function parseMessage(rawData) {
  let text;
  if (typeof rawData === 'string') {
    text = rawData;
  } else if (Buffer.isBuffer(rawData)) {
    text = rawData.toString('utf8');
  } else if (Array.isArray(rawData)) {
    text = Buffer.concat(rawData).toString('utf8');
  } else if (rawData instanceof ArrayBuffer) {
    text = Buffer.from(rawData).toString('utf8');
  } else {
    return {
      ok: false,
      idInfo: { hasId: false, id: undefined },
      errorCode: 'jsonInvalid',
      errorMessage: 'Unsupported WebSocket frame payload type.',
    };
  }

  let payload;
  try {
    payload = JSON.parse(text);
  } catch {
    return {
      ok: false,
      idInfo: { hasId: false, id: undefined },
      errorCode: 'jsonInvalid',
      errorMessage: 'Malformed JSON request.',
    };
  }

  if (!payload || typeof payload !== 'object' || Array.isArray(payload)) {
    return {
      ok: false,
      idInfo: { hasId: false, id: undefined },
      errorCode: 'jsonInvalid',
      errorMessage: 'Request payload must be a JSON object.',
    };
  }

  const idInfo = extractRequestId(payload);

  if (typeof payload.command !== 'string' || payload.command.trim() === '') {
    return {
      ok: false,
      idInfo,
      errorCode: 'jsonInvalid',
      errorMessage: 'Missing or invalid "command" field.',
    };
  }

  return {
    ok: true,
    payload,
    idInfo,
  };
}

/**
 * Classify a parsed XRPL command into role, channelType, stateful flag, and token weight.
 * @param {Record<string, any>} payload
 * @param {typeof defaultConfig.routing} [routingCfg]
 */
export function classifyRoute(payload, routingCfg = defaultConfig.routing) {
  const command = payload.command;
  const pathSet = new Set(routingCfg.pathCommands || []);
  const historySet = new Set(routingCfg.historyCommands || []);
  const submitSet = new Set(routingCfg.submitCommands || []);

  let role = 'normal';

  if (pathSet.has(command)) {
    role = 'path';
  } else if (submitSet.has(command)) {
    role = 'submit';
  } else if (historySet.has(command)) {
    role = 'history';
  } else if (
    routingCfg.routeExplicitLedgerToHistory &&
    command !== 'subscribe' &&
    command !== 'unsubscribe' &&
    hasExplicitHistoricalLedger(payload)
  ) {
    role = 'history';
  }

  let channelType = 'stateless';
  if (command === 'path_find') {
    channelType = 'path_find';
  } else if (command === 'subscribe' || command === 'unsubscribe') {
    channelType = 'subscribe';
  }

  const isStateful = channelType !== 'stateless';
  const weights = routingCfg.commandWeights || {};
  const weight =
    typeof weights[command] === 'number' && weights[command] > 0
      ? weights[command]
      : routingCfg.defaultWeight ?? 2;

  return {
    command,
    role,
    channelType,
    isStateful,
    weight,
    subcommand: typeof payload.subcommand === 'string' ? payload.subcommand : undefined,
  };
}

/**
 * Detect explicit historical ledger selector (`ledger_hash` or numeric `ledger_index`).
 * @param {Record<string, any>} payload
 */
function hasExplicitHistoricalLedger(payload) {
  if (typeof payload.ledger_hash === 'string' && payload.ledger_hash.length > 0) {
    return true;
  }
  if (typeof payload.ledger_index === 'number' && Number.isFinite(payload.ledger_index)) {
    return true;
  }
  return false;
}
