"use strict";

const MAX_WS_OUTPUT_MESSAGE_BYTES = 8 * 1024 * 1024;
const MAX_INTERNAL_RESPONSE_BODY_BYTES = MAX_WS_OUTPUT_MESSAGE_BYTES;
const MAX_INTERNAL_SSE_EVENT_BYTES = MAX_WS_OUTPUT_MESSAGE_BYTES;
// A paused response can retain the rest of the already delivered HTTP chunk.
// Bound that tail independently from the single event currently being sent.
const MAX_INTERNAL_SSE_BUFFER_BYTES = 2 * MAX_INTERNAL_SSE_EVENT_BYTES;

function serializeWsOutput(data) {
  const payload = typeof data === "string" ? data : JSON.stringify(data);
  const bytes = Buffer.byteLength(payload, "utf8");
  return { payload, bytes, withinLimit: bytes <= MAX_WS_OUTPUT_MESSAGE_BYTES };
}

function createSseOutputBuffer(options = {}) {
  const maxEventBytes = options.maxEventBytes ?? MAX_INTERNAL_SSE_EVENT_BYTES;
  const maxBufferedBytes = options.maxBufferedBytes ?? MAX_INTERNAL_SSE_BUFFER_BYTES;
  const delimiter = /\r?\n\r?\n/g;
  let text = "";
  let bytes = 0;
  let scanOffset = 0;
  let ended = false;
  let allowUnterminated = true;

  return {
    append(chunk) {
      const lastCode = text.charCodeAt(text.length - 1);
      const firstCode = chunk.charCodeAt(0);
      // IncomingMessage's UTF-8 decoder preserves code points. Also tolerate
      // callers splitting a UTF-16 surrogate pair between string chunks.
      const joinedSurrogate =
        lastCode >= 0xd800 && lastCode <= 0xdbff && firstCode >= 0xdc00 && firstCode <= 0xdfff;
      const nextBytes = bytes + Buffer.byteLength(chunk, "utf8") - (joinedSurrogate ? 2 : 0);
      if (nextBytes > maxBufferedBytes) return { accepted: false, bytes: nextBytes };
      text += chunk;
      bytes = nextBytes;
      return { accepted: true, bytes };
    },
    end(endOptions = {}) {
      ended = true;
      allowUnterminated = endOptions.allowUnterminated !== false;
    },
    next() {
      if (text.length === 0) return { kind: "empty" };
      delimiter.lastIndex = scanOffset;
      const boundary = delimiter.exec(text);
      if (!boundary && (!ended || !allowUnterminated)) {
        scanOffset = Math.max(0, text.length - 3);
        // A delimiter split across chunks is not part of the event budget.
        // Retain at most the longest proper prefix of an LF/CRLF delimiter.
        const delimiterPrefixBytes = text.endsWith("\r\n\r")
          ? 3
          : text.endsWith("\r\n") || text.endsWith("\n\r")
            ? 2
            : text.endsWith("\r") || text.endsWith("\n")
              ? 1
              : 0;
        const eventBytes = bytes - delimiterPrefixBytes;
        if (eventBytes > maxEventBytes) return { kind: "event_too_large", bytes: eventBytes };
        return { kind: "incomplete" };
      }

      const event = boundary ? text.slice(0, boundary.index) : text;
      const eventBytes = Buffer.byteLength(event, "utf8");
      if (eventBytes > maxEventBytes) {
        return { kind: "event_too_large", bytes: eventBytes };
      }
      const delimiterBytes = boundary ? boundary[0].length : 0;
      text = boundary ? text.slice(boundary.index + boundary[0].length) : "";
      bytes -= eventBytes + delimiterBytes;
      scanOffset = 0;
      return { kind: "event", text: event, bytes: eventBytes };
    },
    clear() {
      text = "";
      bytes = 0;
      scanOffset = 0;
    },
    get bytes() {
      return bytes;
    },
  };
}

module.exports = {
  MAX_WS_OUTPUT_MESSAGE_BYTES,
  MAX_INTERNAL_RESPONSE_BODY_BYTES,
  MAX_INTERNAL_SSE_EVENT_BYTES,
  MAX_INTERNAL_SSE_BUFFER_BYTES,
  serializeWsOutput,
  createSseOutputBuffer,
};
