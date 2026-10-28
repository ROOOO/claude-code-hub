import { createRequire } from "node:module";
import { describe, expect, it } from "vitest";

const requireFromHere = createRequire(import.meta.url);
type SseResult =
  | { kind: "event"; text: string; bytes: number }
  | { kind: "event_too_large"; bytes: number }
  | { kind: "incomplete" | "empty" };
type OutputBuffer = {
  append: (text: string) => { accepted: boolean; bytes: number };
  end: (options?: { allowUnterminated?: boolean }) => void;
  next: () => SseResult;
  clear: () => void;
  readonly bytes: number;
};
const {
  MAX_WS_OUTPUT_MESSAGE_BYTES,
  MAX_INTERNAL_SSE_EVENT_BYTES,
  MAX_INTERNAL_SSE_BUFFER_BYTES,
  MAX_INTERNAL_RESPONSE_BODY_BYTES,
  serializeWsOutput,
  createSseOutputBuffer,
}: {
  MAX_WS_OUTPUT_MESSAGE_BYTES: number;
  MAX_INTERNAL_SSE_EVENT_BYTES: number;
  MAX_INTERNAL_SSE_BUFFER_BYTES: number;
  MAX_INTERNAL_RESPONSE_BODY_BYTES: number;
  serializeWsOutput: (value: unknown) => { payload: string; bytes: number; withinLimit: boolean };
  createSseOutputBuffer: (options?: {
    maxEventBytes?: number;
    maxBufferedBytes?: number;
  }) => OutputBuffer;
} = requireFromHere("../../server-lib/responses-ws-output.js");

const MIB = 1024 * 1024;

describe("Responses WebSocket output limits", () => {
  it("aligns the message, event and JSON limits while separately bounding raw backlog", () => {
    expect(MAX_WS_OUTPUT_MESSAGE_BYTES).toBe(8 * MIB);
    expect(MAX_INTERNAL_SSE_EVENT_BYTES).toBe(8 * MIB);
    expect(MAX_INTERNAL_RESPONSE_BODY_BYTES).toBe(8 * MIB);
    expect(MAX_INTERNAL_SSE_BUFFER_BYTES).toBe(16 * MIB);
  });

  it("measures final serialized UTF-8 bytes and preserves pre-serialized messages", () => {
    const value = { type: "response.output_text.delta", delta: "中文é" };
    const expected = JSON.stringify(value);
    const result = serializeWsOutput(value);
    expect(result).toEqual({
      payload: expected,
      bytes: Buffer.byteLength(expected),
      withinLimit: true,
    });
    expect(serializeWsOutput(expected)).toEqual(result);
    expect(serializeWsOutput(null).payload).toBe("null");
  });

  it.each([0, 1])("checks the complete wire message at the 8 MiB boundary plus %i", (extra) => {
    const overhead = Buffer.byteLength(JSON.stringify({ text: "" }));
    const result = serializeWsOutput({ text: "x".repeat(8 * MIB - overhead + extra) });
    expect(result.bytes).toBe(8 * MIB + extra);
    expect(result.withinLimit).toBe(extra === 0);
  });
});

describe("bounded SSE output buffer", () => {
  it("returns empty for an empty stream, including after end", () => {
    const buffer = createSseOutputBuffer();
    expect(buffer.bytes).toBe(0);
    expect(buffer.next()).toEqual({ kind: "empty" });
    buffer.end();
    expect(buffer.next()).toEqual({ kind: "empty" });
  });

  it("retains partial events and drains several complete events in order", () => {
    const buffer = createSseOutputBuffer();
    const first = "data: first";
    const second = "data: second";
    expect(buffer.append(first.slice(0, 5)).accepted).toBe(true);
    expect(buffer.next()).toEqual({ kind: "incomplete" });
    buffer.append(`${first.slice(5)}\n\n${second}\n\nlast`);
    expect(buffer.next()).toEqual({ kind: "event", text: first, bytes: first.length });
    expect(buffer.next()).toEqual({ kind: "event", text: second, bytes: second.length });
    expect(buffer.bytes).toBe(4);
    expect(buffer.next()).toEqual({ kind: "incomplete" });
    buffer.end();
    expect(buffer.next()).toEqual({ kind: "event", text: "last", bytes: 4 });
    expect(buffer.bytes).toBe(0);
    expect(buffer.next()).toEqual({ kind: "empty" });
  });

  it("recognizes CRLF delimiters split across successive chunks", () => {
    const buffer = createSseOutputBuffer();
    const text = "data: 中文\r\ndata: é";
    for (const part of [text, "\r", "\n", "\r"]) {
      buffer.append(part);
      expect(buffer.next().kind).toBe("incomplete");
    }
    buffer.append("\n");
    expect(buffer.next()).toEqual({ kind: "event", text, bytes: Buffer.byteLength(text) });
    expect(buffer.next()).toEqual({ kind: "empty" });
  });

  it("uses bytes rather than character count for terminated and unterminated events", () => {
    for (const suffix of ["", "\n\n"]) {
      const buffer = createSseOutputBuffer({ maxEventBytes: 8, maxBufferedBytes: 32 });
      buffer.append(`中文中${suffix}`);
      expect(buffer.next()).toEqual({ kind: "event_too_large", bytes: 9 });
      buffer.clear();
      expect(buffer.bytes).toBe(0);
      expect(buffer.next()).toEqual({ kind: "empty" });
    }
  });

  it("accepts an event exactly at its byte limit", () => {
    const buffer = createSseOutputBuffer({ maxEventBytes: 8, maxBufferedBytes: 32 });
    buffer.append("中文ab\n\n");
    expect(buffer.next()).toEqual({ kind: "event", text: "中文ab", bytes: 8 });
  });

  it.each([
    ["\n", "\n"],
    ["\r", "\n\r\n"],
    ["\r\n", "\r\n"],
    ["\r\n\r", "\n"],
  ])("does not count partial delimiter %j against the event byte cap", (prefix, suffix) => {
    const buffer = createSseOutputBuffer({ maxEventBytes: 8, maxBufferedBytes: 32 });
    buffer.append(`12345678${prefix}`);
    expect(buffer.next().kind).toBe("incomplete");
    buffer.append(suffix);
    expect(buffer.next()).toEqual({ kind: "event", text: "12345678", bytes: 8 });
    expect(buffer.next()).toEqual({ kind: "empty" });
  });

  it("counts a surrogate pair split between chunks as one UTF-8 code point", () => {
    const buffer = createSseOutputBuffer({ maxEventBytes: 4, maxBufferedBytes: 32 });
    const text = String.fromCodePoint(0x1d11e);
    buffer.append(text.slice(0, 1));
    expect(buffer.bytes).toBe(3);
    buffer.append(text.slice(1));
    expect(buffer.bytes).toBe(4);
    buffer.append("\n\n");
    expect(buffer.next()).toEqual({ kind: "event", text, bytes: 4 });
  });

  it("rejects gross backlog without appending over-budget data", () => {
    const buffer = createSseOutputBuffer({ maxEventBytes: 16, maxBufferedBytes: 32 });
    buffer.append("x".repeat(16));
    const rejected = buffer.append("x".repeat(17));
    expect(rejected.accepted).toBe(false);
    expect(buffer.bytes).toBe(16);
    buffer.clear();
    expect(buffer.append("x".repeat(32)).accepted).toBe(true);
    expect(buffer.bytes).toBe(32);
  });

  it("resets fragmented delimiter state when cleared", () => {
    const buffer = createSseOutputBuffer();
    buffer.append("old\r\n\r");
    expect(buffer.next().kind).toBe("incomplete");
    buffer.clear();
    buffer.append("new\n\n");
    expect(buffer.next()).toEqual({ kind: "event", text: "new", bytes: 3 });
  });

  it("leaves an abnormal source tail incomplete while retaining complete earlier events", () => {
    const buffer = createSseOutputBuffer();
    buffer.append("complete\n\npartial");
    buffer.end({ allowUnterminated: false });
    expect(buffer.next()).toEqual({ kind: "event", text: "complete", bytes: 8 });
    expect(buffer.next()).toEqual({ kind: "incomplete" });
  });

  it("does not confuse event aggregation with the complete stream byte count", () => {
    const buffer = createSseOutputBuffer({ maxEventBytes: 4, maxBufferedBytes: 32 });
    for (let i = 0; i < 50; i += 1) {
      buffer.append("text\n\n");
      expect(buffer.next()).toEqual({ kind: "event", text: "text", bytes: 4 });
      expect(buffer.bytes).toBe(0);
    }
  });
});
