import { EventEmitter } from "node:events";
import http from "node:http";
import { createRequire } from "node:module";
import { Socket } from "node:net";
import { afterEach, describe, expect, it, vi } from "vitest";

const requireFromHere = createRequire(import.meta.url);

type WebSocketLike = {
  readyState: number;
  send: (payload: string, callback?: (error?: Error) => void) => void;
  close: (code: number, reason: string) => void;
};

type ServerModule = {
  handleWebSocketConnection: (
    ws: WebSocketLike & EventEmitter,
    request: { headers: Record<string, string>; url: string }
  ) => Promise<void>;
  forwardToInternalHttp: (
    ws: WebSocketLike,
    request: { headers: Record<string, string>; url: string },
    body: Record<string, unknown>,
    sessionId: string,
    registerRequest?: (
      request: http.ClientRequest,
      response?: http.IncomingMessage | null,
      settleTurn?: () => boolean
    ) => boolean | undefined,
    close?: (code: number, reason: string) => void,
    internalHttpTarget?: { hostname: string; port: number }
  ) => Promise<void>;
};

const serverModule: ServerModule = requireFromHere("../../server.js");

function createClientRequest(writeResult: boolean, events: string[]): http.ClientRequest {
  const request: http.ClientRequest = Object.create(http.ClientRequest.prototype);
  EventEmitter.call(request);
  Object.assign(request, {
    destroyed: false,
    write: () => {
      events.push("write");
      return writeResult;
    },
    end: () => {
      events.push("end");
      return request;
    },
    destroy: () => {
      events.push("destroy");
      request.destroyed = true;
      return request;
    },
  });
  return request;
}

function createIncomingResponse(): http.IncomingMessage {
  const response = new http.IncomingMessage(new Socket());
  response.headers = { "content-type": "text/event-stream" };
  vi.spyOn(response, "pause");
  vi.spyOn(response, "resume");
  return response;
}

function requestInput() {
  return {
    ws: {
      readyState: 1,
      send: vi.fn((_payload: string, callback?: (error?: Error) => void) => callback?.()),
      close: vi.fn(),
    },
    request: { headers: { authorization: "Bearer test" }, url: "/v1/responses" },
    body: { model: "gpt-5.5", input: "hello" },
  };
}

function createWebSocket(send: WebSocketLike["send"]) {
  return Object.assign(new EventEmitter(), { readyState: 1, send, close: vi.fn() });
}

function forwardRequest(
  request: http.ClientRequest,
  close?: (code: number, reason: string) => void
) {
  vi.spyOn(http, "request").mockImplementation(() => request);
  const input = requestInput();
  return serverModule.forwardToInternalHttp(
    input.ws,
    input.request,
    input.body,
    "test-session",
    undefined,
    close
  );
}

async function startSseBridge(
  send: WebSocketLike["send"],
  contentType = "text/event-stream",
  statusCode = 200,
  writeResult = true
) {
  const events: string[] = [];
  const request = createClientRequest(writeResult, events);
  const response = createIncomingResponse();
  response.headers = { "content-type": contentType };
  response.statusCode = statusCode;
  let respond: ((response: http.IncomingMessage) => void) | undefined;
  vi.spyOn(http, "request").mockImplementation((_options, callback) => {
    if (callback) respond = callback;
    return request;
  });
  const ws = createWebSocket(send);
  await serverModule.handleWebSocketConnection(ws, {
    headers: { host: "localhost" },
    url: "/v1/responses",
  });
  ws.emit(
    "message",
    Buffer.from('{"type":"response.create","model":"gpt-5.5","input":"hello"}'),
    false
  );
  await Promise.resolve();
  respond?.(response);
  return { events, request, response, ws };
}

afterEach(() => {
  vi.useRealTimers();
  vi.restoreAllMocks();
});

describe("server response write backpressure", () => {
  it("delivers a terminal response larger than the former 1 MiB limit", async () => {
    const sent: string[] = [];
    const bridge = await startSseBridge((payload, callback) => {
      sent.push(payload);
      callback?.();
    });
    try {
      const text = "x".repeat(2 * 1024 * 1024);
      bridge.response.emit(
        "data",
        `data: ${JSON.stringify({ type: "response.completed", response: { output: text } })}\n\n`
      );
      bridge.response.emit("end");
      await new Promise<void>((resolve) => setImmediate(resolve));

      expect(sent.map((payload) => JSON.parse(payload).type)).toEqual(["response.completed"]);
      expect(JSON.parse(sent[0]).response.output.length).toBe(text.length);
      expect(bridge.ws.close).not.toHaveBeenCalled();
    } finally {
      bridge.ws.emit("close");
    }
  });

  it("uses the explicit per-worker loopback target instead of the shared public port", async () => {
    const events: string[] = [];
    const request = createClientRequest(true, events);
    let capturedOptions: http.RequestOptions | undefined;
    vi.spyOn(http, "request").mockImplementation((options) => {
      capturedOptions = options as http.RequestOptions;
      setImmediate(() => request.emit("error", new Error("test settlement")));
      return request;
    });
    const input = requestInput();

    await serverModule.forwardToInternalHttp(
      input.ws,
      input.request,
      input.body,
      "test-session",
      undefined,
      undefined,
      { hostname: "127.0.0.1", port: 43123 }
    );

    expect(capturedOptions).toMatchObject({ hostname: "127.0.0.1", port: 43123 });
  });

  it("waits for request drain before ending a backpressured payload", async () => {
    const events: string[] = [];
    const request = createClientRequest(false, events);
    const forwarding = forwardRequest(request);

    expect(events).toEqual(["write"]);
    request.emit("drain");
    expect(events).toEqual(["write", "end"]);
    request.emit("error", Object.assign(new Error("closed"), { code: "ECONNRESET" }));
    await forwarding;
  });

  it.each(["ECONNREFUSED", "ECONNRESET"])(
    "sends one fatal frame and waits for its acknowledgement on active request error %s",
    async (code) => {
      const events: string[] = [];
      const request = createClientRequest(false, events);
      vi.spyOn(http, "request").mockImplementation(() => request);
      const input = requestInput();
      const sent: string[] = [];
      let sendCallback: ((error?: Error) => void) | undefined;
      input.ws.send = (payload, callback) => {
        sent.push(payload);
        sendCallback = callback;
      };
      const close = vi.fn();

      const forwarding = serverModule.forwardToInternalHttp(
        input.ws,
        input.request,
        input.body,
        "request-error-session",
        undefined,
        close
      );
      let settled = false;
      void forwarding.then(() => {
        settled = true;
      });

      request.emit("error", Object.assign(new Error(code), { code }));
      await new Promise<void>((resolve) => setImmediate(resolve));

      expect(sent).toHaveLength(1);
      expect(JSON.parse(sent[0]).error.code).toBe("internal_request_error");
      expect(settled).toBe(false);
      expect(close).not.toHaveBeenCalled();

      sendCallback?.();
      await forwarding;
      expect(close).toHaveBeenCalledWith(1011, "internal_request_error");

      expect(() => request.emit("error", new Error("late request error"))).not.toThrow();
      expect(sent).toHaveLength(1);
    }
  );

  it("force-settles an active turn without relying on request destroy events", async () => {
    const events: string[] = [];
    const request = createClientRequest(false, events);
    vi.spyOn(http, "request").mockImplementation(() => request);
    const input = requestInput();
    let settleTurn: (() => boolean) | undefined;

    const forwarding = serverModule.forwardToInternalHttp(
      input.ws,
      input.request,
      input.body,
      "force-settle-session",
      (_request, _response, settle) => {
        settleTurn = settle;
        return true;
      }
    );

    expect(settleTurn?.()).toBe(true);
    await forwarding;
    expect(events).toContain("destroy");
    expect(input.ws.send).not.toHaveBeenCalled();
    expect(() => request.emit("error", new Error("late request error"))).not.toThrow();
  });

  it("lets close win before drain without ending twice or stranding completion", async () => {
    const events: string[] = [];
    const request = createClientRequest(false, events);
    const forwarding = forwardRequest(request);

    request.emit("close");
    await forwarding;

    request.emit("drain");
    expect(events).toEqual(["write"]);
  });

  it("terminates a request body when drain never arrives", async () => {
    vi.useFakeTimers();
    const events: string[] = [];
    const request = createClientRequest(false, events);
    const close = vi.fn();
    const forwarding = forwardRequest(request, close);
    await vi.advanceTimersByTimeAsync(60_000);
    await forwarding;

    expect(close.mock.calls).toEqual([[1011, "internal_request_drain_timeout"]]);
    request.emit("drain");
    request.emit("error", new Error("late error"));
    expect(events).toEqual(["write", "destroy"]);
  });

  it("serializes outbound sends and pauses SSE until callbacks release pressure", async () => {
    const events: string[] = [];
    const request = createClientRequest(true, events);
    const response = createIncomingResponse();
    let respond: ((response: http.IncomingMessage) => void) | undefined;
    vi.spyOn(http, "request").mockImplementation((_options, callback) => {
      if (callback) respond = callback;
      return request;
    });
    const callbacks: Array<(error?: Error) => void> = [];
    const sent: string[] = [];
    const input = requestInput();
    input.ws.send = (payload, callback) => {
      sent.push(payload);
      if (callback) callbacks.push(callback);
    };

    const forwarding = serverModule.forwardToInternalHttp(
      input.ws,
      input.request,
      input.body,
      "session-2"
    );
    respond?.(response);
    response.emit(
      "data",
      'data: {"type":"response.output_text.delta","delta":"a"}\n\n' +
        'data: {"type":"response.output_text.delta","delta":"b"}\n\n' +
        'data: {"type":"response.completed","response":{"id":"r1"}}\n\n'
    );

    expect(sent).toHaveLength(1);
    expect(response.pause).toHaveBeenCalled();
    callbacks.shift()?.();
    await new Promise<void>((resolve) => setImmediate(resolve));
    expect(sent).toHaveLength(2);
    callbacks.shift()?.();
    await new Promise<void>((resolve) => setImmediate(resolve));
    expect(sent).toHaveLength(3);
    callbacks.shift()?.();
    await new Promise<void>((resolve) => setImmediate(resolve));
    expect(response.resume).toHaveBeenCalled();

    response.emit("end");
    await forwarding;
    response.destroy();
  });

  it("invalidates late send callbacks and destroys both internal transports on close", async () => {
    const callbacks: Array<(error?: Error) => void> = [];
    const sent: string[] = [];
    const bridge = await startSseBridge((payload, callback) => {
      sent.push(payload);
      if (callback) callbacks.push(callback);
    });
    const destroyResponse = vi.spyOn(bridge.response, "destroy");
    bridge.response.emit(
      "data",
      'data: {"type":"response.output_text.delta","delta":"a"}\n\n' +
        'data: {"type":"response.completed","response":{"id":"r1"}}\n\n'
    );

    expect(sent).toHaveLength(1);
    bridge.ws.emit("close");
    expect(bridge.events).toContain("destroy");
    expect(destroyResponse).toHaveBeenCalledOnce();
    callbacks.shift()?.();
    expect(sent).toHaveLength(1);
  });

  it.each(["error", "timeout"] as const)("cleans up on send %s", async (failure) => {
    if (failure === "timeout") vi.useFakeTimers();
    let callback: ((error?: Error) => void) | undefined;
    const bridge = await startSseBridge((_payload, done) => {
      callback = done;
    });
    const destroyResponse = vi.spyOn(bridge.response, "destroy");
    bridge.response.emit("data", 'data: {"type":"response.output_text.delta","delta":"a"}\n\n');

    if (failure === "error") callback?.(new Error("send failed"));
    else await vi.advanceTimersByTimeAsync(60_000);

    expect(bridge.events).toContain("destroy");
    expect(destroyResponse).toHaveBeenCalledOnce();
    callback?.();
    expect(bridge.ws.close.mock.calls).toEqual([[1011, `outbound_send_${failure}`]]);
  });

  it("waits before materializing later events from the same HTTP chunk", async () => {
    const callbacks: Array<(error?: Error) => void> = [];
    const sent: string[] = [];
    const bridge = await startSseBridge((payload, callback) => {
      sent.push(payload);
      if (callback) callbacks.push(callback);
    });
    try {
      const delta = "x".repeat(600 * 1024);
      const event = `data: ${JSON.stringify({ type: "response.output_text.delta", delta })}\n\n`;
      bridge.response.emit("data", event + event);
      expect(sent).toHaveLength(1);
      expect(bridge.request.destroyed).toBe(false);
      callbacks.shift()?.();
      await new Promise<void>((resolve) => setImmediate(resolve));
      expect(sent).toHaveLength(2);
      expect(bridge.ws.close).not.toHaveBeenCalled();
      callbacks.shift()?.();
    } finally {
      bridge.ws.emit("close");
    }
  });

  it("sends a fatal terminal frame before initiating close", async () => {
    let callback: ((error?: Error) => void) | undefined;
    let payload = "";
    const bridge = await startSseBridge((sent, done) => {
      payload = sent;
      callback = done;
    });

    bridge.response.emit("end");

    expect(JSON.parse(payload).error.code).toBe("stream_ended_without_terminal");
    expect(bridge.ws.close).not.toHaveBeenCalled();
    callback?.();
    expect(bridge.ws.close.mock.calls).toEqual([[1011, "stream_ended_without_terminal"]]);
  });

  it("sends a protocol error frame before closing the client", async () => {
    const send = vi.fn<WebSocketLike["send"]>();
    const ws = createWebSocket(send);
    await serverModule.handleWebSocketConnection(ws, { headers: {}, url: "/v1/responses" });

    ws.emit("message", Buffer.from("binary"), true);

    const payload = send.mock.calls[0]?.[0] ?? "";
    const callback = send.mock.calls[0]?.[1];
    expect(JSON.parse(payload)).toMatchObject({ error: { code: "invalid_frame_type" } });
    expect(ws.close).not.toHaveBeenCalled();
    callback?.();
    expect(ws.close).toHaveBeenCalledWith(1003, "binary_not_supported");
  });

  it("aborts an active internal turn before a fatal protocol frame is acknowledged", async () => {
    const callbacks: Array<(error?: Error) => void> = [];
    const ws = createWebSocket((_payload, callback) => {
      if (callback) callbacks.push(callback);
    });
    const events: string[] = [];
    const request = createClientRequest(true, events);
    vi.spyOn(http, "request").mockImplementation(() => request);
    await serverModule.handleWebSocketConnection(ws, { headers: {}, url: "/v1/responses" });

    ws.emit(
      "message",
      Buffer.from('{"type":"response.create","model":"gpt-5.5","input":"hello"}'),
      false
    );
    await new Promise<void>((resolve) => setImmediate(resolve));
    ws.emit("message", Buffer.from("binary"), true);

    expect(request.destroyed).toBe(true);
    expect(ws.close).not.toHaveBeenCalled();
    callbacks.at(-1)?.();
    expect(ws.close).toHaveBeenCalledWith(1003, "binary_not_supported");
  });

  it("settles an SSE turn when terminal acknowledgement precedes a premature close", async () => {
    const events: string[] = [];
    const request = createClientRequest(true, events);
    const response = createIncomingResponse();
    response.complete = false;
    let respond: ((response: http.IncomingMessage) => void) | undefined;
    vi.spyOn(http, "request").mockImplementation((_options, callback) => {
      if (callback) respond = callback;
      return request;
    });
    const close = vi.fn();
    const input = requestInput();
    const forwarding = serverModule.forwardToInternalHttp(
      input.ws,
      input.request,
      input.body,
      "terminal-close-session",
      undefined,
      close
    );
    respond?.(response);
    response.emit(
      "data",
      `data: ${JSON.stringify({ type: "response.completed", response: { id: "r1" } })}\n\n`
    );
    response.emit("close");

    await forwarding;
    expect(close).not.toHaveBeenCalled();
  });

  it("waits for the JSON terminal send acknowledgement across end and close", async () => {
    const events: string[] = [];
    const request = createClientRequest(true, events);
    const response = new http.IncomingMessage(new Socket());
    response.headers = { "content-type": "application/json" };
    let respond: ((response: http.IncomingMessage) => void) | undefined;
    vi.spyOn(http, "request").mockImplementation((_options, callback) => {
      if (callback) respond = callback;
      return request;
    });
    let sendCallback: ((error?: Error) => void) | undefined;
    const sent: string[] = [];
    const close = vi.fn();
    const input = requestInput();
    input.ws.send = (payload, callback) => {
      sent.push(payload);
      sendCallback = callback;
    };

    const forwarding = serverModule.forwardToInternalHttp(
      input.ws,
      input.request,
      input.body,
      "json-session",
      undefined,
      close
    );
    respond?.(response);
    response.emit("data", Buffer.from('{"id":"response-1"}'));
    response.emit("end");
    response.emit("close");

    let settled = false;
    void forwarding.then(() => {
      settled = true;
    });
    await new Promise<void>((resolve) => setImmediate(resolve));
    expect(settled).toBe(false);
    expect(sent.map((payload) => JSON.parse(payload).type)).toEqual(["response.completed"]);
    expect(close).not.toHaveBeenCalled();

    sendCallback?.();
    await forwarding;
    expect(close).not.toHaveBeenCalled();
  });

  it("does not start a request while a fatal protocol frame awaits acknowledgement", async () => {
    const callbacks: Array<(error?: Error) => void> = [];
    const ws = createWebSocket((_payload, callback) => {
      if (callback) callbacks.push(callback);
    });
    const requestSpy = vi
      .spyOn(http, "request")
      .mockImplementation(() => createClientRequest(true, []));
    await serverModule.handleWebSocketConnection(ws, { headers: {}, url: "/v1/responses" });

    ws.emit("message", Buffer.from("binary"), true);
    ws.emit(
      "message",
      Buffer.from('{"type":"response.create","model":"gpt-5.5","input":"hello"}'),
      false
    );
    await new Promise<void>((resolve) => setImmediate(resolve));

    expect(requestSpy).not.toHaveBeenCalled();
    callbacks.shift()?.();
    expect(ws.close).toHaveBeenCalledWith(1003, "binary_not_supported");
  });

  it.each([0, 1])("enforces the UTF-8 SSE event byte boundary plus %i", async (extra) => {
    const sent: string[] = [];
    const bridge = await startSseBridge((payload, callback) => {
      sent.push(payload);
      callback?.();
    });
    try {
      const empty = { type: "response.completed", response: { output: "" } };
      const prefixBytes = Buffer.byteLength(`data: ${JSON.stringify(empty)}`);
      empty.response.output = "x".repeat(8 * 1024 * 1024 - prefixBytes + extra);
      const event = `data: ${JSON.stringify(empty)}`;
      expect(Buffer.byteLength(event)).toBe(8 * 1024 * 1024 + extra);
      bridge.response.emit("data", `${event}\n\n`);
      bridge.response.emit("end");
      await new Promise<void>((resolve) => setImmediate(resolve));
      const result = JSON.parse(sent[0]);
      if (extra === 0) {
        expect(result.type).toBe("response.completed");
        expect(result.response.output.length).toBe(empty.response.output.length);
        expect(bridge.ws.close).not.toHaveBeenCalled();
      } else {
        expect(result.error.code).toBe("internal_sse_event_too_large");
        expect(bridge.ws.close).toHaveBeenCalledWith(1011, "internal_sse_event_too_large");
      }
    } finally {
      bridge.ws.emit("close");
    }
  });

  it.each([
    ["\n", "\n"],
    ["\r", "\n\r\n"],
    ["\r\n", "\r\n"],
    ["\r\n\r", "\n"],
  ])("accepts an exact 8 MiB event with delimiter split after %j", async (prefix, suffix) => {
    const sent: string[] = [];
    const bridge = await startSseBridge((payload, callback) => {
      sent.push(payload);
      callback?.();
    });
    try {
      const value = { type: "response.completed", response: { text: "" } };
      const overhead = Buffer.byteLength(`data: ${JSON.stringify(value)}`);
      value.response.text = "x".repeat(8 * 1024 * 1024 - overhead);
      bridge.response.emit("data", `data: ${JSON.stringify(value)}${prefix}`);
      expect(sent).toHaveLength(0);
      expect(bridge.ws.close).not.toHaveBeenCalled();
      bridge.response.emit("data", suffix);
      bridge.response.emit("end");
      await new Promise<void>((resolve) => setImmediate(resolve));
      expect(sent.map((payload) => JSON.parse(payload).type)).toEqual(["response.completed"]);
      expect(bridge.ws.close).not.toHaveBeenCalled();
    } finally {
      bridge.ws.emit("close");
    }
  });

  it("rejects multibyte events by bytes even when character count is below the cap", async () => {
    const sent: string[] = [];
    const bridge = await startSseBridge((payload, callback) => {
      sent.push(payload);
      callback?.();
    });
    try {
      const text = "中".repeat(3 * 1024 * 1024);
      const event = `data: ${JSON.stringify({ type: "response.completed", response: { text } })}`;
      expect(event.length).toBeLessThan(8 * 1024 * 1024);
      expect(Buffer.byteLength(event)).toBeGreaterThan(8 * 1024 * 1024);
      bridge.response.emit("data", event);
      await new Promise<void>((resolve) => setImmediate(resolve));
      expect(JSON.parse(sent[0]).error.code).toBe("internal_sse_event_too_large");
      expect(bridge.ws.close).toHaveBeenCalledOnce();
    } finally {
      bridge.ws.emit("close");
    }
  });

  it.each(["end", "close", "error"] as const)(
    "drains a buffered terminal after HTTP %s and waits for its send acknowledgement",
    async (ending) => {
      const callbacks: Array<(error?: Error) => void> = [];
      const sent: string[] = [];
      const bridge = await startSseBridge((payload, callback) => {
        sent.push(payload);
        if (callback) callbacks.push(callback);
      });
      try {
        const text = "x".repeat(2 * 1024 * 1024);
        bridge.response.emit(
          "data",
          `data: ${JSON.stringify({ type: "response.output_text.delta", delta: text })}\n\n` +
            'data: {"type":"response.completed","response":{"id":"first"}}\n\n'
        );
        bridge.response.complete = false;
        bridge.response.emit(ending, new Error("source closed after terminal bytes"));
        expect(sent.map((payload) => JSON.parse(payload).type)).toEqual([
          "response.output_text.delta",
        ]);
        bridge.ws.emit(
          "message",
          Buffer.from('{"type":"response.create","model":"gpt-5.5","input":"next"}'),
          false
        );
        expect(vi.mocked(http.request).mock.calls).toHaveLength(1);
        callbacks.shift()?.();
        await new Promise<void>((resolve) => setImmediate(resolve));
        expect(sent.map((payload) => JSON.parse(payload).type)).toEqual([
          "response.output_text.delta",
          "response.completed",
        ]);
        expect(vi.mocked(http.request).mock.calls).toHaveLength(1);
        expect(bridge.ws.close).not.toHaveBeenCalled();
        callbacks.shift()?.();
        await new Promise<void>((resolve) => setImmediate(resolve));
        expect(vi.mocked(http.request).mock.calls).toHaveLength(2);
        expect(bridge.ws.close).not.toHaveBeenCalled();
      } finally {
        bridge.ws.emit("close");
      }
    }
  );

  it("reports missing terminal only after the earlier event has been acknowledged", async () => {
    const callbacks: Array<(error?: Error) => void> = [];
    const sent: string[] = [];
    const bridge = await startSseBridge((payload, callback) => {
      sent.push(payload);
      if (callback) callbacks.push(callback);
    });
    try {
      bridge.response.emit("data", 'data: {"type":"response.output_text.delta","delta":"x"}\n\n');
      bridge.response.emit("end");
      expect(sent).toHaveLength(1);
      callbacks.shift()?.();
      await new Promise<void>((resolve) => setImmediate(resolve));
      expect(sent).toHaveLength(2);
      expect(JSON.parse(sent[1]).error.code).toBe("stream_ended_without_terminal");
      expect(bridge.ws.close).not.toHaveBeenCalled();
      callbacks.shift()?.();
      expect(bridge.ws.close).toHaveBeenCalledWith(1011, "stream_ended_without_terminal");
    } finally {
      bridge.ws.emit("close");
    }
  });

  it("drains thousands of same-chunk events with synchronous send callbacks", async () => {
    let count = 0;
    let lastType = "";
    const bridge = await startSseBridge((payload, callback) => {
      count += 1;
      lastType = JSON.parse(payload).type;
      callback?.();
    });
    try {
      const small = 'data: {"type":"response.output_text.delta","delta":"x"}\n\n';
      bridge.response.emit(
        "data",
        `${small.repeat(5000)}data: {"type":"response.completed","response":{"id":"last"}}\n\n`
      );
      bridge.response.emit("end");
      await new Promise<void>((resolve) => setImmediate(resolve));
      expect(count).toBe(5001);
      expect(lastType).toBe("response.completed");
      expect(bridge.ws.close).not.toHaveBeenCalled();
    } finally {
      bridge.ws.emit("close");
    }
  });

  it("decodes Unicode split inside a UTF-8 code point and fragmented CRLF separators", async () => {
    const sent: string[] = [];
    const bridge = await startSseBridge((payload, callback) => {
      sent.push(payload);
      callback?.();
    });
    try {
      const text = `中文${String.fromCodePoint(0x1d11e)}é`;
      const encoded = Buffer.from(
        `data: ${JSON.stringify({ type: "response.completed", response: { text } })}\r\n\r\n`
      );
      for (const byte of encoded) bridge.response.push(Buffer.from([byte]));
      bridge.response.push(null);
      await new Promise<void>((resolve) => setImmediate(resolve));
      expect(sent).toHaveLength(1);
      expect(JSON.parse(sent[0]).response.text).toBe(text);
      expect(bridge.ws.close).not.toHaveBeenCalled();
    } finally {
      bridge.ws.emit("close");
    }
  });

  it("delivers a raw JSON response above the former limit", async () => {
    const sent: string[] = [];
    const bridge = await startSseBridge((payload, callback) => {
      sent.push(payload);
      callback?.();
    }, "application/json");
    try {
      bridge.response.emit(
        "data",
        Buffer.from(JSON.stringify({ text: "x".repeat(2 * 1024 * 1024) }))
      );
      bridge.response.emit("end");
      await new Promise<void>((resolve) => setImmediate(resolve));
      expect(JSON.parse(sent[0]).type).toBe("response.completed");
      expect(JSON.parse(sent[0]).response.text.length).toBe(2 * 1024 * 1024);
      expect(bridge.ws.close).not.toHaveBeenCalled();
    } finally {
      bridge.ws.emit("close");
    }
  });

  it.each([0, 1])("checks wrapped JSON wire size at the byte boundary plus %i", async (extra) => {
    const sent: string[] = [];
    const bridge = await startSseBridge((payload, callback) => {
      sent.push(payload);
      callback?.();
    }, "application/json");
    try {
      const overhead = Buffer.byteLength(
        JSON.stringify({ type: "response.completed", response: { text: "" } })
      );
      const text = "x".repeat(8 * 1024 * 1024 - overhead + extra);
      const raw = Buffer.from(JSON.stringify({ text }));
      expect(raw.byteLength).toBeLessThan(8 * 1024 * 1024);
      bridge.response.emit("data", raw);
      bridge.response.emit("end");
      await new Promise<void>((resolve) => setImmediate(resolve));
      const result = JSON.parse(sent[0]);
      if (extra === 0) {
        expect(Buffer.byteLength(sent[0])).toBe(8 * 1024 * 1024);
        expect(result.type).toBe("response.completed");
        expect(bridge.ws.close).not.toHaveBeenCalled();
      } else {
        expect(result.error.code).toBe("internal_response_too_large");
        expect(bridge.ws.close).toHaveBeenCalledWith(1011, "internal_response_too_large");
      }
    } finally {
      bridge.ws.emit("close");
    }
  });

  it("checks invalid SSE text after JSON escaping expands the wire payload", async () => {
    const sent: string[] = [];
    const bridge = await startSseBridge((payload, callback) => {
      sent.push(payload);
      callback?.();
    });
    try {
      const event = `data: ${"\u0000".repeat(2 * 1024 * 1024)}`;
      expect(Buffer.byteLength(event)).toBeLessThan(8 * 1024 * 1024);
      bridge.response.emit("data", `${event}\n\n`);
      bridge.response.emit("end");
      await new Promise<void>((resolve) => setImmediate(resolve));
      expect(JSON.parse(sent[0]).error.code).toBe("internal_sse_event_too_large");
      expect(bridge.ws.close).toHaveBeenCalledWith(1011, "internal_sse_event_too_large");
    } finally {
      bridge.ws.emit("close");
    }
  });

  it.each(["close", "error"] as const)(
    "does not forward a truncated SSE event when the source emits %s",
    async (ending) => {
      const sent: string[] = [];
      const bridge = await startSseBridge((payload, callback) => {
        sent.push(payload);
        callback?.();
      });
      try {
        bridge.response.emit("data", 'data: {"type":"response.output_text.delta","delta":"partial');
        bridge.response.complete = false;
        bridge.response.emit(ending, new Error("source ended unexpectedly"));
        await new Promise<void>((resolve) => setImmediate(resolve));
        expect(sent.map((payload) => JSON.parse(payload).type)).toEqual(["error"]);
        expect(JSON.parse(sent[0]).error.code).toBe(
          ending === "close" ? "internal_response_closed" : "internal_response_error"
        );
        expect(bridge.ws.close).toHaveBeenCalledOnce();
      } finally {
        bridge.ws.emit("close");
      }
    }
  );

  it("preserves normal EOF flushing for a complete final event without a delimiter", async () => {
    const sent: string[] = [];
    const bridge = await startSseBridge((payload, callback) => {
      sent.push(payload);
      callback?.();
    });
    try {
      bridge.response.emit("data", 'data: {"type":"response.completed","response":{"id":"tail"}}');
      bridge.response.emit("end");
      await new Promise<void>((resolve) => setImmediate(resolve));
      expect(sent.map((payload) => JSON.parse(payload).type)).toEqual(["response.completed"]);
      expect(bridge.ws.close).not.toHaveBeenCalled();
    } finally {
      bridge.ws.emit("close");
    }
  });

  it("logs acknowledged large events with identifiers and sizes without response text", async () => {
    const writes: string[] = [];
    vi.spyOn(process.stdout, "write").mockImplementation((chunk) => {
      writes.push(String(chunk));
      return true;
    });
    let acknowledge: (() => void) | undefined;
    const bridge = await startSseBridge((_payload, callback) => {
      acknowledge = callback;
    });
    try {
      const secretMarker = "private-output-marker";
      bridge.response.emit(
        "data",
        `data: ${JSON.stringify({
          type: "response.completed",
          response: { text: secretMarker + "x".repeat(2 * 1024 * 1024) },
        })}\n\n`
      );
      bridge.response.emit("end");
      const entries = () =>
        writes.flatMap((line) => {
          try {
            return [JSON.parse(line) as Record<string, unknown>];
          } catch {
            return [];
          }
        });
      expect(entries().filter((entry) => entry.msg === "ws_response_event_forwarded")).toHaveLength(
        0
      );
      acknowledge?.();
      await new Promise<void>((resolve) => setImmediate(resolve));
      const forwarded = entries().filter((entry) => entry.msg === "ws_response_event_forwarded");
      expect(forwarded).toHaveLength(1);
      expect(forwarded[0]).toMatchObject({
        type: "response.completed",
        source: "sse",
        turnId: 1,
        limitBytes: 8 * 1024 * 1024,
      });
      expect(typeof forwarded[0].wsSessionId).toBe("string");
      expect(forwarded[0].bytes).toBeGreaterThan(1024 * 1024);
      expect(writes.join("")).not.toContain(secretMarker);
    } finally {
      bridge.ws.emit("close");
    }
  });

  it.each([
    ["request", "response"],
    ["response", "request"],
  ] as const)(
    "settles competing %s and %s errors once behind a pending send",
    async (first, second) => {
      const callbacks: Array<(error?: Error) => void> = [];
      const sent: string[] = [];
      const bridge = await startSseBridge((payload, callback) => {
        sent.push(payload);
        if (callback) callbacks.push(callback);
      });
      try {
        bridge.response.emit("data", 'data: {"type":"response.output_text.delta","delta":"x"}\n\n');
        for (const source of [first, second]) {
          bridge[source].emit("error", new Error(`${source} failed`));
        }
        expect(sent).toHaveLength(1);
        expect(bridge.ws.close).not.toHaveBeenCalled();
        callbacks.shift()?.();
        await new Promise<void>((resolve) => setImmediate(resolve));
        expect(sent.map((payload) => JSON.parse(payload).type)).toEqual([
          "response.output_text.delta",
          "error",
        ]);
        expect(JSON.parse(sent[1]).error.code).toBe(
          first === "request" ? "internal_request_error" : "internal_response_error"
        );
        callbacks.shift()?.();
        await new Promise<void>((resolve) => setImmediate(resolve));
        expect(bridge.ws.close).toHaveBeenCalledOnce();
        expect(bridge.events.filter((event) => event === "destroy")).toHaveLength(1);
        expect(() => bridge.request.emit("error", new Error("late request failure"))).not.toThrow();
        expect(() =>
          bridge.response.emit("error", new Error("late response failure"))
        ).not.toThrow();
        expect(sent).toHaveLength(2);
      } finally {
        bridge.ws.emit("close");
      }
    }
  );

  it.each([
    ["request", "response"],
    ["response", "request"],
  ] as const)(
    "preserves the first %s error after an early response while upload awaits drain",
    async (first, second) => {
      const callbacks: Array<(error?: Error) => void> = [];
      const sent: string[] = [];
      const bridge = await startSseBridge(
        (payload, callback) => {
          sent.push(payload);
          if (callback) callbacks.push(callback);
        },
        "text/event-stream",
        200,
        false
      );
      try {
        bridge.response.emit("data", 'data: {"type":"response.output_text.delta","delta":"x"}\n\n');
        for (const source of [first, second]) {
          bridge[source].emit("error", new Error(`${source} failed`));
        }
        expect(bridge.events).not.toContain("end");
        expect(sent).toHaveLength(1);
        expect(bridge.ws.close).not.toHaveBeenCalled();
        callbacks.shift()?.();
        await new Promise<void>((resolve) => setImmediate(resolve));
        expect(sent.map((payload) => JSON.parse(payload).type)).toEqual([
          "response.output_text.delta",
          "error",
        ]);
        const expectedCode =
          first === "request" ? "internal_request_error" : "internal_response_error";
        expect(JSON.parse(sent[1]).error.code).toBe(expectedCode);
        callbacks.shift()?.();
        await new Promise<void>((resolve) => setImmediate(resolve));
        expect(bridge.ws.close.mock.calls).toEqual([[1011, expectedCode]]);
        expect(bridge.events.filter((event) => event === "destroy")).toHaveLength(1);
        bridge.request.emit("drain");
        expect(bridge.events).not.toContain("end");
        expect(() => bridge.request.emit("error", new Error("late upload failure"))).not.toThrow();
        expect(() =>
          bridge.response.emit("error", new Error("late response failure"))
        ).not.toThrow();
        expect(sent).toHaveLength(2);
      } finally {
        bridge.ws.emit("close");
      }
    }
  );

  it.each(["close", "abort"] as const)(
    "settles upload %s after an early response behind the outstanding send",
    async (ending) => {
      const callbacks: Array<(error?: Error) => void> = [];
      const sent: string[] = [];
      const bridge = await startSseBridge(
        (payload, callback) => {
          sent.push(payload);
          if (callback) callbacks.push(callback);
        },
        "text/event-stream",
        200,
        false
      );
      try {
        bridge.response.emit("data", 'data: {"type":"response.output_text.delta","delta":"x"}\n\n');
        bridge.request.emit(ending);
        bridge.response.emit("error", new Error("response follows upload closure"));
        expect(sent).toHaveLength(1);
        expect(bridge.ws.close).not.toHaveBeenCalled();
        callbacks.shift()?.();
        await new Promise<void>((resolve) => setImmediate(resolve));
        expect(sent.map((payload) => JSON.parse(payload).type)).toEqual([
          "response.output_text.delta",
          "error",
        ]);
        expect(JSON.parse(sent[1]).error.code).toBe("internal_request_body_closed");
        callbacks.shift()?.();
        await new Promise<void>((resolve) => setImmediate(resolve));
        expect(bridge.ws.close.mock.calls).toEqual([[1011, "internal_request_body_closed"]]);
        expect(bridge.events.filter((event) => event === "destroy")).toHaveLength(1);
        bridge.request.emit("drain");
        expect(bridge.events).not.toContain("end");
        expect(sent).toHaveLength(2);
      } finally {
        bridge.ws.emit("close");
      }
    }
  );

  it("settles upload timeout after an early response without racing the pending send", async () => {
    vi.useFakeTimers();
    const callbacks: Array<(error?: Error) => void> = [];
    const sent: string[] = [];
    const bridge = await startSseBridge(
      (payload, callback) => {
        sent.push(payload);
        if (callback) callbacks.push(callback);
      },
      "text/event-stream",
      200,
      false
    );
    try {
      await vi.advanceTimersByTimeAsync(20_000);
      bridge.response.emit("data", 'data: {"type":"response.output_text.delta","delta":"x"}\n\n');
      await vi.advanceTimersByTimeAsync(10_001);
      expect(sent).toHaveLength(1);
      expect(bridge.ws.close).not.toHaveBeenCalled();
      callbacks.shift()?.();
      await vi.advanceTimersByTimeAsync(0);
      expect(sent.map((payload) => JSON.parse(payload).type)).toEqual([
        "response.output_text.delta",
        "error",
      ]);
      expect(JSON.parse(sent[1]).error.code).toBe("internal_request_drain_timeout");
      callbacks.shift()?.();
      await vi.advanceTimersByTimeAsync(0);
      expect(bridge.ws.close.mock.calls).toEqual([[1011, "internal_request_drain_timeout"]]);
      expect(bridge.events.filter((event) => event === "destroy")).toHaveLength(1);
      bridge.request.emit("drain");
      expect(bridge.events).not.toContain("end");
      await vi.advanceTimersByTimeAsync(60_000);
      expect(sent).toHaveLength(2);
      expect(bridge.ws.close).toHaveBeenCalledOnce();
    } finally {
      bridge.ws.emit("close");
    }
  });

  it.each(["only-done", "after-terminal"] as const)(
    "preserves %s compatibility without forwarding duplicate terminal frames",
    async (mode) => {
      const sent: string[] = [];
      const bridge = await startSseBridge((payload, callback) => {
        sent.push(payload);
        callback?.();
      });
      try {
        const prefix =
          mode === "after-terminal"
            ? 'data: {"type":"response.completed","response":{"id":"already-complete"}}\n\n'
            : "";
        bridge.response.emit("data", `${prefix}data:[DONE]\n\ndata: [DONE]\n\n`);
        bridge.response.emit("end");
        await new Promise<void>((resolve) => setImmediate(resolve));
        expect(sent.map((payload) => JSON.parse(payload).type)).toEqual(["response.completed"]);
        expect(JSON.parse(sent[0]).response).toEqual(
          mode === "after-terminal" ? { id: "already-complete" } : null
        );
        expect(bridge.ws.close).not.toHaveBeenCalled();
      } finally {
        bridge.ws.emit("close");
      }
    }
  );

  it("ignores comment and empty SSE events while combining multiple data lines", async () => {
    const sent: string[] = [];
    const bridge = await startSseBridge((payload, callback) => {
      sent.push(payload);
      callback?.();
    });
    try {
      bridge.response.emit(
        "data",
        ": keepalive\r\nid: ping\r\n\r\n\n\n" +
          'event: response.completed\r\ndata: {"type":"response.completed",\r\n' +
          'data:"response":{"id":"multi-data"}}\r\n\r\n'
      );
      bridge.response.emit("end");
      await new Promise<void>((resolve) => setImmediate(resolve));
      expect(sent.map((payload) => JSON.parse(payload).type)).toEqual(["response.completed"]);
      expect(JSON.parse(sent[0]).response.id).toBe("multi-data");
      expect(bridge.ws.close).not.toHaveBeenCalled();
    } finally {
      bridge.ws.emit("close");
    }
  });

  it("preserves whitespace and quoting when a non-JSON SSE event becomes a text delta", async () => {
    const sent: string[] = [];
    const bridge = await startSseBridge((payload, callback) => {
      sent.push(payload);
      callback?.();
    });
    try {
      bridge.response.emit("data", 'data:   raw "quoted"\ttext\n\ndata: [DONE]\n\n');
      bridge.response.emit("end");
      await new Promise<void>((resolve) => setImmediate(resolve));
      expect(sent.map((payload) => JSON.parse(payload).type)).toEqual([
        "response.output_text.delta",
        "response.completed",
      ]);
      expect(JSON.parse(sent[0]).delta).toBe('  raw "quoted"\ttext');
      expect(bridge.ws.close).not.toHaveBeenCalled();
    } finally {
      bridge.ws.emit("close");
    }
  });

  it("keeps large JSON HTTP error responses structured and the socket reusable", async () => {
    const sent: string[] = [];
    const bridge = await startSseBridge(
      (payload, callback) => {
        sent.push(payload);
        callback?.();
      },
      "application/json",
      429
    );
    try {
      bridge.response.emit(
        "data",
        Buffer.from(
          JSON.stringify({
            error: { code: "rate_limit_exceeded", message: "x".repeat(2 * 1024 * 1024) },
          })
        )
      );
      bridge.response.emit("end");
      await new Promise<void>((resolve) => setImmediate(resolve));
      expect(sent).toHaveLength(1);
      expect(JSON.parse(sent[0])).toMatchObject({
        type: "error",
        status: 429,
        error: { code: "rate_limit_exceeded" },
      });
      expect(JSON.parse(sent[0]).error.message.length).toBe(2 * 1024 * 1024);
      expect(bridge.ws.close).not.toHaveBeenCalled();
    } finally {
      bridge.ws.emit("close");
    }
  });

  it("bounds the HTTP error preview when JSON has no structured error object", async () => {
    const sent: string[] = [];
    const bridge = await startSseBridge(
      (payload, callback) => {
        sent.push(payload);
        callback?.();
      },
      "application/json",
      503
    );
    try {
      bridge.response.emit("data", Buffer.from("upstream plain failure ".repeat(100)));
      bridge.response.emit("end");
      await new Promise<void>((resolve) => setImmediate(resolve));
      expect(sent).toHaveLength(1);
      const value = JSON.parse(sent[0]);
      expect(value).toMatchObject({ type: "error", status: 503, error: { code: "http_503" } });
      expect(value.error.message.length).toBe(512);
      expect(bridge.ws.close).not.toHaveBeenCalled();
    } finally {
      bridge.ws.emit("close");
    }
  });

  it("preserves raw non-JSON success bodies inside the completed response envelope", async () => {
    const sent: string[] = [];
    const bridge = await startSseBridge((payload, callback) => {
      sent.push(payload);
      callback?.();
    }, "application/json");
    try {
      bridge.response.emit("data", Buffer.from("plain response"));
      bridge.response.emit("end");
      await new Promise<void>((resolve) => setImmediate(resolve));
      expect(sent.map((payload) => JSON.parse(payload).type)).toEqual(["response.completed"]);
      expect(JSON.parse(sent[0]).response).toEqual({ raw: "plain response" });
      expect(bridge.ws.close).not.toHaveBeenCalled();
    } finally {
      bridge.ws.emit("close");
    }
  });

  it("bounds raw backlog while a send callback is delayed", async () => {
    const callbacks: Array<(error?: Error) => void> = [];
    const sent: string[] = [];
    const bridge = await startSseBridge((payload, callback) => {
      sent.push(payload);
      if (callback) callbacks.push(callback);
    });
    try {
      bridge.response.emit("data", 'data: {"type":"response.output_text.delta","delta":"x"}\n\n');
      bridge.response.emit("data", "x".repeat(16 * 1024 * 1024 + 1));
      expect(bridge.response.destroyed).toBe(true);
      expect(sent).toHaveLength(1);
      callbacks.shift()?.();
      await new Promise<void>((resolve) => setImmediate(resolve));
      expect(sent).toHaveLength(2);
      expect(JSON.parse(sent[1]).error.code).toBe("internal_sse_event_too_large");
      callbacks.shift()?.();
      expect(bridge.ws.close).toHaveBeenCalledWith(1011, "internal_sse_event_too_large");
    } finally {
      bridge.ws.emit("close");
    }
  });
});
