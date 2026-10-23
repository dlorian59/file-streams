export interface StreamCapabilities {
  readonly requestStreamSyntax: boolean;
  readonly responseStreams: boolean;
  /** A constructor probe cannot verify browser, server or proxy behavior end to end. */
  readonly transportVerified: false;
}

/** No network request is made. A successful probe is necessary, but not sufficient. */
export function streamCapabilities(): StreamCapabilities {
  let readsDuplex = false;
  let requestStreamSyntax = false;
  try {
    const body = new ReadableStream<Uint8Array>({ start(controller) { controller.close(); } });
    const init: RequestInit & { duplex: 'half' } = {
      method: 'PUT', body,
      get duplex() { readsDuplex = true; return 'half' as const; },
    };
    const probe = new Request('https://stream-capability.invalid/', init);
    requestStreamSyntax = readsDuplex && probe.body !== null && !probe.headers.has('content-type');
  } catch { /* absent API or request stream rejected by the runtime */ }
  return Object.freeze({ requestStreamSyntax,
    responseStreams: typeof ReadableStream !== 'undefined' && typeof WritableStream !== 'undefined'
      && typeof Response !== 'undefined' && 'body' in Response.prototype,
    transportVerified: false as const });
}
