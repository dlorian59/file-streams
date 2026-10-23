# HTTP contract — 0.1.0

[Guides EN / RU](README.md)

## Upload

`PUT /objects/{encoded-key}` consumes one raw body. Encode the entire key as one path segment. A new request overwrites the key according to storage policy; authorization and collision prevention belong to the application. There is no public part/session API.

- `Content-Type`: optional, defaults to `application/octet-stream`.
- `Content-Length`: optional HTTP framing length.
- `X-Upload-Length`: optional unsigned decimal Java long; browser sources use this because Fetch controls Content-Length. If both lengths exist they must match. Both shorter and longer bodies fail validation.
- Request metadata is not copied wholesale to S3; the WebFlux adapter passes no arbitrary metadata or authentication headers.

201 is sent only after SDK storage acknowledgement and validated source EOF:

```json
{"committed":true,"key":"example","bytes":123,"etag":"opaque-etag"}
```

`bytes` is a Java long. The browser module admits only safe integer sizes. `etag` is nonempty and opaque; multipart ETags are not content hashes.

Error shape:

```json
{"committed":false,"code":"LENGTH_MISMATCH","message":"Upload is shorter than declared"}
```

| Status / code | Meaning |
| --- | --- |
| 400 / INVALID_REQUEST | Invalid length or request metadata before upload |
| 400 / LENGTH_MISMATCH | Source shorter or longer than declared |
| 400 / BUFFER_TOO_LARGE | One source buffer exceeds its configured bound |
| 413 / TOO_LARGE | Maximum upload size exceeded |
| 503 / BUSY | Transfer admission limit reached; no unbounded waiting queue |
| 502 / STORAGE_FAILURE | Storage did not confirm the outcome; `committed` is **null**, not false |

`committed:false` means this request was rejected before its commit path. It does not assert that a pre-existing object is absent, nor that orphaned multipart parts are already removed. `committed:null`, network failure or cancellation during/after PUT may mean that the object was committed. Reconcile by an application-owned upload identity/key and storage metadata. Do not blindly replay an unknown outcome.

## Download

`GET /objects/{encoded-key}` streams bytes with Content-Length, Content-Type, ETag and Accept-Ranges: bytes. S3 HEAD and conditional GET with If-Match protect the metadata/body version relationship. No compression is added by the demo; objects with nonidentity S3 content encoding are rejected by the adapter.

Only one Range is supported: `bytes=first-last`, `bytes=first-`, or `bytes=-suffix`. The server clips an overlarge end/suffix to the object length. Empty objects have no satisfiable byte range. 206 includes the resolved Content-Range; invalid/multiple/unsatisfiable ranges return 416, with `Content-Range: bytes */total` when the size is known. Absent objects return 404; mutation between HEAD and GET returns 412. No multipart/byteranges response or If-Range negotiation is implemented.

Once response bytes have been sent, failures terminate the stream; a new JSON error cannot replace a committed HTTP response. A partially written destination remains the caller's responsibility.

## Browser and proxy requirements

CORS must allow PUT/GET/OPTIONS and Content-Type, X-Upload-Length, Range. Expose ETag, Content-Length, Content-Range, Accept-Ranges and Content-Encoding. Use an explicit application origin. Browser request streaming typically requires HTTP/2 or HTTP/3 with TLS and `duplex: 'half'`; full duplex response consumption is not promised. Disable request/response buffering and automatic compression on intermediary proxies, and test the actual deployed route. Only the direct local HTTP/2 route is verified here; no reverse proxy is certified.
