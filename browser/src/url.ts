/** Encode a logical object key as one URL segment; do not encode it again at the caller. */
export function objectUrl(objectsBaseUrl: string | URL, key: string): URL {
  if (typeof key !== 'string' || !key || key === '.' || key === '..')
    throw new TypeError('key must be non-empty and cannot be a URL dot segment');
  const base = new URL(objectsBaseUrl);
  if (base.search || base.hash) throw new TypeError('objectsBaseUrl must not contain a query or fragment');
  base.pathname = base.pathname.replace(/\/$/, '') + '/' + encodeURIComponent(key);
  return base;
}
