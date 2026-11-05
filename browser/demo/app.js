import { fileProfiles, uploadStream, downloadTo, objectUrl, streamCapabilities } from '/dist/index.js';
import { runBrowserChecks } from '/demo/browser-checks.js';
import { runJavaIntegrationChecks } from '/demo/java-integration-checks.js';
const base = document.querySelector('#base');
base.value = location.origin + '/objects';
const status = document.querySelector('#status');
let active;
const show = text => { status.textContent = text; };
document.querySelector('#abort').onclick = () => active?.abort('User cancelled');
document.querySelector('#upload').onclick = async () => {
  const file = document.querySelector('#file').files[0];
  if (!file) { show('Choose a file first.'); return; }
  active?.abort(); active = new AbortController();
  try {
    const key = document.querySelector('#key').value;
    const commit = await uploadStream({ url: objectUrl(base.value, key), key, source: file.stream(),
      knownLength: file.size, maxBytes: fileProfiles.media.maxBytes, signal: active.signal,
      headers: { 'Content-Type': file.type || 'application/octet-stream' },
      onProgress(progress) {
        show(progress.phase === 'reading' ? `Source read: ${progress.bytesRead}/${file.size} bytes. Commit not confirmed.`
          : `Commit confirmed: ${progress.commit.bytes} bytes; ETag ${progress.commit.etag}`);
      } });
    show(JSON.stringify(commit, null, 2));
  } catch (error) { show(`${error.code ?? error.name}: ${error.message}\nUpload outcome: ${error.uploadOutcome ?? 'not reported'}`); }
};
const downloadButton = document.querySelector('#download');
if (typeof window.showSaveFilePicker !== 'function') {
  downloadButton.disabled = true;
  downloadButton.title = 'Provide a WritableStream in application code; no whole-Blob fallback is provided.';
}
downloadButton.onclick = async () => {
  let destination;
  try {
    const key = document.querySelector('#key').value;
    const handle = await window.showSaveFilePicker({ suggestedName: key.split('/').pop() || 'download.bin' });
    destination = await handle.createWritable();
    active?.abort(); active = new AbortController();
    const result = await downloadTo({ url: objectUrl(base.value, key), destination,
      maxBytes: fileProfiles.media.maxBytes, signal: active.signal,
      onProgress(progress) { show(`Sink accepted: ${progress.bytesWritten}/${progress.responseBytes} bytes.`); } });
    show(JSON.stringify(result, null, 2));
  } catch (error) {
    if (destination && !destination.locked) await destination.abort().catch(() => {});
    show(`${error.code ?? error.name}: ${error.message}`);
  }
};
document.querySelector('#checks').onclick = async event => {
  event.target.disabled = true;
  const target = document.querySelector('#results');
  try {
    const results = await runBrowserChecks(message => { target.textContent = message; });
    window.__streamResults = results;
    target.textContent = JSON.stringify(results, null, 2);
  } catch (error) {
    window.__streamResults = { failed: error.message };
    target.textContent = error.stack ?? error.message;
  } finally { event.target.disabled = false; }
};
window.__streamCapabilities = streamCapabilities();
document.querySelector('#java-checks').onclick = async event => {
  event.target.disabled = true;
  const target = document.querySelector('#java-results');
  try {
    const results = await runJavaIntegrationChecks(base.value, message => { target.textContent = message; });
    window.__javaStreamResults = results;
    target.textContent = JSON.stringify(results, null, 2);
  } catch (error) {
    window.__javaStreamResults = { failed: error.message };
    target.textContent = error.stack ?? error.message;
  } finally { event.target.disabled = false; }
};
