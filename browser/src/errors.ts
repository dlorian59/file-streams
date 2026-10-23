/** A failed PUT can have committed even when its response was not observed. */
export type UploadOutcome = 'not-started' | 'unknown' | 'rejected';
export type TransferErrorCode = 'INVALID_ARGUMENT' | 'UNSUPPORTED' | 'ABORTED' |
  'LIMIT_EXCEEDED' | 'LENGTH_MISMATCH' | 'HTTP_ERROR' | 'PROTOCOL_ERROR' |
  'SOURCE_ERROR' | 'SINK_ERROR' | 'NETWORK_ERROR';

export class TransferError extends Error {
  readonly code: TransferErrorCode;
  readonly uploadOutcome: UploadOutcome | undefined;
  readonly status: number | undefined;
  constructor(code: TransferErrorCode, message: string,
    details: { cause?: unknown; uploadOutcome?: UploadOutcome; status?: number } = {}) {
    super(message, { cause: details.cause });
    this.name = 'TransferError';
    this.code = code;
    this.uploadOutcome = details.uploadOutcome;
    this.status = details.status;
  }
}
