/**
 * Pure, UI-independent logic for the report-level destructive and full-definition
 * edit flows. Kept separate from the React components so it can be reasoned about
 * (and, if a frontend test runner is ever configured, tested) in isolation. Nothing
 * here logs or transmits its inputs.
 */

/**
 * Whether the text a user typed into the delete confirmation field exactly matches the
 * target report id. The match is strict (no trimming or case folding) so a destructive
 * delete only unlocks on a byte-for-byte reproduction of the id.
 */
export function deleteConfirmationMatches(
  typed: string,
  reportId: string,
): boolean {
  return typed === reportId;
}

/** Classification of the full report-definition editor text. */
export type ReportDraftParse =
  | { readonly kind: 'invalid-json' }
  | { readonly kind: 'not-object' }
  | { readonly kind: 'id-mismatch'; readonly actual: string | undefined }
  | { readonly kind: 'valid'; readonly value: Record<string, unknown> };

/** True for a non-null, non-array object value. */
function isPlainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

/**
 * Parse and validate the full-definition editor text against the report being edited.
 * The text must be a JSON object whose `report_id` exactly equals `expectedReportId`;
 * the report id cannot be changed through this editor. Any other shape is rejected so an
 * invalid or mis-targeted definition never reaches the backend.
 */
export function parseReportDefinitionDraft(
  text: string,
  expectedReportId: string,
): ReportDraftParse {
  let value: unknown;
  try {
    value = JSON.parse(text);
  } catch {
    return { kind: 'invalid-json' };
  }
  if (!isPlainObject(value)) {
    return { kind: 'not-object' };
  }
  const actual = value.report_id;
  if (typeof actual !== 'string' || actual !== expectedReportId) {
    return {
      kind: 'id-mismatch',
      actual: typeof actual === 'string' ? actual : undefined,
    };
  }
  return { kind: 'valid', value };
}
