import type { ReactNode } from 'react';
import type { MessageSummary } from '../api/types';
import { formatTimestamp } from '../util/format';
import { MessageBodyTabs } from './MessageBodyTabs';

interface MessageDetailProps {
  readonly message: MessageSummary | undefined;
}

export function MessageDetail({ message }: MessageDetailProps): ReactNode {
  if (!message) {
    return (
      <aside className="detail" aria-label="Message detail">
        <div className="state state--empty" role="status">
          <p>Select a message to view its details.</p>
        </div>
      </aside>
    );
  }

  return (
    <aside className="detail" aria-label="Message detail">
      <h2 className="detail__title">Message detail</h2>
      <dl className="detail__meta">
        <dt>Document ID</dt>
        <dd className="cell-mono">{message.documentId}</dd>
        <dt>Source format</dt>
        <dd>{message.sourceFormat}</dd>
        <dt>Document time</dt>
        <dd>{formatTimestamp(message.documentTime)}</dd>
        <dt>Ingested time</dt>
        <dd>{formatTimestamp(message.ingestedTime)}</dd>
      </dl>
      <MessageBodyTabs messageId={message.documentId} />
    </aside>
  );
}
