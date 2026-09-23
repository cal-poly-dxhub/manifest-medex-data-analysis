import {
  useEffect,
  useRef,
  type KeyboardEvent as ReactKeyboardEvent,
  type MouseEvent as ReactMouseEvent,
  type ReactNode,
  type RefObject,
} from 'react';

interface DetailDrawerProps {
  /** Accessible name for the dialog. Also used as the drawer's aria-label. */
  readonly title: string;
  /** Called when the drawer requests to close (Escape, backdrop click, or ✕). */
  readonly onClose: () => void;
  /**
   * The element focus should return to on close — typically the currently selected
   * list/table row. Focus is only restored when the element is still connected to the
   * document; otherwise the element focused before the drawer opened is used.
   */
  readonly returnFocusRef?: RefObject<HTMLElement | null>;
  /**
   * Selects the previous (-1) or next (+1) item while the drawer is open. Parents clamp
   * at the list edges and update both the drawer contents and the return-focus target in
   * place. When omitted, ArrowUp/ArrowDown are left to their default behavior.
   */
  readonly onNavigate?: (delta: -1 | 1) => void;
  readonly children: ReactNode;
}

/** Tags whose own key handling (caret movement, option cycling) must not be hijacked. */
const INTERACTIVE_TAGS: ReadonlySet<string> = new Set([
  'INPUT',
  'TEXTAREA',
  'SELECT',
]);

/**
 * A fixed, right-aligned slide-over panel that renders arbitrary detail content over a
 * dismissable backdrop. It is intentionally non-modal (`aria-modal="false"`): focus is
 * moved to the close button on open and restored on close, but it is not trapped, so the
 * underlying view stays operable.
 *
 * Only rendered by callers when there is something to show, so the drawer never appears
 * empty. Escape and a backdrop click both close it. ArrowUp/ArrowDown move the selection
 * through the caller's list when {@link DetailDrawerProps.onNavigate} is provided.
 */
export function DetailDrawer({
  title,
  onClose,
  returnFocusRef,
  onNavigate,
  children,
}: DetailDrawerProps): ReactNode {
  const closeButtonRef = useRef<HTMLButtonElement | null>(null);
  // The element focused before the drawer opened, used as a return-focus fallback.
  const previousFocusRef = useRef<HTMLElement | null>(null);
  // The most recent connected return-focus target seen while the drawer was open.
  const lastReturnTargetRef = useRef<HTMLElement | null>(null);

  // Capture the previously focused element, move focus into the drawer, and restore
  // focus to the selected row (or the prior element) when the drawer unmounts.
  useEffect(() => {
    previousFocusRef.current =
      document.activeElement instanceof HTMLElement
        ? document.activeElement
        : null;
    closeButtonRef.current?.focus();
    return () => {
      const returnTarget = lastReturnTargetRef.current;
      if (returnTarget && returnTarget.isConnected) {
        returnTarget.focus();
        return;
      }
      const previous = previousFocusRef.current;
      if (previous && previous.isConnected) {
        previous.focus();
      }
    };
    // Mount/unmount only: the return-focus target is tracked via the effect below.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // Track the latest connected return-focus target on every render so it survives the
  // selection being cleared on close (when the row ref is detached before unmount).
  useEffect(() => {
    const current = returnFocusRef?.current ?? null;
    if (current && current.isConnected) {
      lastReturnTargetRef.current = current;
    }
  });

  const handleKeyDown = (event: ReactKeyboardEvent<HTMLDivElement>): void => {
    if (event.key === 'Escape') {
      event.stopPropagation();
      onClose();
      return;
    }
    if (event.key !== 'ArrowUp' && event.key !== 'ArrowDown') {
      return;
    }
    if (!onNavigate) {
      return;
    }
    const target = event.target as HTMLElement;
    if (INTERACTIVE_TAGS.has(target.tagName)) {
      return;
    }
    event.preventDefault();
    onNavigate(event.key === 'ArrowDown' ? 1 : -1);
  };

  const handleBackdropClick = (event: ReactMouseEvent<HTMLDivElement>): void => {
    if (event.target === event.currentTarget) {
      onClose();
    }
  };

  return (
    <div
      className="detail-drawer__backdrop"
      role="presentation"
      onClick={handleBackdropClick}
      onKeyDown={handleKeyDown}
    >
      <aside
        className="detail-drawer"
        role="dialog"
        aria-modal="false"
        aria-label={title}
      >
        <header className="detail-drawer__header">
          <button
            ref={closeButtonRef}
            type="button"
            className="detail-drawer__close"
            aria-label="Close detail"
            onClick={onClose}
          >
            ✕
          </button>
        </header>
        <div className="detail-drawer__body">{children}</div>
      </aside>
    </div>
  );
}
