# PHI Explorer (`web/`)

React/Vite/TypeScript single-page app for browsing clinical-message metadata and fetching one message body on demand.

> **PHI handling:** The app does not log message bodies or document IDs, loads no external assets, and runs no analytics. Bodies are fetched only when a body tab is active and are discarded from the inactive query cache promptly.

## Install and build

Dependencies are pinned to exact versions in `package.json` and locked in `package-lock.json`.

```bash
npm ci
npm run typecheck
npm run build
```

For local development, run `npm run dev`. Set `VITE_DEV_API_TARGET` in `web/.env.local` to proxy `/api` to a backend.

## Runtime configuration

The app loads `/config.json` at startup so one build can be promoted between environments:

```json
{
  "authority": "https://cognito-idp.<region>.amazonaws.com/<userPoolId>",
  "clientId": "<public OIDC client id>",
  "redirectUri": "https://<cloudfront-host>/",
  "postLogoutRedirectUri": "https://<cloudfront-host>/",
  "apiBasePath": "/api"
}
```

These are public configuration values. The deployed `config.json` replaces the development placeholder under `public/`.

## Authentication

The app uses Cognito Hosted UI Authorization Code with PKCE and sends the access token as `Authorization: Bearer <token>` on every API request. Authenticated user/token data stays in memory. Only the transient OIDC state and PKCE verifier use tab-scoped `sessionStorage`, because they must survive the full-page Hosted UI redirect. Silent renewal is disabled.

## API contract

Same-origin calls are rooted at `/api`:

- `GET /messages?from&to&source_format&cursor&limit` returns `{ items, nextCursor, totalCount }`; `from` means `ingested_time >=` and `to` means `ingested_time <`.

The backend returns an exact filtered total on every page. The UI shows the current result range, total results, current/total page numbers, and numbered controls for pages whose keyset cursors have been discovered. Additional page numbers become reachable sequentially; pagination never uses `OFFSET`.
- `GET /messages/{documentId}` returns one metadata record.
- `POST /messages/{documentId}/body` with `{ "variant": "parsed" | "raw" }` returns content directly: JSON for `parsed`, text/XML for `raw`.
- `POST /query` with `{ "sql": "..." }` executes one unrestricted SQL statement and returns `{ columns, rows, numberOfRecordsUpdated }`.

The SQL query view displays a loading spinner during execution. Named queries can be saved, loaded, and deleted in browser `localStorage`; they are not synchronized and remain on the device until deleted. Do not save identifiers or clinical values on shared devices.

The backend cursor is newest-first and forward-only. The UI keeps prior cursors in memory to provide Previous navigation. Aurora cursor timestamps are normalized to UTC so Next works with offsetless Data API timestamp text.
