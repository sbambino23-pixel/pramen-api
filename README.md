# prAmen API

Prayer Circle backend for the prAmen app.

## Deploy to Railway

1. Create a **new, separate** GitHub repo called `pramen-api`
2. Push ONLY this folder's contents to that repo
3. In Railway: **New Project → Deploy from GitHub → select `pramen-api`**
4. Railway will detect Node.js via `nixpacks.toml` and auto-build
5. Go to **Settings → Networking → Generate Domain**
6. Copy the URL and set it in your iOS app's `Config.swift`

## Local dev

```bash
npm install
npm run dev
```

## Endpoints

- `GET /` — Health check
- `POST /api/circles` — Create circle
- `GET /api/circles/:code` — Get circle
- `POST /api/circles/:code/join` — Join circle
- `PUT /api/circles/:code` — Update circle
- `PUT /api/circles/:code/members/:userId/status` — Update member
- `DELETE /api/circles/:code/members/:userId` — Leave circle
- `DELETE /api/circles/:code` — Delete circle
- `POST /api/circles/:code/prayer-requests` — Add prayer request
- `POST /api/circles/:code/prayer-requests/:id/pray` — Mark praying
- `POST /api/circles/:code/encouragements` — Send encouragement
