CRICHUB - realtime server (replaces Netlify + its database)
============================================================
One Node.js program: serves the website, pushes live updates over a WebSocket,
stores accounts/rooms/uploads on its own disk. No npm install, no database.

RUN IT ON YOUR OWN PC (to test)
  1. Install Node.js 18 or newer (nodejs.org)
  2. Open a terminal in this folder and run:   node server.js
  3. Open http://localhost:3000   (other devices on your Wi-Fi: http://YOUR-PC-IP:3000)

PUT IT ONLINE (pick one)
  A) Render.com  (easiest)
     - Upload this folder to a GitHub repo, then Render > New > Web Service > pick the repo
     - Runtime: Node   Build command: (leave empty)   Start command: node server.js
     - IMPORTANT: files on Render's FREE plan are wiped on every restart/redeploy and the
       app sleeps after 15 min idle. For real use choose a paid instance and add a Disk:
       Disks > Add disk > Mount path /data, then Environment > DATA_DIR = /data
  B) Any VPS / Oracle Cloud "Always Free" VM / Railway / Fly.io
     - Copy this folder, run:  node server.js   (keep it alive with pm2 or systemd)
     - Put Caddy or nginx in front for https (WebSockets must be allowed - they are by default)
     - Or use the included Dockerfile and mount a volume at /data

SETTINGS (environment variables)
  PORT      port to listen on (default 3000; hosts like Render set it for you)
  DATA_DIR  where db.json and uploaded files are kept (default ./data) - back this folder up

NOTES
  - Old finished rooms are deleted automatically after 2 days.
  - Accounts, rooms and uploads from the old Netlify site are NOT carried over.
  - Health check URL for hosts that ask for one: /healthz
