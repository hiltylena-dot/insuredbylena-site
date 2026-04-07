# insuredbylena-site

Static website for `insuredbylena.com`.

## Structure

- `/index.html` -> public landing page
- `/landing.css` -> landing page styles
- `/portal/` -> internal dashboard app
- `/.cpanel.yml` -> cPanel Git deploy tasks
- `/.github/workflows/deploy-release.yml` -> automatic upload to Namecheap on push to `main`

## Local preview

```bash
cd /Users/hankybot/Documents/Playground/insuredbylena-site
python3 -m http.server 8080
```

Open:

- http://127.0.0.1:8080
- http://127.0.0.1:8080/portal/

## Notes

- Update phone/email in `index.html` before launch.
- Replace `CPANEL_USERNAME` in `.cpanel.yml` with your real cPanel username.
- For production form submissions, replace the live intake URL in `index.html` with the Google Apps Script deployment URL in [google-intake/README.md](/Users/hankybot/Documents/Playground/insuredbylena-site/google-intake/README.md).
- The hero/consultation forms still work locally against `http://127.0.0.1:8787/api/public/intake`.

## Automatic deploy

To automate uploads from GitHub to Namecheap, add these repository secrets:

- `NAMECHEAP_SFTP_HOST` -> `server367.web-hosting.com`
- `NAMECHEAP_SFTP_USERNAME` -> your cPanel username, for example `lenahilty`
- `NAMECHEAP_SFTP_PASSWORD` -> your cPanel or SFTP password
- `NAMECHEAP_SFTP_PORT` -> optional, defaults to `21098`
- `NAMECHEAP_REMOTE_ROOT` -> optional, defaults to `/home/<username>/public_html`
- `NAMECHEAP_PORTAL_AUTH_DIR` -> optional, defaults to `/home/<username>/portal-auth`

The workflow mirrors the repo to the hosting account with `lftp` over SFTP and refreshes the portal auth file at `/portal`.
It runs on every push to `main`, and you can still run it manually from GitHub Actions if needed.
