# insuredbylena-site

Static website for `insuredbylena.com`.

## Structure

- `/index.html` -> public landing page
- `/landing.css` -> landing page styles
- `/portal/` -> internal dashboard app
- `/.cpanel.yml` -> cPanel Git deploy tasks

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
