# Google intake setup

This folder contains the Google Apps Script version of the public intake backend.

## What it does

- Accepts consultation and team-inquiry submissions
- Appends each submission to a Google Sheet
- Sends an email alert through Gmail

## Setup

1. Create a Google Sheet for incoming submissions.
2. Open Apps Script from that Sheet or create a standalone script.
3. Add the contents of `Code.gs`.
4. In Apps Script, set these Script Properties:
   - `SPREADSHEET_ID` = your Google Sheet ID
   - `SHEET_NAME` = optional, defaults to `Intake`
   - `NOTIFY_TO` = optional comma-separated recipients, defaults to `hello@insuredbylena.com`
5. Deploy the script as a Web App:
   - Execute as: `Me`
   - Who has access: `Anyone`
6. Copy the deployed URL and replace the placeholder in `index.html`:
   - `https://script.google.com/macros/s/REPLACE_WITH_DEPLOYMENT_ID/exec`

## Sheet columns

The script creates these columns on first run:

- Timestamp
- Source
- Full Name
- Email
- Phone
- ZIP
- Coverage Need
- Preferred Time
- Message
- Experience
- Page URL
- User Agent

## Notes

- The current local dev site still posts to the local intake API.
- The live site will post to the Google Apps Script URL once you replace the placeholder deployment ID.
- GmailApp sends the notification, so no SMTP server is required.
