# Google Sheets/Docs/Slides access via Claude Code

We use a shared GCP service account. It can only access docs explicitly shared with its email — no access to anyone's personal Google Drive.

## Setup

### 1. Get your key from Tõnu

Ask Tõnu for a personal JSON key file. He'll create one for you in GCP Console.

### 2. Add to .env

In your project root, create/edit `.env` and paste the entire JSON content as a single line:

```
GCP_SERVICE_ACCOUNT={"type":"service_account","project_id":"tuleva-claude",...the rest of the JSON...}
```

### 3. Share your docs with the service account

Any Google Sheet, Doc, or Slides you want to access from Claude must be shared with:

```
read-write@tuleva-claude.iam.gserviceaccount.com
```

In Google Sheets/Docs/Slides: Share > add the email above. Viewer for reading, Editor for writing.

**NB:** Sharing a doc with this email makes it visible to everyone on the team who has a key. Only share docs you're comfortable with the whole team seeing.

## Security notes

- Never commit `.env` to git (it's already in `.gitignore`)
- Each team member has their own key — if someone leaves, Tõnu revokes just that key
- The service account can only see docs you explicitly share with it
