"""Push a blog post's post.md (with images via GitHub raw URLs) to its Google Doc.

Usage:
    .venv/bin/python3 blogposts/_scripts/push_to_doc.py <slug>
    .venv/bin/python3 blogposts/_scripts/push_to_doc.py 2026-05-fondivalitsejate-aruanded

Reads <slug>/meta.yaml to find post_md path and google_doc_id.
Requires:
  - Google Doc shared with read-write@tuleva-claude.iam.gserviceaccount.com (Editor)
  - PNGs in <slug>/charts/ already pushed to main branch on GitHub
    (Drive's HTML importer fetches them from raw.githubusercontent.com)
"""
import io
import json
import os
import re
import sys
from pathlib import Path

import yaml
from dotenv import load_dotenv
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
import markdown as md

REPO_ROOT = Path(__file__).resolve().parents[2]
BLOGPOSTS_DIR = REPO_ROOT / "blogposts"
GITHUB_REPO_PATH = "TulevaEE/reporting-engine"
GITHUB_BRANCH = "main"

SCOPES = [
    "https://www.googleapis.com/auth/documents",
    "https://www.googleapis.com/auth/drive",
]

IMG_SRC_RE = re.compile(r'(<img\b[^>]*?\bsrc=")([^"]+)(")', re.IGNORECASE)

STYLE_CSS = """
body {
  font-family: 'Roboto', sans-serif;
  font-size: 11pt;
  line-height: 1.5;
  color: #222222;
}
p, ul, ol, blockquote {
  margin-top: 0;
  margin-bottom: 12pt;
  line-height: 1.5;
}
h1, h2, h3, h4, h5, h6 {
  font-family: 'Merriweather', serif;
  color: #002F63;
  line-height: 1.3;
  margin-top: 18pt;
  margin-bottom: 8pt;
}
h1 { font-size: 22pt; }
h2 { font-size: 17pt; }
h3 { font-size: 14pt; }
h4 { font-size: 12pt; }
h5 { font-size: 11pt; font-weight: bold; }
blockquote {
  border-left: 3pt solid #00AEEA;
  padding-left: 12pt;
  font-style: italic;
  color: #444444;
}
table {
  border-collapse: collapse;
  width: 100%;
  margin-top: 0;
  margin-bottom: 12pt;
}
th, td {
  border: 1pt solid #cccccc;
  padding: 4pt 8pt;
  vertical-align: top;
}
th, td, th p, td p {
  margin: 0;
  line-height: 1.15;
}
th {
  background-color: #f2f5f9;
  font-family: 'Roboto', sans-serif;
  font-weight: bold;
  text-align: center;
}
img {
  max-width: 100%;
  height: auto;
}
em { font-style: italic; }
"""


def load_credentials():
    info = json.loads(os.environ["GCP_SERVICE_ACCOUNT"])
    return service_account.Credentials.from_service_account_info(info, scopes=SCOPES)


def strip_frontmatter(text: str) -> str:
    if text.startswith("---\n"):
        end = text.find("\n---\n", 4)
        if end >= 0:
            return text[end + 5 :]
    return text


def rewrite_svg_to_png(text: str) -> str:
    """Switch any .svg image refs to .png (PNG is more reliable for Doc import)."""
    text = re.sub(
        r"\[!\[([^\]]*)\]\(([^\)]+)\.svg\)\]\([^\)]+\)",
        lambda m: f"![{m.group(1)}]({m.group(2)}.png)",
        text,
    )
    text = re.sub(
        r"!\[([^\]]*)\]\(([^\)]+)\.svg\)",
        lambda m: f"![{m.group(1)}]({m.group(2)}.png)",
        text,
    )
    return text


def rewrite_image_srcs_to_github(html: str, slug: str) -> str:
    base = f"https://raw.githubusercontent.com/{GITHUB_REPO_PATH}/{GITHUB_BRANCH}/blogposts/{slug}"

    def replace(match):
        prefix, src, suffix = match.group(1), match.group(2), match.group(3)
        if not src.startswith(("http://", "https://", "data:")):
            src = f"{base}/{src}"
        return f"{prefix}{src}{suffix}"

    return IMG_SRC_RE.sub(replace, html)


def constrain_image_width(html: str, width_px: int = 620) -> str:
    def add_width(m):
        tag = m.group(0)
        if re.search(r"\bwidth\s*=", tag, re.IGNORECASE):
            return tag
        return tag[:-1] + f' width="{width_px}">'

    return re.sub(r"<img\b[^>]*>", add_width, html, flags=re.IGNORECASE)


def md_to_html(text: str) -> str:
    body = md.markdown(text, extensions=["tables", "fenced_code", "extra"])
    return (
        "<!DOCTYPE html>\n<html><head><meta charset='utf-8'>"
        f"<style>{STYLE_CSS}</style>"
        "</head><body>\n"
        + body
        + "\n</body></html>"
    )


def main():
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <slug>")
        sys.exit(2)
    slug = sys.argv[1].rstrip("/")
    post_dir = BLOGPOSTS_DIR / slug
    if not post_dir.is_dir():
        print(f"ERROR: post folder not found: {post_dir}")
        sys.exit(1)

    meta_path = post_dir / "meta.yaml"
    if not meta_path.exists():
        print(f"ERROR: missing meta.yaml in {post_dir}")
        sys.exit(1)

    meta = yaml.safe_load(meta_path.read_text(encoding="utf-8"))
    doc_id = meta.get("google_doc_id")
    if not doc_id:
        print("ERROR: google_doc_id not set in meta.yaml — create a Doc, share with the SA, and paste its id.")
        sys.exit(1)

    post_md_path = post_dir / meta.get("post_md", "post.md")
    raw = post_md_path.read_text(encoding="utf-8")
    raw = strip_frontmatter(raw)
    raw = rewrite_svg_to_png(raw)

    html = md_to_html(raw)
    html = rewrite_image_srcs_to_github(html, slug)
    html = constrain_image_width(html, width_px=620)

    preview_path = REPO_ROOT / "temp" / f"{slug}_doc_preview.html"
    preview_path.parent.mkdir(exist_ok=True)
    preview_path.write_text(html, encoding="utf-8")
    print(f"Wrote preview: {preview_path}")
    for m in IMG_SRC_RE.finditer(html):
        print(f"  img src -> {m.group(2)}")

    load_dotenv(REPO_ROOT / ".env")
    creds = load_credentials()
    drive = build("drive", "v3", credentials=creds)

    doc_meta = drive.files().get(fileId=doc_id, fields="id,name", supportsAllDrives=True).execute()
    print(f"\nDoc: {doc_meta['name']}  ({doc_id})")

    media = MediaIoBaseUpload(io.BytesIO(html.encode("utf-8")), mimetype="text/html", resumable=False)
    drive.files().update(fileId=doc_id, media_body=media, supportsAllDrives=True).execute()
    print(f"Pushed: https://docs.google.com/document/d/{doc_id}/edit")


if __name__ == "__main__":
    main()
