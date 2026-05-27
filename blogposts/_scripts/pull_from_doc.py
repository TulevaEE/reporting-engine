"""Pull edited content from a Google Doc back into post.md.

Usage:
    .venv/bin/python3 blogposts/_scripts/pull_from_doc.py <slug>
    .venv/bin/python3 blogposts/_scripts/pull_from_doc.py <slug> --diff   # show diff, no write

Reads <slug>/meta.yaml to find google_doc_id and post_md.

What it does:
  - Preserves the YAML frontmatter from existing post.md verbatim.
  - Reads the Doc via Docs API, converts body to markdown (headings, lists, bold/italic/links,
    blockquotes, tables).
  - Image positions: replaces embedded Doc images with the original `![](charts/X.png)` refs
    from post.md, matched by order. If counts mismatch, emits a TODO marker.
  - Saves the result. By default writes post.md.from-doc next to post.md so you can diff first;
    pass --apply to overwrite post.md directly.

Assumes Doc was pushed via push_to_doc.py (so structure is known).
Best-effort — review the diff before committing.
"""
import argparse
import json
import os
import re
import sys
from pathlib import Path

import yaml
from dotenv import load_dotenv
from google.oauth2 import service_account
from googleapiclient.discovery import build

REPO_ROOT = Path(__file__).resolve().parents[2]
BLOGPOSTS_DIR = REPO_ROOT / "blogposts"

SCOPES = ["https://www.googleapis.com/auth/documents.readonly"]


def load_credentials():
    info = json.loads(os.environ["GCP_SERVICE_ACCOUNT"])
    return service_account.Credentials.from_service_account_info(info, scopes=SCOPES)


def split_frontmatter(text: str):
    if text.startswith("---\n"):
        end = text.find("\n---\n", 4)
        if end >= 0:
            return text[: end + 5], text[end + 5 :]
    return "", text


def extract_image_refs(md_text: str) -> list[str]:
    """List image markdown tokens in order: `![alt](src)` strings."""
    return [m.group(0) for m in re.finditer(r"!\[[^\]]*\]\([^)]+\)", md_text)]


def render_text_run(tr: dict) -> str:
    content = tr.get("content", "")
    if not content:
        return ""
    style = tr.get("textStyle", {}) or {}
    link = style.get("link", {})
    link_url = link.get("url") if link else None
    bold = style.get("bold", False)
    italic = style.get("italic", False)

    # Strip trailing newline; handle separately
    trailing_nl = content.endswith("\n")
    text = content.rstrip("\n")
    if not text.strip():
        return content  # whitespace-only

    out = text
    if bold and italic:
        out = f"***{out}***"
    elif bold:
        out = f"**{out}**"
    elif italic:
        out = f"*{out}*"
    if link_url:
        out = f"[{out}]({link_url})"
    if trailing_nl:
        out += "\n"
    return out


def paragraph_text(para: dict, image_placeholder: str = "<!--IMG-->") -> tuple[str, bool]:
    """Return (text, contains_image). image positions are marked by image_placeholder."""
    parts = []
    has_image = False
    for el in para.get("elements", []):
        if "textRun" in el:
            parts.append(render_text_run(el["textRun"]))
        elif "inlineObjectElement" in el:
            parts.append(image_placeholder)
            has_image = True
        elif "footnoteReference" in el or "horizontalRule" in el:
            continue
    return "".join(parts), has_image


def is_bullet_list(para: dict) -> bool:
    return "bullet" in para and para.get("bullet", {}).get("listId")


def paragraph_to_md(para: dict, image_placeholder: str = "<!--IMG-->") -> str:
    style = (para.get("paragraphStyle") or {}).get("namedStyleType", "NORMAL_TEXT")
    text, _ = paragraph_text(para, image_placeholder)
    text = text.rstrip("\n")

    if style == "TITLE":
        return f"# {text}"
    if style.startswith("HEADING_"):
        level = int(style.split("_")[1])
        return f"{'#' * level} {text}"

    if is_bullet_list(para):
        return f"- {text}"

    return text


def table_to_md(table: dict, image_placeholder: str = "<!--IMG-->") -> str:
    rows_md = []
    for r_idx, row in enumerate(table.get("tableRows", [])):
        cells = []
        for cell in row.get("tableCells", []):
            cell_parts = []
            for el in cell.get("content", []):
                p = el.get("paragraph")
                if not p:
                    continue
                txt, _ = paragraph_text(p, image_placeholder)
                cell_parts.append(txt.replace("\n", " ").strip())
            cells.append(" ".join(c for c in cell_parts if c).replace("|", "\\|"))
        rows_md.append("| " + " | ".join(cells) + " |")
        if r_idx == 0:
            rows_md.append("|" + "|".join("---" for _ in cells) + "|")
    return "\n".join(rows_md)


def doc_to_md(doc: dict) -> tuple[str, int]:
    """Convert Doc body to markdown. Returns (md_text, image_count)."""
    placeholder = "<!--IMG-->"
    out_blocks = []
    img_count = 0

    for element in doc.get("body", {}).get("content", []):
        if "paragraph" in element:
            para = element["paragraph"]
            md = paragraph_to_md(para, placeholder)
            img_count += md.count(placeholder)
            out_blocks.append(md)
        elif "table" in element:
            tbl_md = table_to_md(element["table"], placeholder)
            img_count += tbl_md.count(placeholder)
            out_blocks.append(tbl_md)
        elif "sectionBreak" in element:
            out_blocks.append("")

    # Collapse multiple blank lines, ensure blank line between blocks
    text = "\n\n".join(b for b in out_blocks if b is not None)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip() + "\n", img_count


def replace_image_placeholders(md: str, originals: list[str], placeholder: str = "<!--IMG-->") -> str:
    out = []
    idx = 0
    cursor = 0
    while True:
        pos = md.find(placeholder, cursor)
        if pos < 0:
            out.append(md[cursor:])
            break
        out.append(md[cursor:pos])
        if idx < len(originals):
            out.append(originals[idx])
        else:
            out.append(f"<!-- TODO: image {idx + 1} missing in post.md (was in Doc) -->")
        idx += 1
        cursor = pos + len(placeholder)
    if idx < len(originals):
        # fewer images in Doc than in original; flag at end
        out.append(
            "\n\n<!-- TODO: original post.md had {} more image(s) not found in Doc. -->".format(
                len(originals) - idx
            )
        )
    return "".join(out)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("slug")
    ap.add_argument("--apply", action="store_true", help="overwrite post.md (default: write post.md.from-doc)")
    args = ap.parse_args()

    post_dir = BLOGPOSTS_DIR / args.slug
    meta_path = post_dir / "meta.yaml"
    if not meta_path.exists():
        print(f"ERROR: missing meta.yaml in {post_dir}")
        sys.exit(1)

    meta = yaml.safe_load(meta_path.read_text(encoding="utf-8"))
    doc_id = meta.get("google_doc_id")
    if not doc_id:
        print("ERROR: google_doc_id not set in meta.yaml")
        sys.exit(1)

    post_md_path = post_dir / meta.get("post_md", "post.md")
    existing_post = post_md_path.read_text(encoding="utf-8") if post_md_path.exists() else ""
    frontmatter, original_body = split_frontmatter(existing_post)
    image_refs = extract_image_refs(original_body)

    load_dotenv(REPO_ROOT / ".env")
    creds = load_credentials()
    docs = build("docs", "v1", credentials=creds)

    doc = docs.documents().get(documentId=doc_id).execute()
    body_md, img_count = doc_to_md(doc)
    print(f"Doc images found: {img_count}; original post.md had {len(image_refs)} image refs")
    if img_count != len(image_refs):
        print(f"  WARN: image counts differ — review TODO markers in output")

    body_md = replace_image_placeholders(body_md, image_refs)

    final = (frontmatter + "\n" if frontmatter else "") + body_md
    target = post_md_path if args.apply else post_md_path.with_suffix(".md.from-doc")
    target.write_text(final, encoding="utf-8")
    print(f"\nWrote: {target}")
    if not args.apply:
        print("(diff against post.md and pass --apply to overwrite)")


if __name__ == "__main__":
    main()
