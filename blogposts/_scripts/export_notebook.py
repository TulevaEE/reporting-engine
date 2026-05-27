"""Export a blog post's analysis.ipynb to docs/blogposts/<slug>.html for GitHub Pages.

Usage:
    .venv/bin/python3 blogposts/_scripts/export_notebook.py <slug>
    .venv/bin/python3 blogposts/_scripts/export_notebook.py <slug> --execute  # rerun cells first

Reads <slug>/meta.yaml to find notebook path and target HTML location.
Uses blogposts/_scripts/nbconvert_blog_config.py (shows code, hides remove_cell-tagged cells).
"""
import argparse
import shutil
import subprocess
import sys
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
BLOGPOSTS_DIR = REPO_ROOT / "blogposts"
SCRIPTS_DIR = Path(__file__).resolve().parent
DOCS_BLOGPOSTS = REPO_ROOT / "docs" / "blogposts"
PYTHON_BIN = REPO_ROOT / ".venv" / "bin" / "python3"
JUPYTER_BIN = REPO_ROOT / ".venv" / "bin" / "jupyter"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("slug")
    ap.add_argument("--execute", action="store_true",
                    help="re-execute the notebook before export")
    args = ap.parse_args()

    post_dir = BLOGPOSTS_DIR / args.slug
    meta_path = post_dir / "meta.yaml"
    if not meta_path.exists():
        print(f"ERROR: missing meta.yaml in {post_dir}")
        sys.exit(1)
    meta = yaml.safe_load(meta_path.read_text(encoding="utf-8"))

    nb_name = meta.get("notebook", "analysis.ipynb")
    nb_path = post_dir / nb_name
    if not nb_path.exists():
        print(f"ERROR: notebook not found: {nb_path}")
        sys.exit(1)

    target_rel = meta.get("notebook_html_target", f"docs/blogposts/{args.slug}.html")
    target_path = REPO_ROOT / target_rel
    target_path.parent.mkdir(parents=True, exist_ok=True)

    if args.execute:
        print(f"Executing {nb_path} ...")
        subprocess.run(
            [str(JUPYTER_BIN), "nbconvert", "--to", "notebook",
             "--execute", "--allow-errors", "--inplace", str(nb_path)],
            check=True,
        )

    config_path = SCRIPTS_DIR / "nbconvert_blog_config.py"
    print(f"Exporting {nb_path} -> {target_path}")
    subprocess.run(
        [str(JUPYTER_BIN), "nbconvert",
         "--to", "html",
         "--config", str(config_path),
         "--output", target_path.stem,
         "--output-dir", str(target_path.parent),
         str(nb_path)],
        check=True,
    )

    print(f"\nDone: {target_path}")
    if (REPO_ROOT / "docs" / "index.html").exists():
        print(f"Reminder: link this from docs/index.html if not yet listed.")


if __name__ == "__main__":
    main()
