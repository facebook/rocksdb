#!/usr/bin/env python3
"""Generate prompt from template for a given component and template type."""
import json
import sys
import os

REPO = "/home/xbw/workspace/ws36/rocksdb"
DOCS = f"{REPO}/docs"

def load_json(path):
    with open(path) as f:
        return json.load(f)

def main():
    if len(sys.argv) < 3:
        print("Usage: generate_prompt.py <component> <template_type> [sub_index]", file=sys.stderr)
        print("  template_type: enrich, enrich_sub, stitch, review_cc, review_codex, fix", file=sys.stderr)
        print("  sub_index: required for enrich_sub (0-based index into splits)", file=sys.stderr)
        sys.exit(1)

    component = sys.argv[1]
    template_type = sys.argv[2]
    sub_index = int(sys.argv[3]) if len(sys.argv) > 3 else None

    template_map = {
        "enrich": f"{DOCS}/cc_enrichment_prompt_template.md",
        "enrich_sub": f"{DOCS}/cc_enrichment_subcomponent_prompt_template.md",
        "stitch": f"{DOCS}/cc_index_stitcher_prompt_template.md",
        "review_cc": f"{DOCS}/cc_review_prompt_template.md",
        "review_codex": f"{DOCS}/codex_review_prompt_template.md",
        "fix": f"{DOCS}/cc_fix_prompt_template.md",
    }

    with open(template_map[template_type]) as f:
        template = f.read()

    plan = load_json(f"{DOCS}/enrichment_plan.json")
    comp_docs = plan["component_external_docs"].get(component, {})
    wikis = comp_docs.get("wikis", [])
    blogs = comp_docs.get("blogs", [])

    wiki_list = "\n".join(f"- `docs/wiki_source/{w}`" for w in wikis) if wikis else "(none)"
    blog_list = "\n".join(f"- `docs/_posts/{b}`" for b in blogs) if blogs else "(none)"

    result = template.replace("{{COMPONENT}}", component)
    result = result.replace("{{WIKI_LIST}}", wiki_list)
    result = result.replace("{{BLOG_LIST}}", blog_list)

    if template_type == "enrich_sub" and sub_index is not None:
        splits = load_json(f"{DOCS}/splits.json")
        if component in splits and sub_index < len(splits[component]):
            sub = splits[component][sub_index]
            result = result.replace("{{SUBCOMPONENT_NAME}}", sub["name"])
            result = result.replace("{{SUBCOMPONENT_DESCRIPTION}}", sub["description"])
            result = result.replace("{{CHAPTER_LIST}}", sub["chapters"])
            result = result.replace("{{KEY_FILES}}", sub["key_files"])
        else:
            print(f"Error: no split [{sub_index}] for {component}", file=sys.stderr)
            sys.exit(1)

    print(result)

def list_splits():
    splits = load_json(f"{DOCS}/splits.json")
    for comp, subs in splits.items():
        for i, sub in enumerate(subs):
            print(f"{comp}\t{i}\t{sub['name']}")

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--list-splits":
        list_splits()
    else:
        main()
