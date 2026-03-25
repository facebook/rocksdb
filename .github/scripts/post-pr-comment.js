// Shared PR comment posting utility.
// Used by both clang-tidy-comment.yml and claude-review-comment.yml.
//
// Usage from actions/github-script:
//   const post = require('./.github/scripts/post-pr-comment.js');
//   await post({ github, context, core, prNumber, body, marker });
//
// Parameters:
//   github   - octokit instance from actions/github-script
//   context  - GitHub Actions context
//   core     - @actions/core for logging
//   prNumber - PR number to comment on
//   body     - comment body (markdown string)
//   marker   - HTML comment marker for dedup (e.g. '<!-- claude-review -->')
//              If an existing comment with this marker is found, it is updated.
//              If not found, a new comment is created.

module.exports = async function postPrComment(
    {github, context, core, prNumber, body, marker}) {
  if (!prNumber || !body) {
    core.warning('Missing prNumber or body; skipping comment.');
    return;
  }

  const owner = context.repo.owner;
  const repo = context.repo.repo;

  // Ensure marker is embedded in the body
  const markedBody = body.includes(marker) ? body : `${marker}\n${body}`;

  // Search for existing comment with this marker
  const {data: comments} = await github.rest.issues.listComments({
    owner,
    repo,
    issue_number: prNumber,
    per_page: 100,
  });
  const existing = comments.find(c => c.body.includes(marker));

  if (existing) {
    await github.rest.issues.updateComment({
      owner,
      repo,
      comment_id: existing.id,
      body: markedBody,
    });
    core.info(`Updated existing comment ${existing.id}`);
  } else {
    await github.rest.issues.createComment({
      owner,
      repo,
      issue_number: prNumber,
      body: markedBody,
    });
    core.info('Created new PR comment');
  }
};
