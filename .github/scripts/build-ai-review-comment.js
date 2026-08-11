// Shared markdown builder for AI review comment bodies.
//
// Usage:
//   const build = require('./build-ai-review-comment.js');
//   return build({ icon, headerTitle, triggerLine, responseBody, footerLines
//   });

// GitHub rejects issue comments longer than this many characters. The model
// response (responseBody) is the only unbounded input, so we trim it to fit
// while keeping the fixed scaffolding (header, trigger, footer) intact.
const MAX_COMMENT_LENGTH = 65536;
const TRUNCATION_NOTE =
    '\n\n*[Truncated — full output in execution log artifact]*';

module.exports = function buildAiReviewComment(
    {icon, headerTitle, triggerLine, responseBody, footerLines}) {
  const assemble = (body) => [`## ${icon} ${headerTitle}`,
                                 '',
                                 triggerLine,
                                 '',
                                 '---',
                                 '',
                                 body,
                                 '',
                                 '---',
                                 '',
                                 '<details>',
                                 '<summary>ℹ️ About this response</summary>',
                                 '',
                                 ...footerLines,
                                 '</details>',
  ].join('\n');

  const body = responseBody || '';
  const comment = assemble(body);
  if (comment.length <= MAX_COMMENT_LENGTH) {
    return comment;
  }
  // Drop only as much of the body as needed to fit, then re-assemble so the
  // footer/<details> block is preserved rather than cut off mid-comment.
  const overflow = comment.length - MAX_COMMENT_LENGTH;
  const keep = Math.max(0, body.length - overflow - TRUNCATION_NOTE.length);
  return assemble(body.substring(0, keep) + TRUNCATION_NOTE);
};
