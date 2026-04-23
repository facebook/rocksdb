// Shared markdown builder for AI review comment bodies.
//
// Usage:
//   const build = require('./build-ai-review-comment.js');
//   return build({ icon, headerTitle, triggerLine, responseBody, footerLines
//   });

module.exports = function buildAiReviewComment(
    {icon, headerTitle, triggerLine, responseBody, footerLines}) {
  return [
    `## ${icon} ${headerTitle}`,
    '',
    triggerLine,
    '',
    '---',
    '',
    responseBody,
    '',
    '---',
    '',
    '<details>',
    '<summary>ℹ️ About this response</summary>',
    '',
    ...footerLines,
    '</details>',
  ].join('\n');
};
