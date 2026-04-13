# Conventional Commits Configuration
# This file enforces conventional commit message format
# Format: <type>(<scope>): <subject>

module.exports = {
  extends: ['@commitlint/config-conventional'],
  rules: {
    'type-enum': [
      2,
      'always',
      [
        'chore',
        'ci',
        'docs',
        'feat',
        'fix',
        'perf',
        'refactor',
        'revert',
        'style',
        'test',
        'build',
        'ops',
        'hotfix'
      ]
    ],
    'header-max-length': [2, 'always', 100]
  }
};
