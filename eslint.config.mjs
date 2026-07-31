import globals from 'globals';
import reviewableConfigBaseline from 'reviewable-configs/eslint-config/baseline.js';

export default [
  ...reviewableConfigBaseline,
  {
    files: ['*.js', 'tests/*.js'],
    languageOptions: {
      globals: {
        ...globals.node,
        ...globals.es2018,
      },
      ecmaVersion: 2018,
      sourceType: 'commonjs'
    }
  },
];
