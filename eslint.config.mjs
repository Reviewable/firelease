import globals from 'globals';
import reviewableConfigBaseline from 'reviewable-configs/eslint-config/baseline.js';
import reviewableConfigLodash from 'reviewable-configs/eslint-config/lodash.js';
import reviewableConfigTypescript from 'reviewable-configs/eslint-config/typescript.js';

export default [
  ...reviewableConfigBaseline,
  ...reviewableConfigLodash.map(config => ({files: ['**/*.ts'], ...config})),
  ...reviewableConfigTypescript,
  {
    files: ['tests/*.js'],
    languageOptions: {
      globals: {
        ...globals.node,
        ...globals.es2018
      },
      ecmaVersion: 2018,
      sourceType: 'commonjs'
    }
  }
];
