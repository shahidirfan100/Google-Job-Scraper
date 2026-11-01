/* eslint-disable import-x/no-default-export */
import apifyEslintConfig from '@apify/eslint-config';

export default [
    {
        ignores: ['scratch/**', 'storage/**', 'node_modules/**', 'test/**'],
    },
    ...apifyEslintConfig,
];
