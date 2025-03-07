module.exports = {
    "env": {
        "browser": true,
        "es2021": true
    },
    "plugins": ["simple-import-sort", "prettier", "n", "promise"],
    "extends": "standard-with-typescript",
    "overrides": [
        {
            "env": {
                "node": true
            },
            "files": [
                ".eslintrc.{js,cjs}"
            ],
            "parserOptions": {
                "sourceType": "script"
            }
        }
    ],
    "parserOptions": {
        "ecmaVersion": "latest",
        "sourceType": "module"
    },
    "rules": {
        '@typescript-eslint/no-explicit-any': 'off',
        '@typescript-eslint/explicit-module-boundary-types': 'off',
        '@typescript-eslint/no-unused-vars': 'error',
        'simple-import-sort/imports': 'warn',
        'simple-import-sort/exports': 'warn',
        'no-async-promise-executor': 'off',
        'prefer-arrow-callback': 'error',
        'no-prototype-builtins': 'off',
        'prefer-const': 'error',
        'no-var': 'error',
        'prefer-template': 'error',
        'no-useless-escape': 'off',
        "indent": "off",
        "no-use-before-define": "off",
        '@typescript-eslint/indent': 'off',
        '@typescript-eslint/restrict-template-expressions': 'off',
        '@typescript-eslint/prefer-nullish-coalescing': 'off',
        '@typescript-eslint/semi': 'off',
        '@typescript-eslint/explicit-function-return-type': 'off',
        '@typescript-eslint/space-before-function-paren': 'off',
        '@typescript-eslint/no-floating-promises': 'off',
        '@typescript-eslint/strict-boolean-expressions': 'off',
        '@typescript-eslint/comma-dangle': 'off',
        "@typescript-eslint/member-delimiter-style": "off"
    }
}
