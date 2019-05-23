'use-strict';

const path = require('path');
const webpack = require('webpack');
const DuplicatePackageCheckerPlugin = require('duplicate-package-checker-webpack-plugin');
const TerserWebpackPlugin = require('terser-webpack-plugin');
const nodeExternals = require('webpack-node-externals');

const devMode = 'development';
const mode = process.env.NODE_ENV || devMode;
const isDevMode = mode === devMode;
const srcPath = path.resolve(__dirname, 'src');
const bundlePath = path.resolve(__dirname, 'bundle');
const nodeModulesPath = path.resolve(__dirname, 'node_modules');
const jsFileRegExp = /\.js$/;

const config = {
  mode,
  context: srcPath,
  entry: '/',
  externals: [
    nodeExternals(),
  ],
  module: {
    rules: [
      {
        test: jsFileRegExp,
        exclude: nodeModulesPath,
        use: [
          'thread-loader',
          {
            loader: 'babel-loader',
            options: {
              cacheDirectory: true,
            },
          },
        ],
      }, {
        test: jsFileRegExp,
        enforce: 'pre',
        exclude: nodeModulesPath,
        use: [
          'thread-loader',
          {
            loader: 'eslint-loader',
            options: {
              cache: true,
            },
          },
        ],
      }, {
        test: /[^\.js]$/,
        exclude: nodeModulesPath,
        use: [
          'thread-loader',
          {
            loader: 'file-loader',
            options: {
              name: '[path][name].[ext]',
            },
          },
        ]
      },
    ],
  },
  resolve: {},
  watch: isDevMode,
  devtool: 'eval',
  plugins: [],
  optimization: {},
  output: {
    path: bundlePath,
    filename: '[name].js',
    libraryTarget: 'commonjs2',
  },
  node: {
    __dirname: false,
  },
  target: 'node',
};

const minimazer = [];
const plugins = [
  new webpack.NoEmitOnErrorsPlugin(),
  new DuplicatePackageCheckerPlugin({
    emitError: true,
  }),
];

// MARK: - Development mode
if (isDevMode) {
  Array.prototype.push.apply(plugins, [
    new webpack.NamedModulesPlugin(),
  ]);
// MARK: - Production mode
} else {
  Array.prototype.push.apply(minimazer, [
    new TerserWebpackPlugin({
      cache: true,
      parallel: true,
      exclude: nodeModulesPath,
      terserOptions: {
        mangle: true,
        warnings: false,
        compress: true,
        output: {
          comments: false,
          beautify: false,
        },
      },
    }),
  ]);

  config.devtool = false;
}

config.optimization.minimizer = minimazer;
config.plugins = plugins;

module.exports = config;
