const tools = {
  has: Object.prototype.hasOwnProperty,
  urlRegExp: /^(https?:\/\/)?(?:[^@/\n]+@)?(?:www\.)?([^:/\n]+)((?::\d+)?)/iy,
};

module.exports = tools;
