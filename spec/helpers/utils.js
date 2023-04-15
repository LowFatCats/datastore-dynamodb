module.exports = {
  // Produces the input argument after a delay
  delay: function delay(time) {
    const handler = function handler(arg) {
      return new Promise((resolve) => {
        setTimeout(resolve, time, arg);
      });
    };
    return handler;
  },
};
