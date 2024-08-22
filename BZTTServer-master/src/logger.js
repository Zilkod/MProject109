import pino from "pino"

const getTime = () => {
  const time = new Date().toISOString()
  return `,"time":${time}`
}

const options = {
  level: process.env.LOG_LEVEL || "debug",
  timestamp: getTime,
}

const logger = pino(options)

export {logger, options}
