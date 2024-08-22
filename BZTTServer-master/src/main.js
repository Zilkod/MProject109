import net from "net"
import {BZTTClientConnection} from "./bztt-client.js"
import {TopicManager} from "./bztt-topic.js"
import {logger} from "./logger.js"

const BIND_ADDRESS = process.env.BIND_ADDRESS || 'localhost'
const BIND_PORT = process.env.BIND_PORT || 8889

const log = logger.child({module: "bztt-main"})



const topicManager = new TopicManager()

const server = net.createServer()
server.listen(BIND_PORT, BIND_ADDRESS, () => {
  log.info({message: `Server running on port: ${BIND_PORT}`})
})

const bzttClients = new Set()

server.on("connection", (socket) => {
  try{
    socket.setKeepAlive(true, 60 * 1000)
    const connection = new BZTTClientConnection(socket, topicManager, () =>
      bzttClients.delete(connection),
    )
    bzttClients.add(connection)
  } catch(err) {
    log.error({err, message: 'BZTTClient crashed!'})
  }
})
