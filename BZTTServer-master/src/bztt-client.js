import Parsimmon from "parsimmon"
import {logger} from "./logger.js"

import smartbuffer from 'smart-buffer'


const {SmartBuffer} = smartbuffer

const log = logger.child({module: "bztt-client"})

const T_CONNECT = 1
const T_CONNECT_ACK = 2
const T_DISCONNECT = 3
const T_DISCONNECT_ACK = 4
const T_SUBSCRIBE = 5
const T_SUBSCRIBE_ACK = 6
const T_UNSUBSCRIBE = 7
const T_UNSUBSCRIBE_ACK = 8
const T_PUBLISH = 9
const T_PUBLISH_ACK = 10
const T_PUSH = 11

const PBZString = Parsimmon((input, i) => {
  const len = input.readUInt32BE(i)
  const str = Parsimmon.Binary.encodedString("utf-8", len).parse(
    input.slice(i + 4, len + i + 4),
  )
  return Parsimmon.makeSuccess(i + len + 4, str.value)
})

const PBZArrayOf = (parser) => {
  return Parsimmon((input, i) => {
    const size = input.readUInt32BE(i)
    const len = input.readUInt32BE(i + 4)
    const res = parser.times(len).parse(input.slice(i + 8, i + size + 4))
    return Parsimmon.makeSuccess(size + i + 8 + 1, res.value)
  })
}

const PBZBlob = Parsimmon((input, i) => {
  const len = input.readUInt32BE(i)
  const blob = Parsimmon.Binary.buffer(len).parse(
    input.slice(i + 4, len + i + 4),
  )
  return Parsimmon.makeSuccess(i + len + 4, blob.value)
})

// TODO: amount of strings changed from 3 to 2 update elsewhere
const UserParser = Parsimmon.seqMap(
  PBZString,
  PBZString,
  Parsimmon.Binary.uint8BE,
  Parsimmon.Binary.uint8BE,
  (clientId, username, userId, team) => ({
    clientId,
    userId,
    username,
    team,
  }),
)

const PublishParser = Parsimmon.seqMap(PBZString, PBZBlob, (topic, data) => ({
  topic,
  data,
}))

export class BZTTClientConnection {
  constructor(socket, topicManager, onDestroy) {

    log.debug("Client created")

    this.socket = socket
    this.topicManager = topicManager
    this.writeCursor = 0
    this.tmpBuffer = SmartBuffer.fromSize(2048) //Buffer.alloc(2048)
    this.isHeaderRead = false
    this.onDestroy = onDestroy
    this.socket.on("data", (...a) => this.handleData(...a))
    this.socket.on("close", () => this._cleanup())
    this.socket.on("error", (error) => log.error({error}))

    this.subscribedTopics = new Map()
    this.connected = false
    this.connectedUser = undefined
    this._nextId = 0
  }

  set nextId(newId) {
    // Prevent unsafe overflow
    if (this._nextId > Number.MAX_SAFE_INTEGER) {
      this._nextId = 0
    } else {
      this._nextId = newId
    }
  }

  get nextId() {
    return this._nextId
  }

  _cleanup() {
    log.debug("Client closed")
    for (const topic of this.subscribedTopics.values()) {
      topic.unsubscribe()
    }
    this.onDestroy()
  }

  write(...a){
    if(!this.socket.destroyed){
      this.socket.write(...a)
    }
  }

  end(...a){
    if(!this.socket.destroyed){
      this.socket.end(...a)
    }
  }

  handleCompleteMessage(msg) {
    log.debug(`Got message ${msg.type} ${msg.id}`)
    if (this.connected) {
      switch (msg.type) {
        case T_DISCONNECT: {
          this.connectedUser = undefined
          this.connected = false
          this.end(
            this._createMessage(T_DISCONNECT_ACK, {id: msg.id}, 0),
          )
          break
        }
        case T_SUBSCRIBE: {
          for (const topicName of msg.payload) {
            if (this.subscribedTopics.has(topicName)) continue
            const topic = this.topicManager.getOrCreateTopic(topicName)
            const subscribtion = topic
              .observe(this.connectedUser)
              .subscribe((payload) => {
                this.write(
                  this._createMessage(T_PUSH, {topic: topicName}, payload),
                )
              })
            this.subscribedTopics.set(topicName, subscribtion)
          }
          this.write(
            this._createMessage(T_SUBSCRIBE_ACK, {id: msg.id}, 0),
          )
          break
        }
        case T_UNSUBSCRIBE: {
          for (const topicName of msg.payload) {
            if (!this.subscribedTopics.has(topicName)) continue
            this.subscribedTopics.get(topicName).unsubscribe()
            this.subscribedTopics.delete(topicName)
          }
          this.write(
            this._createMessage(T_UNSUBSCRIBE_ACK, {id: msg.id}, 0),
          )
          break
        }
        case T_PUBLISH: {
          // TODO: fail if topic not exist
          const topic = this.topicManager.getTopic(msg.payload.topic)
          if(topic){
            topic.publish(this.connectedUser, msg.payload.data)
            this.write(this._createMessage(T_PUBLISH_ACK, {id: msg.id}, 0))
          }
          else{
            log.debug({error: `Topic does not exist ${msg.payload.topic}`})
            this.write(this._createMessage(T_PUBLISH_ACK, {id: msg.id}, 1))
          }
          break
        }
        default: {
          //invalid message type
          this.socket.destroy(
            new Error(`Got invalid message type while connected ${msg.type}`),
          )
        }
      }
    } else {
      switch (msg.type) {
        case T_CONNECT: {
          this.connectedUser = msg.payload
          this.connected = true
          log.debug(`User connected: ${this.connectedUser.userId} ${this.connectedUser.username}`)
          this.write(this._createMessage(T_CONNECT_ACK, {id: msg.id}, 0))
          break
        }

        // Message attempted before connection
        default: {
          this.socket.destroy(
            new Error(
              `Got invalid message type while disconnected ${msg.type}`,
            ),
          )
        }
      }
    }
  }

  _createMessage(type, header, payload) {
    log.debug(`Creating message ${type}`)
    switch (type) {
      case T_PUSH: {
        const topic = header.topic
        const topicLen = topic.length
        const buffer = SmartBuffer.fromSize(2048)
        
        buffer.writeUInt8(type)
        buffer.writeUInt32BE(this.nextId)
        buffer.writeUInt32BE(payload.length + topicLen + 8)
        buffer.writeUInt32BE(topicLen)
        buffer.writeString(topic)
        buffer.writeUInt32BE(payload.length)
        buffer.writeBuffer(payload)
        //payload.copy(buffer, 9 + 4 + topicLen + 4)
        this.nextId += 1
        log.debug({type: type, topic: topic, id: this.nextId, buffer})
        
        return buffer.toBuffer()
      }

      case T_CONNECT_ACK:
      case T_SUBSCRIBE_ACK:
      case T_PUBLISH_ACK:
      case T_DISCONNECT_ACK: {
        const buffer = SmartBuffer.fromSize(128) 
        buffer.writeUInt8(type)
        buffer.writeUInt32BE(header.id)
        buffer.writeUInt32BE(4)
        buffer.writeUInt32BE(payload)
        log.debug({type: type, buffer})
        return buffer.toBuffer()
      }
    }
  }

  parseCompleteMessage(msg) {
    log.debug(msg)
    const completeMessage = {...msg, payload: undefined}
    switch (msg.type) {
      case T_CONNECT:
        completeMessage.payload = UserParser.parse(msg.payload).value
        break
      case T_SUBSCRIBE:
      case T_UNSUBSCRIBE:
        //parsed =  //parsePayload(msg.payload, SUBSCRIBE_MSG_PAYLOAD)
        completeMessage.payload = PBZArrayOf(PBZString).parse(msg.payload).value //parsed[0]
        break
      case T_PUBLISH:
        completeMessage.payload = PublishParser.parse(msg.payload).value
    }

    this.handleCompleteMessage(completeMessage)
  }


  handleData(data) {
    this.tmpBuffer.writeBuffer(data)
    // read header
    while (true) {
      const remaining = this.tmpBuffer.remaining()
      log.debug({msg: 'looping', remaining})
      if (this.isHeaderRead === false && remaining >= 9) {
        this.currentMessage = {
          type: this.tmpBuffer.readUInt8(),
          id: this.tmpBuffer.readUInt32BE(),
          len: this.tmpBuffer.readUInt32BE(),
          payload: null,
        }
        log.debug({msg: 'Header', packet: this.currentMessage})
      
        this.isHeaderRead = true
      } else if (
        this.isHeaderRead === true &&
        remaining >= this.currentMessage.len
      ) {

        this.currentMessage.payload = this.tmpBuffer.readBuffer(this.currentMessage.len)
        this._sliceBuffer()
        
        this.isHeaderRead = false

        this.parseCompleteMessage(this.currentMessage)
        this.currentMessage = null
      } else {
        break
      }
    }
  }

  _sliceBuffer() {
    const newBuffer = SmartBuffer.fromSize(2048);
    newBuffer.writeBuffer(
      this.tmpBuffer.toBuffer().slice(this.tmpBuffer.length - this.tmpBuffer.remaining())
    );
    this.buffer = newBuffer;
  }
}
