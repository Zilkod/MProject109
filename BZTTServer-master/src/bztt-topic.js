import rxjs from "rxjs"
import pino from "pino"
import fs from 'fs'
import {logger, options} from "./logger.js"

const log = logger.child({module: "bztt-topic"})



const {Observable} = rxjs

class Topic {
  constructor(topicPath) {
    this.userList = new Map()
    this.topicPath = topicPath
  }

  publish(user, msg) {}

  notify(data) {}

  observe(user) {
    const stream = Observable.create((observer) => {
      this._joinTopic(user, observer)
      return () => {
        this._leaveTopic(user)
      }
    })

    return stream
  }

  _joinTopic(user, observer) {
    log.debug({message: "user join topic", user})
    return this.userList.set(user, observer)
  }

  _leaveTopic(user) {
    log.debug({message: "user leave topic", user})
    return this.userList.delete(user)
  }
}

class ChatTopic extends Topic {
  publish(publishingUser, message) {
    for (const [user, observer] of this.userList) {
      observer.next(Buffer.from(`${publishingUser.username}: ${message.toString()}`))
    }
  }
}

class TransparentTopic extends Topic {
  publish(publishingUser, message) {
    for (const [user, observer] of this.userList) {
      if (user !== publishingUser) {
        observer.next(message)
      }
    }
  }
}

class BroadcastTopic extends Topic {
  publish(publishingUser, message) {
    for (const [user, observer] of this.userList) {
      observer.next(message)
    }
  }
}

class LoopbackTopic extends Topic {
  publish(publishingUser, message) {
    for (const [user, observer] of this.userList) {
      if (user === publishingUser) {
        observer.next(message)
      }
    }
  }
}
class DebugTopic extends Topic{
  constructor(topicPath){
    super(topicPath)
    fs.mkdirSync(`./logs${this.topicPath}`, {recursive: true})
    this.logMap = new Map()
    
  }

  logger(user){
    if(!this.logMap.has(user)){
      this.logMap.set(user, pino(options, pino.destination({dest: `./logs${this.topicPath}/${user.username}_${Math.random() * 10000}.log`, sync: false})).child({topic: this.topicPath, user}))
    }
    return this.logMap.get(user)
  }


  _joinTopic(user, observer){
    super._joinTopic(user, observer)
    this.logger(user).info({type: 'USER_JOINED'})
  }


  publish(publishingUser, message){
    const raw = message.toString()
    try{
      const jsonMessage = JSON.parse(raw)
      this.logger(publishingUser).info({type: 'DEBUG_MSG_JSON', message: jsonMessage})
    } catch(e) {
      log.debug({type: 'FAIL_PARSE_JSON', message: e.message})
      this.logger(publishingUser).info({type: 'DEBUG_MSG_STR', message: raw})
    }
  }

  _leaveTopic(user){
    super._leaveTopic(user)
    this.logger(user).info({type: 'USER_LEFT', user})
  }

}


class UserListTopic extends Topic {

  _notify(extra) {
    log.debug({message: "Userlist notify", extra})
    const content = Buffer.from(
      JSON.stringify({
        ...extra,
        users: [...this.userList.keys()],
      }),
    )

    for (const observer of this.userList.values()) {
      observer.next(content)
    }
  }

  _joinTopic(user, observer) {
    super._joinTopic(user, observer)
    this._notify({
      event: "USER_JOIN",
      user: user,
    })
  }

  _leaveTopic(user) {
    super._leaveTopic(user)
    this._notify({
      event: "USER_LEAVE",
      user: user,
    })
  }
}

const topicClassMap = {
  users: UserListTopic,
  chat: ChatTopic,
  direct: TransparentTopic,
  loopback: LoopbackTopic,
  broadcast: BroadcastTopic,
  debug: DebugTopic
}

class TopicManager {
  constructor(topicsMap = new Map()) {
    this.topics = topicsMap
  }

  getOrCreateTopic(topicPath) {
    if (this.topics.has(topicPath)) {
      return this.topics.get(topicPath)
    }
    const [, modid, gameid, topicName] = topicPath.split("/")
    log.debug({message: "Creating topic", modid, gameid, topicName})
    const topic = new topicClassMap[topicName](topicPath)
    this.topics.set(topicPath, topic)

    return topic
  }

  getTopic(topic) {
    return this.topics.get(topic)
  }
}




export {TopicManager, UserListTopic, TransparentTopic, ChatTopic, Topic}
