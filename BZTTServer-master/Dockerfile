FROM node:14.8-alpine

WORKDIR /app
COPY ./src ./src
COPY ./package.json .
COPY ./package-lock.json .

RUN npm ci --frozen-lockfile

USER node
ENTRYPOINT ["npm", "start"]
