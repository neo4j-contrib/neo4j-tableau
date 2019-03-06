FROM node:10
ENV PRIVATE_KEY key.pem
ENV CERTIFICATE cert.pem
ENV PORT 3000
COPY package.json package.json
COPY package-lock.json package-lock.json
RUN npm install
COPY server.js server.js
COPY website website
CMD ["npm", "start"]