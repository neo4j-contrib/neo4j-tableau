FROM node:10
ENV PRIVATE_KEY key.pem
ENV CERTIFICATE cert.pem
ENV PORT 3000
COPY . .
CMD ["npm", "start"]