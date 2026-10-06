# Builds the documentation site and serves it with nginx.
# The build needs the guides in ../docs, so run it from the repository root:
#
#   docker build -f website/Dockerfile -t rabbitmq-message-bus-docs .
#   docker run -p 8080:80 rabbitmq-message-bus-docs
FROM node:22-alpine AS build
WORKDIR /repo
COPY docs ./docs
COPY website/package.json website/package-lock.json ./website/
RUN cd website && npm ci
COPY website ./website
ARG DOCS_BASE=/
ARG DOCS_REPOSITORY_URL=
ARG PLAYGROUND_URL=http://localhost:4100
RUN cd website && npm run build

FROM nginx:1.27-alpine
COPY website/nginx.conf /etc/nginx/conf.d/default.conf
COPY --from=build /repo/website/.vitepress/dist /usr/share/nginx/html
EXPOSE 80
