FROM wifimemes/vegate-backend:latest AS builder

RUN uv run mkdocs build

FROM nginx:alpine

COPY --from=builder /app/site/ /usr/share/nginx/html

COPY docs.nginx.conf /etc/nginx/conf.d/default.conf

EXPOSE 80

CMD ["nginx", "-g", "daemon off;"]