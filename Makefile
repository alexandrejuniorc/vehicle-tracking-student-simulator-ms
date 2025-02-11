### DOCKER SETTINGS

## Start the development environment helpers: mongodb, api
docker/dev/start:
	@docker compose up -d

## Stop the development environment helpers
docker/dev/stop:
	@docker compose down

docker/dev/restart:
	@docker compose down --volumes --remove-orphans
	@docker compose up -d
	@sleep 2
	@pnpm prisma migrate deploy

docker/dev/clean:
	@docker compose down --rmi all --volumes --remove-orphans

docker/dev/shell:
	@docker compose exec -it simulator sh