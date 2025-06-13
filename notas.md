# acceder contenedor

```
  docker ps
  docker exec -it 2ba090962c52 bash
  
```

# inciar contenedor por primera vez
docker compose up

# Contenedor construido
```
# 1. Eliminar Contenedor y volumenes
 docker compose down -v

# 2. Construir Contenedor
docker-compose build --no-cache

# 3. Iniciar Servicios
docker-compose up

```

# Elimina la imagen que está causando conflicto:
```
docker rmi custom-airflow:2.9.1
```

# Limpia imágenes colgantes y caché:
```
docker builder prune -f
docker image prune -f
```


docker compose down -v
docker compose up -d --build