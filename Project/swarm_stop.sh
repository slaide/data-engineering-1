# 1. bring down stack
docker stack rm myproject

# 2. bring down registry
# docker service rm registry

# 3. leave swarm
docker swarm leave --force
