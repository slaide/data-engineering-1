sudo docker ps
sudo docker logs $(sudo docker ps --format 'json' | jq -r '.ID')
