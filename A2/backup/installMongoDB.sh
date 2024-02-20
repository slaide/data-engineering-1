sudo apt-get install gnupg curl

curl -fsSL https://pgp.mongodb.com/server-7.0.asc | \
   sudo gpg -o /usr/share/keyrings/mongodb-server-7.0.gpg \
   --dearmor

echo "deb [ arch=amd64,arm64 signed-by=/usr/share/keyrings/mongodb-server-7.0.gpg ] https://repo.mongodb.org/apt/ubuntu focal/mongodb-org/7.0 multiverse" | sudo tee /etc/apt/sources.list.d/mongodb-org-7.0.list

sudo apt-get update
sudo apt-get install -y mongodb-org

# run it
sudo systemctl start mongod
# check running status
sudo systemctl status mongod

# set up python environment
sudo apt install python3.8-venv
# crate venv in .
python3 -m venv env
# start venv
source env/bin/activate
# inside venv, python links to python3
python -m pip install 'pymongo[srv]' tqdm
