echo "export PYSPARK_PYTHON=python3" >> ~/.bashrc
source ~/.bashrc

# install git
sudo apt-get install -y git

# install python dependencies, and start jupyterlab

# install the python package manager 'pip' -- it is recommended to do this directly 
sudo apt-get install -y python3-pip

# check the version:
python3 -m pip --version

# install pyspark (version must be matched as the Spark cluster), and some other useful deps
python3 -m pip install pyspark==3.5.0 --user
python3 -m pip install pandas --user
python3 -m pip install matplotlib --user

# clone the examples from the lectures, so you have a copy to experiment with
git clone https://github.com/JSFRi/DE1-spark.git

# install jupyterlab
python3 -m pip install jupyterlab --user

# start!
jupyter lab
