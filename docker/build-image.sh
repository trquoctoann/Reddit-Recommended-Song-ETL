cwd=$(pwd)

sudo chmod +x ./build-image.sh

cd ./hadoop
docker build --tag trquoctoann/hadoop .

cd ..

cd ./hive
docker build --tag trquoctoann/hive .