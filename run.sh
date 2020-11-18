#docker run -i -d --privileged -t --name sparkify-container sparkify /bin/bash
docker run --name sparkify-container -e POSTGRES_PASSWORD=password -d sparkify
docker exec -it sparkify-container sudo -i -u postgres
#jupyter nbconvert --execute ./test.ipynb
