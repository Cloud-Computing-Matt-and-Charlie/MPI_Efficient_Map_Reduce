
CC = mpicxx
EXEC = mpirun
MAPCODE=master.cpp
REDCODE=reducer.cpp
MAPEXEC=master
REDEXEC=reducer

build: master.cpp
        ${CC} $(MAPCODE) -o $(MAPEXEC) -Wall
        ${CC} $(REDCODE) -o $(REDEXEC) -Wall

run: build
        ${EXEC} -f /root/hosts -np 1 ./master

clean:
        rm -fr $(MAPEXEC) $(REDEXEC)

