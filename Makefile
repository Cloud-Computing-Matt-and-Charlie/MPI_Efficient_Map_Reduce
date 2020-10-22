
CC = mpicxx
EXEC = mpirun
MAPSOURCES=main.cpp
MAPEXEC= main_map_reduce


build: master.cpp
        ${CC} -std=c++11 $(MAPSOURCES) -o $(MAPEXEC) -Wall
        ${CC} $(REDSOURCES) -o $(REDEXEC) -Wall

run: build
        ${EXEC}  -f /root/hosts -n 8 ./main_map_reduce

clean:
        rm -fr $(MAPEXEC) $(REDEXEC)

