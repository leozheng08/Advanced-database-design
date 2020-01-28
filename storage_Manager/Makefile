CC = gcc
CFLAG = -g -wall

all: test1

test1: test_assign1_1.o storage_mgr_Leo.o dberror.o 
	$(CC) $(CFLAGS) -o test1 test_assign1_1.o storage_mgr_Leo.o dberror.o

test_assign1_1.o: test_assign1_1.c dberror.h storage_mgr.h test_helper.h
	$(CC) $(CFLAGS) -c test_assign1_1.c

storage_mgr_Leo.o: storage_mgr_Leo.c dberror.h storage_mgr.h 
	$(CC) $(CFLAGS) -c storage_mgr_Leo.c

dberror.o: dberror.c dberror.h 
	$(CC) $(CFLAGS) -c dberror.c

clean: 
	$(RM) test1 *.o *~
	
