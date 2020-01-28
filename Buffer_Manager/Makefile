CC = gcc
CFLAGS  = -g -Wall

default: test1

test1: test_assign2_1.o storage_mgr_Leo.o dberror.o buffer_mgr.o buffer_mgr_stat.o
	$(CC) $(CFLAGS) -o test1 test_assign2_1.o storage_mgr_Leo.o dberror.o buffer_mgr.o buffer_mgr_stat.o

test2: test_assign2_2.o storage_mgr_Leo.o dberror.o buffer_mgr.o buffer_mgr_stat.o
	$(CC) $(CFLAGS) -o test2 test_assign2_2.o storage_mgr_Leo.o dberror.o buffer_mgr.o buffer_mgr_stat.o

test_assign2_1.o: test_assign2_1.c dberror.h storage_mgr.h test_helper.h buffer_mgr.h buffer_mgr_stat.h
	$(CC) $(CFLAGS) -c test_assign2_1.c

test_assign2_2.o: test_assign2_2.c dberror.h storage_mgr.h test_helper.h buffer_mgr.h buffer_mgr_stat.h
	$(CC) $(CFLAGS) -c test_assign2_2.c

buffer_mgr_stat.o: buffer_mgr_stat.c buffer_mgr_stat.h buffer_mgr.h
	$(CC) $(CFLAGS) -c buffer_mgr_stat.c

buffer_mgr.o: buffer_mgr.c buffer_mgr.h dt.h storage_mgr.h
	$(CC) $(CFLAGS) -c buffer_mgr.c

storage_mgr_Leo.o: storage_mgr_Leo.c storage_mgr.h dberror.h
	$(CC) $(CFLAGS) -c storage_mgr_Leo.c

dberror.o: dberror.c dberror.h
	$(CC) $(CFLAGS) -c dberror.c

clean:
	$(RM) test1 test2 *.o *~

run_test1:
	./test1

run_test2:
	./test2