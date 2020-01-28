all: test_assign41

test_assign41: test_assign4_1.o btree_mgr.o record_mgr.o rm_serializer.o expr.o storage_mgr.o dberror.o buffer_mgr_stat.o buffer_mgr.o
	gcc -w record_mgr.o btree_mgr.o rm_serializer.o expr.o storage_mgr.o dberror.o buffer_mgr_stat.o buffer_mgr.o test_assign4_1.o -o test4
test_assign4_1.o: test_assign4_1.c
	gcc -c -w test_assign4_1.c

storage_mgr.o: storage_mgr.c
	gcc -c -w storage_mgr.c
record_mgr.o: record_mgr.c
	gcc -c -w record_mgr.c
buffer_mgr.o: buffer_mgr.c
	gcc -c -w buffer_mgr.c
btree_mgr.o: btree_mgr.c
	gcc -c -w btree_mgr.c

dberror.o: dberror.c
	gcc -c -w dberror.c
rm_serializer.o: rm_serializer.c
	gcc -c -w rm_serializer.c
buffer_mgr_stat.o: buffer_mgr_stat.c
	gcc -c -w buffer_mgr_stat.c