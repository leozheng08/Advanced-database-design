recordmanager: record_mgr.o test_assign3_1.o dberror.o rm_serializer.o expr.o storage_mgr.o buffer_mgr.o buffer_mgr_stat.o
	gcc -g -w -o recordmanager test_assign3_1.o dberror.o expr.o record_mgr.o rm_serializer.o storage_mgr.o buffer_mgr.o -lm buffer_mgr_stat.o 


record_mgr.o: record_mgr.c record_mgr.h buffer_mgr.h storage_mgr.h
	gcc -g -w -c  record_mgr.c


test_assign3_1.o: test_assign3_1.c dberror.h storage_mgr.h test_helper.h buffer_mgr.h buffer_mgr_stat.h
	gcc -g -w -c test_assign3_1.c -lm


dberror.o: dberror.c dberror.h 
	gcc -g -w -c dberror.c


rm_serializer.o: rm_serializer.c dberror.h tables.h record_mgr.h
	gcc -g -w -c rm_serializer.c


expr.o: expr.c dberror.h record_mgr.h expr.h tables.h
	gcc -g -w -c expr.c
 

test_expr: test_expr.o dberror.o expr.o record_mgr.o rm_serializer.o storage_mgr.o buffer_mgr.o buffer_mgr_stat.o
	gcc -g -w -o test_expr test_expr.o dberror.o expr.o record_mgr.o rm_serializer.o storage_mgr.o buffer_mgr.o -lm buffer_mgr_stat.o 


test_expr.o: test_expr.c dberror.h expr.h record_mgr.h tables.h test_helper.h
	gcc -g -w -c test_expr.c -lm


storage_mgr.o: storage_mgr.c storage_mgr.h 
	gcc -g -w -c storage_mgr.c -lm


buffer_mgr.o: buffer_mgr.c buffer_mgr.h dt.h storage_mgr.h
	gcc -g -w -c buffer_mgr.c


buffer_mgr_stat.o: buffer_mgr_stat.c buffer_mgr_stat.h buffer_mgr.h
	gcc -g -w -c buffer_mgr_stat.c


clean: 
	$(RM) recordmgr test_expr *.o *~
