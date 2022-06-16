#include <string.h>
#include "../engine/db.h"
#include "../engine/variant.h"
#include "bench.h"
#include <pthread.h> 		// prosthikh
#include <sys/types.h>		// prosthikh
#include <unistd.h>			// prosthikh
#include <sys/syscall.h>	// prosthikh

#define DATAS ("testdb")

struct arg_struct{
	int arg1; // pername ta nhmata tou write pou exoume (to xrhsimopoioume gia to arg10 aki to arg11)
	int arg2; // r
	int arg3; // found metavlhth
	long int arg4; // the number of repeats for _x_test (otan exw pososto vazw to write)
	long int arg7; // gia na kratame to threads repeat otan exoume pososto kratame to read
	int arg8; // krataw tis allages sto pcond gia na 3erw ti paei meta
	int arg9; // krataw to 1o thread id
	int arg10; // kratame to upoloipo twn leitourgiwn dia ta nhmata gia to write
	int arg11; // kratame to upoloipo twn leitourgiwn dia ta nhmata gia to read(xreiazetai logo tou percent)
	int arg12; // gia na ksekinaei to search meta to add
	double *pinakas_xronwn;//pinakas krataei tous xronous twn ektelesewn twn thread
};

void *_write_test(void *arguments)
{
	int i;
	Variant sk, sv;
	DB* db;

	// XRONOS GIA NHMATA
	double cost;
	long long start,end;
	start = get_ustime_sec();

	char key[KSIZE + 1];
	char val[VSIZE + 1];
	char sbuf[1024];
	struct arg_struct *args = (struct arg_struct *) arguments;
	memset(key, 0, KSIZE + 1);
	memset(val, 0, VSIZE + 1);
	memset(sbuf, 0, 1024);

	db = db_open(DATAS);

	// XRONOS GIA NHMATA
	pid_t x = syscall(__NR_gettid);
	int y = x-(args->arg9);				 //to id pou pernaw katw sto nhma

	if (args->arg10 != 0 && y == 0)
	{
		for (i = 0; i < (args->arg4 + args->arg10); i++) {// sto for trexoume to arg4 sto write gt ekei to kratame (allaxe logo posostou)
			if (args->arg2)
				_random_key(key, KSIZE);
			else
				snprintf(key, KSIZE, "key-%d", i);

			fprintf(stderr, "%d adding %s\n", i, key);
			snprintf(val, VSIZE, "val-%d", i);
			sk.length = KSIZE;
			sk.mem = key;
			sv.length = VSIZE;
			sv.mem = val;

			db_add(db, &sk, &sv);

			if ((i % 10000) == 0) {
				fprintf(stderr,"random write finished %d ops%30s\r", i,	"");
				fflush(stderr);
			}
		}
		args->arg12 = 0;			// shma gia na trexei to search meta to add tou upoloipou
	}
	else{
		for (i = 0; i < args->arg4; i++) {	// sto for trexoume to arg4 sto write gt ekei to kratame (allaxe logo posostou)
			if (args->arg2)
				_random_key(key, KSIZE);
			else
				snprintf(key, KSIZE, "key-%d", i);

			fprintf(stderr, "%d adding %s\n", i, key);
			snprintf(val, VSIZE, "val-%d", i);

			sk.length = KSIZE;
			sk.mem = key;
			sv.length = VSIZE;
			sv.mem = val;

			db_add(db, &sk, &sv);

			if ((i % 10000) == 0) {
				fprintf(stderr,"random write finished %d ops%30s\r", i,	"");
				fflush(stderr);
			}
		}
	}

	db_close(db);
	args->arg8 = 0;		// meta to prwto write stelnei shma na ksekinhsoun ta read pou perimenoun
	// XRONOS GIA NHMATA
	end = get_ustime_sec();
	cost = end -start;
	*((args->pinakas_xronwn) + y) = cost;

	return NULL;
}

void *_read_test(void *arguments)
{
	int i;
	int ret;
	Variant sk;
	Variant sv;
	DB* db;
	// XRONOS GIA NHMATA
	double cost;
	long long start,end;
	start = get_ustime_sec();

	char key[KSIZE + 1];
	struct arg_struct *args = (struct arg_struct *) arguments;

	while(args->arg8){			// perimenei to prwto nhma tou write na teleiwsei
	}

	//XRONOS GIA NHMATA
	pid_t x = syscall(__NR_gettid);
	int y = x-(args->arg9); //to id pou pernaw katw sto nhma

	db = db_open(DATAS);

	if (args->arg11 != 0 && y == args->arg1)
	{
		while(args->arg12){			// perimenei to prwto nhma tou write me to upoloipo na teleiwsei
		}
		for (i = 0; i < args->arg7 + args->arg11; i++) {	// sto for trexoume to arg7 sto write gt ekei to kratame (allaxe logo posostou)
			memset(key, 0, KSIZE + 1);
			/* if you want to test random write, use the following */
			if (args->arg2)
				_random_key(key, KSIZE);
			else
				snprintf(key, KSIZE, "key-%d", i);
			fprintf(stderr, "%d searching %s\n", i, key);
			sk.length = KSIZE;
			sk.mem = key;
			ret = db_get(db, &sk, &sv);

			if (ret) {
				//db_free_data(sv.mem);
				args->arg3 ++;
			} else {
				INFO("not found key#%s", sk.mem);
				}
			if ((i % 10000) == 0) {
				fprintf(stderr,"random read finished %d ops%30s\r", i, "");
				fflush(stderr);
			}
		}
	}
	else
	{
		for (i = 0; i < args->arg7; i++) {	// sto for trexoume to arg7 sto write gt ekei to kratame (allaxe logo posostou)
			memset(key, 0, KSIZE + 1);
			/* if you want to test random write, use the following */
			if (args->arg2)
				_random_key(key, KSIZE);
			else
				snprintf(key, KSIZE, "key-%d", i);
			fprintf(stderr, "%d searching %s\n", i, key);
			sk.length = KSIZE;
			sk.mem = key;
			ret = db_get(db, &sk, &sv);

			if (ret) {
				//db_free_data(sv.mem);
				args->arg3 ++;
			} else {
				INFO("not found key#%s", sk.mem);
				}
			if ((i % 10000) == 0) {
				fprintf(stderr,"random read finished %d ops%30s\r", i, "");
				fflush(stderr);
			}
		}
	}
	db_close(db);

	// XRONOS GIA NHMATA
	end = get_ustime_sec();
	cost = end -start;
	*((args->pinakas_xronwn) + y) = cost;
	return NULL;
}

void read_or_write(long int count, int r, int r_o_w, int threads)
{
	double cost,time_sum,avg_time;
	long long start,end;
	pthread_t id[threads];
	long int threads_repeat = count / threads;

	// XRONOS GIA NHMATA
	pid_t x = syscall(__NR_gettid);

	start = get_ustime_sec();

	struct arg_struct args;
	args.arg1 = 0;						// gia na apothikeysoyme to pid toy prwtoy read h write thelei m0 mia leitourgia
	args.arg2 = r;
	args.arg3 = 0;
	args.arg4 = threads_repeat;
	args.arg7 = threads_repeat;
	args.arg8 = 0;						// arxikopoieitai sto 0 giati edw den enoxlei
	args.arg10 = 0;						//den xrhsimopoieitai amesa mono gia na parei kalh timh
	args.arg11 = 0;
	args.arg12 = 0;						// arxikopoieitai sto 1 gia na perimenoun ta read

	if (count % threads != 0)			// koitame an oi leitourgies diairountai akrivws me ta nhmata
	{									// auto ginetai gia na mhn xasoume kapoia leitourgia
		args.arg10 = count % threads;
		args.arg11 = count % threads;	// einai to idio exei allaksei gia to percent
	}

	// XRONOS GIA NHMATA
	args.pinakas_xronwn = (double*)malloc(threads * sizeof(double));
	args.arg9 = x+1; 					// krataw to id ths synarthshs(+1 gia kalh afairesh)
	if(r_o_w == 1)						// gia na grapsei ginetai 1
	{
		for (int i = 0; i < threads; i++)
		{
			pthread_create(&id[i], NULL, _write_test, (void *) &args);
		}
	}
	else if(r_o_w == 2)				// gia na diavazei ginetai 2
	{
		args.arg1 = 0;				// 0 giati otan kanw mono read 8elw to prwto nhma na perasei ton elegxo
		for (int i = 0; i < threads; i++)
		{
			pthread_create(&id[i], NULL, _read_test, (void *) &args);
		}
	}
	for(int i=0; i<threads; i++){
		pthread_join(id[i], NULL);
	}
	end = get_ustime_sec();
	cost = end - start;
	// XRONOS GIA NHMATA
	printf(LINE);
	time_sum = 0;
	avg_time = 0;
	for(int i=0; i<threads; i++){
		printf("xronos gia nhma %d = %.6f\n", i+1, *(args.pinakas_xronwn + i));
		time_sum += *(args.pinakas_xronwn + i);
	}
	avg_time = time_sum / threads;
	if(r_o_w == 1)
	{
		printf(LINE);
		printf("|Random-Write (done:%ld): %.6f sec/op; %.1f writes/sec(estimated); cost:%.3f(sec);\n"
		,count, (double)(cost / count)
		,(double)(count / cost)
		,cost);
		printf("|Random-Write average time is %.6f\n", avg_time);
	}else if (r_o_w == 2)
	{
		printf(LINE);
		printf("|Random-Read	(done:%ld, found:%d): %.6f sec/op; %.1f reads /sec(estimated); cost:%.3f(sec)\n",
		count, args.arg3,
		(double)(cost / count),
		(double)(count / cost),
		cost);
		printf("|Random-Read average time is %.6f\n", avg_time);
	}

}


void _read_write_test(long int count, int r, int threads_all)
{

	double cost,time_sum, avg_time_wr, avg_time_rd;
	long long start,end;
	int threads;

	// XRONOS GIA NHMATA
	pid_t x = syscall(__NR_gettid);

	if (threads_all == 1)
	{
		threads = threads_all;
	}
	else
	{
		threads = threads_all /2;
	}

	pthread_t id[threads];
	pthread_t yo[threads];
	long int threads_repeat = count / threads;

	start = get_ustime_sec();

	struct arg_struct args;
	args.arg1 = threads_all-1;			// gia na apothikeysoyme to pid toy prwtoy read
	args.arg2 = r;
	args.arg3 = 0;
	args.arg4 = threads_repeat;			// gia na dinei to for gia thn write (to xreiazomaste gia ta pososta dn to allazw)
	args.arg7 = threads_repeat;			// gia na dinei to for gia thn read (to xreiazomaste gia ta pososta dn to allazw)
	args.arg8 = 1;						// arxikopoieitai sto 1 gia na perimenoun ta read
	args.arg12 = 1;						// arxikopoieitai sto 1 gia na perimenoun ta read

	if (count % threads != 0)		// koitame an oi leitourgies diairountai akrivws me ta nhmata
	{									// auto ginetai gia na mhn xasoume kapoia leitourgia
		args.arg10 = count % threads;
		args.arg11 = count % threads;
	}

	// XRONOS GIA NHMATA
	args.pinakas_xronwn = (double*)malloc(2*threads * sizeof(double));
	args.arg9 = x+1; 					// krataw to id ths synarthshs(+1 gia kalh afairesh)

	for (int i = 0; i < threads; i++)
	{
		pthread_create(&id[i], NULL, _write_test, (void *) &args);
	}
	for (int i = 0; i < threads; i++)
	{
		pthread_create(&yo[i], NULL, _read_test, (void *) &args);
	}

	for (int i = 0; i < threads; i++)
	{
		pthread_join(id[i], NULL);
		pthread_join(yo[i], NULL);
	}

	end = get_ustime_sec();
	cost = end -start;
	// XRONOS GIA NHMATA
	printf(LINE);
	time_sum = 0;
	avg_time_wr = 0;
	avg_time_rd = 0;
	for(int i=0; i<threads; i++){
		printf("xronos gia nhma write %d = %.6f\n", i+1, *(args.pinakas_xronwn + i));
		time_sum += *(args.pinakas_xronwn + i);
	}
	avg_time_wr = time_sum / threads;
	time_sum = 0;
	for(int i=threads; i<threads_all; i++){
		printf("xronos gia nhma read %d = %.6f\n", i+1, *(args.pinakas_xronwn + i));
		time_sum += *(args.pinakas_xronwn + i);
	}
	avg_time_rd = time_sum / threads;
	printf(LINE);
	printf("|Random-Write  (done:%ld): %.6f sec/op; %.1f writes/sec(estimated); cost:%.3f(sec);\n",
		count, (double)(cost / count),
		(double)(count / cost),
		cost);
		printf("|Random-Write average time is %.6f\n", avg_time_wr);
	printf(LINE);
	printf("|Random-Read  (done:%ld, found:%d): %.6f sec/op; %.1f reads /sec(estimated); cost:%.3f(sec)\n",
		count, args.arg3,
		(double)(cost / count),
		(double)(count / cost),
		cost);
	printf("|Random-Read average time is %.6f\n", avg_time_rd);

}
void _read_write_test_percent(long int count, int r, int threads_all, int per_write, int per_read)
{
	double percent_read,percent_write,time_sum, avg_time_wr, avg_time_rd;
	double cost;
	long long start,end;
	int threads, sum_for_modulo_wr, sum_for_modulo_rd;

	// XRONOS GIA NHMATA
	pid_t x = syscall(__NR_gettid);

	if (threads_all == 1)
	{
		threads = threads_all;
	}
	else
	{
		threads = threads_all /2;
	}

	pthread_t id[threads];
	pthread_t yo[threads];

	percent_write = per_write * 0.01;
	percent_read = per_read * 0.01;					// px 40 * 0.01 = 0,4 gia na to pollaplasiasw me to count na brw to pososto leitourgiwn
	long int threads_repeat_write = (percent_write * count) / threads;		// upologizoume tis leitourgies write
	long int threads_repeat_read = (percent_read * count) / threads;		// upologizoume tis leitourgies read

	start = get_ustime_sec();

	struct arg_struct args;
	args.arg1 = threads_all-1;			// gia na apothikeysoyme to pid toy prwtoy read (nhmata spane sth mesh)
	args.arg2 = r;
	args.arg3 = 0;
	args.arg4 = threads_repeat_write;
	args.arg7 = threads_repeat_read;
	args.arg8 = 1;

	sum_for_modulo_wr = percent_write * count;
	sum_for_modulo_rd = percent_read * count;

	if (sum_for_modulo_wr % threads != 0)		// koitame an oi leitourgies diairountai akrivws me ta nhmata
	{												// auto ginetai gia na mhn xasoume kapoia leitourgia
		args.arg10 = sum_for_modulo_wr % threads;
	}
	if (sum_for_modulo_rd % threads != 0)		// koitame an oi leitourgies diairountai akrivws me ta nhmata
	{											// auto ginetai gia na mhn xasoume kapoia leitourgia
		args.arg11 = sum_for_modulo_rd % threads;
	}
	if(args.arg10 != 0 && args.arg11 != 0)
	{
		args.arg12 = 1;
	}
	else{
		args.arg12 = 0;
	}

	// XRONOS GIA NHMATA
	args.pinakas_xronwn = (double*)malloc(2*threads * sizeof(double));
	args.arg9 = x+1; 					// krataw to id ths synarthshs(+1 gia kalh afairesh)

	for (int i = 0; i < threads; i++)
	{
		pthread_create(&id[i], NULL, _write_test, (void *) &args);
	}
	for (int i = 0; i < threads; i++)
	{
		pthread_create(&yo[i], NULL, _read_test, (void *) &args);
	}
	for (int i = 0; i < threads; i++)
	{
		pthread_join(id[i], NULL);
		pthread_join(yo[i], NULL);
	}
	end = get_ustime_sec();
	cost = end -start;
	// XRONOS GIA NHMATA
	printf(LINE);
	time_sum = 0;
	avg_time_wr = 0;
	avg_time_rd = 0;
	for(int i=0; i<threads; i++){
		printf("xronos gia nhma write %d = %.6f\n", i+1, *(args.pinakas_xronwn + i));
		time_sum += *(args.pinakas_xronwn + i);
	}
	avg_time_wr = time_sum / threads;
	time_sum = 0;
	for(int i=threads; i<threads_all; i++){
		printf("xronos gia nhma read %d = %.6f\n", i+1, *(args.pinakas_xronwn + i));
		time_sum += *(args.pinakas_xronwn + i);
	}
	avg_time_rd = time_sum / threads;
	printf(LINE);
	printf("|Random-Write  (done:%.0f): %.6f sec/op; %.1f writes/sec(estimated); cost:%.3f(sec);\n",
		percent_write * count, (double)(cost / count),
		(double)(count / cost),
		cost);
	printf("Write did %d%% of the functions \n", per_write);
	printf("|Random-Write average time is %.6f\n", avg_time_wr);
	printf(LINE);
	printf("|Random-Read	(done:%.0f, found:%d): %.6f sec/op; %.1f reads /sec(estimated); cost:%.3f(sec)\n",
		percent_write * count, args.arg3,																				// theloume na mas tupwnei sto done posa write ekane ara na kseroume posa psaxnoume
		(double)(cost / count),
		(double)(count / cost),
		cost);
	printf("Read did %d%% of the functions \n", per_read);
	printf("|Random-Read average time is %.6f\n", avg_time_rd);
}
