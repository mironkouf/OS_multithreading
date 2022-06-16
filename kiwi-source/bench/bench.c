#include "bench.h"

void _random_key(char *key,int length) {
	int i;
	char salt[36]= "abcdefghijklmnopqrstuvwxyz0123456789";

	for (i = 0; i < length; i++)
		key[i] = salt[rand() % 36];
}

void _print_header(int count)
{
	double index_size = (double)((double)(KSIZE + 8 + 1 * count) / 1048576.0);
	double data_size = (double)((double)(VSIZE + 4) * count) / 1048576.0;

	printf("Keys:\t\t%d bytes each\n",
			KSIZE);
	printf("Values: \t%d bytes each\n",
			VSIZE);
	printf("Entries:\t%d\n",
			count);
	printf("IndexSize:\t%.1f MB (estimated)\n",
			index_size);
	printf("DataSize:\t%.1f MB (estimated)\n",
			data_size);

	printf(LINE1);
}

void _print_environment()
{
	time_t now = time(NULL);

	printf("Date:\t\t%s",
			(char*)ctime(&now));

	int num_cpus = 0;
	char cpu_type[256] = {0};
	char cache_size[256] = {0};

	FILE* cpuinfo = fopen("/proc/cpuinfo", "r");
	if (cpuinfo) {
		char line[1024] = {0};
		while (fgets(line, sizeof(line), cpuinfo) != NULL) {
			const char* sep = strchr(line, ':');
			if (sep == NULL || strlen(sep) < 10)
				continue;

			char key[1024] = {0};
			char val[1024] = {0};
			strncpy(key, line, sep-1-line);
			strncpy(val, sep+1, strlen(sep)-1);
			if (strcmp("model name", key) == 0) {
				num_cpus++;
				strcpy(cpu_type, val);
			}
			else if (strcmp("cache size", key) == 0)
				strncpy(cache_size, val + 1, strlen(val) - 1);
		}

		fclose(cpuinfo);
		printf("CPU:\t\t%d * %s",
				num_cpus,
				cpu_type);

		printf("CPUCache:\t%s\n",
				cache_size);
	}
}

int main(int argc,char** argv)
{
	long int count;
	int threads;

	srand(time(NULL));
	if (argc < 4) {											// na mhn dexetai ligotero apo 4 orismata
		fprintf(stderr,"Usage: db-bench <write | read> | <readwrite> <count>\n");
		exit(1);
	}else if (atoi(argv[2]) < 1 || atoi(argv[3]) < 1)		// na dexetai mono egkuro noumero sto argv[2] kai to argv[3]
	{
		fprintf(stderr,"Invalid number or not a number\n");
		exit(1);
	}
	if (strcmp(argv[1], "write") == 0 || strcmp(argv[1], "read") == 0) 
	{
		int r = 0;
		count = atoi(argv[2]);
		threads = atoi(argv[3]);
		_print_header(count);
		_print_environment();
		if (argc == 5)
			r = 1;
		if (strcmp(argv[1], "write") == 0)
		{
			read_or_write(count, r, 1, threads);		// 1 kanoume write
		}else
		{
			read_or_write(count, r, 2, threads);		// 2 kanoume read
		}
	}
	else if (strcmp(argv[1], "readwrite") == 0) // edw
	{ 
		int r = 0;
		int per_read;
		int per_write;

		count = atoi(argv[2]); //leitourgies
		threads = atoi(argv[3]);
		_print_header(count);
		_print_environment();
		if (argc == 7 || argc == 5)			// an argc = 5 tuxaia kleidia sthn test an argc = 7 tuxaia kleidia sthn percent 
		{
			r = 1;
		}
		if (argc == 6 || argc == 7)
		{
			per_write = atoi(argv[4]);
			per_read = atoi(argv[5]);
			if (per_read + per_write == 100)
				_read_write_test_percent(count, r, threads, per_write, per_read);
			else
			{
				fprintf(stderr,"Invalid percentage. Doesn't add to 100%% \n");
				exit(1);
			}
		}
		else if(argc == 4 || argc == 5)
        {
            _read_write_test(count, r, threads);
        }
        else
        {
            fprintf(stderr,"Too many arguments given\n");
            exit(1);
        }
	}
	else {
		fprintf(stderr,"Usage: db-bench <write | read> <count> <random>\n");
		exit(1);
	}

	return 1;
}
