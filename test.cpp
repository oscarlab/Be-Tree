// This test program performs a series of inserts, deletes, updates,
// and queries to a betree.  It performs the same sequence of
// operatons on a std::map.  It checks that it always gets the same
// result from both data structures.

// The program takes 1 command-line parameter -- the number of
// distinct keys it can use in the test.

// The values in this test are strings.  Since updates use operator+
// on the values, this test performs concatenation on the strings.

#include <string.h>
#include <sys/types.h>
#include <unistd.h>
#include "betree.hpp"

int next_command(FILE *input, int *op, uint64_t *arg)
{
  int ret;
  char command[64];

  ret = fscanf(input, "%s %ld", command, arg);
  if (ret == EOF)
    return EOF;
  else if (ret != 2) {
    fprintf(stderr, "Parse error\n");
    exit(3);
  }
  
  if (strcmp(command, "Inserting") == 0) {
    *op = 0;
  } else if (strcmp(command, "Updating") == 0) {
    *op = 1;
  } else if (strcmp(command, "Deleting") == 0) {
    *op = 2;
  } else if (strcmp(command, "Query") == 0) {
    *op = 3;
    if (1 != fscanf(input, " -> %s", command)) {
      fprintf(stderr, "Parse error\n");
      exit(3);
    }
  } else if (strcmp(command, "Full_scan") == 0) {
    *op = 4;
  } else if (strcmp(command, "Lower_bound_scan") == 0) {
    *op = 5;
  } else if (strcmp(command, "Upper_bound_scan") == 0) {
    *op = 6;
  } else {
    fprintf(stderr, "Unknown command: %s\n", command);
    exit(1);
  }
  
  return 0;
}

template<class Key, class Value>
void do_scan(typename betree<Key, Value>::iterator &betit,
	     typename std::map<Key, Value>::iterator &refit,
	     betree<Key, Value> &b,
	     typename std::map<Key, Value> &reference)
{
  while (refit != reference.end()) {
    assert(betit != b.end());
    assert(betit.first == refit->first);
    assert(betit.second == refit->second);
    ++refit;
    if (refit == reference.end()) {
      debug(std::cout << "Almost done" << std::endl);
    }
    ++betit;
  }
  assert(betit == b.end());
}

#define DEFAULT_TEST_MAX_NODE_SIZE (1ULL<<4)
#define DEFAULT_TEST_MIN_FLUSH_SIZE (DEFAULT_MAX_NODE_SIZE / 4)
#define DEFAULT_TEST_CACHE_SIZE (4)
#define DEFAULT_TEST_NDISTINCT_KEYS (1ULL << 10)
#define DEFAULT_TEST_NOPS (1ULL << 12)

void usage(char *name)
{
  std::cout
    << "Usage: " << name << " [OPTIONS]" << std::endl
    << "Tests the betree implementation" << std::endl
    << std::endl
    << "Options are" << std::endl
    << "  Required:"   << std::endl
    << "    -d <backing_store_directory>                    [ default: none, parameter is required ]"           << std::endl
    << "    -m  <mode>  (test or benchmark)                 [ default: none, parameter required ]"              << std::endl
    << "  Betree tuning parameters:" << std::endl
    << "    -N <max_node_size>            (in elements)     [ default: " << DEFAULT_TEST_MAX_NODE_SIZE  << " ]" << std::endl
    << "    -f <min_flush_size>           (in elements)     [ default: " << DEFAULT_TEST_MIN_FLUSH_SIZE << " ]" << std::endl
    << "    -C <max_cache_size>           (in betree nodes) [ default: " << DEFAULT_TEST_CACHE_SIZE     << " ]" << std::endl
    << "  Options for both tests and benchmarks" << std::endl
    << "    -k <number_of_distinct_keys>                    [ default: " << DEFAULT_TEST_NDISTINCT_KEYS << " ]" << std::endl
    << "    -t <number_of_operations>                       [ default: " << DEFAULT_TEST_NOPS           << " ]" << std::endl
    << "    -s <random_seed>                                [ default: random ]"                                << std::endl
    << "  Test scripting options" << std::endl
    << "    -o <output_script>                              [ default: no output ]"                             << std::endl
    << "    -i <script_file>                                [ default: none ]"                                  << std::endl;
}

int test(betree<uint64_t, std::string> &b,
	 uint64_t nops,
	 uint64_t number_of_distinct_keys,
	 FILE *script_input,
	 FILE *script_output)
{
  std::map<uint64_t, std::string> reference;

  for (unsigned int i = 0; i < nops; i++) {
    int op;
    uint64_t t;
    if (script_input) {
      int r = next_command(script_input, &op, &t);
      if (r == EOF)
	exit(0);
      else if (r < 0)
	exit(4);
    } else {
      op = rand() % 7;
      t = rand() % number_of_distinct_keys;
    }
    
    switch (op) {
    case 0: // insert
      if (script_output)
	fprintf(script_output, "Inserting %lu\n", t);
      b.insert(t, std::to_string(t) + ":");
      reference[t] = std::to_string(t) + ":";
      break;
    case 1: // update
      if (script_output)
	fprintf(script_output, "Updating %lu\n", t);
      b.update(t, std::to_string(t) + ":");
      if (reference.count(t) > 0)
      	reference[t] += std::to_string(t) + ":";
      else
      	reference[t] = std::to_string(t) + ":";
      break;
    case 2: // delete
      if (script_output)
	fprintf(script_output, "Deleting %lu\n", t);
      b.erase(t);
      reference.erase(t);
      break;
    case 3: // query
      try {
	std::string bval = b.query(t);
	assert(reference.count(t) > 0);
	std::string rval = reference[t];
	assert(bval == rval);
	if (script_output)
	  fprintf(script_output, "Query %lu -> %s\n", t, bval.c_str());
      } catch (std::out_of_range e) {
	if (script_output)
	  fprintf(script_output, "Query %lu -> DNE\n", t);
	assert(reference.count(t) == 0);
      }
      break;
    case 4: // full scan
      {
	if (script_output)
	  fprintf(script_output, "Full_scan 0\n");
	auto betit = b.begin();
	auto refit = reference.begin();
	do_scan(betit, refit, b, reference);
      }
      break;
    case 5: // lower-bound scan
      {
	if (script_output)
	  fprintf(script_output, "Lower_bound_scan %lu\n", t);
	auto betit = b.lower_bound(t);
	auto refit = reference.lower_bound(t);
	do_scan(betit, refit, b, reference);
      }
      break;
    case 6: // scan
      {
	if (script_output)
	  fprintf(script_output, "Upper_bound_scan %lu\n", t);
	auto betit = b.upper_bound(t);
	auto refit = reference.upper_bound(t);
	do_scan(betit, refit, b, reference);
      }
      break;
    default:
      abort();
    }
  }

  std::cout << "Test PASSED" << std::endl;
  
  return 0;
}

void benchmark(betree<uint64_t, std::string> &b,
	       uint64_t nops,
	       uint64_t number_of_distinct_keys)
{
  for (uint64_t i = 0; i < nops; i++) {
    uint64_t t = rand() % number_of_distinct_keys;
    b.update(t, std::to_string(t) + ":");
  }
}

int main(int argc, char **argv)
{
  char *mode = NULL;
  uint64_t max_node_size = DEFAULT_TEST_MAX_NODE_SIZE;
  uint64_t min_flush_size = max_node_size / 4;
  uint64_t cache_size = DEFAULT_TEST_CACHE_SIZE;
  char *backing_store_dir = NULL;
  uint64_t number_of_distinct_keys = DEFAULT_TEST_NDISTINCT_KEYS;
  uint64_t nops = DEFAULT_TEST_NOPS;
  char *script_infile = NULL;
  char *script_outfile = NULL;
  unsigned int random_seed = time(NULL) * getpid();
 
  int opt;
  char *term;
    
  //////////////////////
  // Argument parsing //
  //////////////////////
  
  while ((opt = getopt(argc, argv, "m:d:N:f:C:o:k:t:s:i:")) != -1) {
    switch (opt) {
    case 'm':
      mode = optarg;
      break;
    case 'd':
      backing_store_dir = optarg;
      break;
    case 'N':
      max_node_size = strtoull(optarg, &term, 10);
      if (*term) {
	std::cerr << "Argument to -N must be an integer" << std::endl;
	usage(argv[0]);
	exit(1);
      }
      break;
    case 'f':
      min_flush_size = strtoull(optarg, &term, 10);
      if (*term) {
	std::cerr << "Argument to -f must be an integer" << std::endl;
	usage(argv[0]);
	exit(1);
      }
      break;
    case 'C':
      cache_size = strtoull(optarg, &term, 10);
      if (*term) {
	std::cerr << "Argument to -C must be an integer" << std::endl;
	usage(argv[0]);
	exit(1);
      }
      break;
    case 'o':
      script_outfile = optarg;
      break;
    case 'k':
      number_of_distinct_keys = strtoull(optarg, &term, 10);
      if (*term) {
	std::cerr << "Argument to -k must be an integer" << std::endl;
	usage(argv[0]);
	exit(1);
      }
      break;
    case 't':
      nops = strtoull(optarg, &term, 10);
      if (*term) {
	std::cerr << "Argument to -t must be an integer" << std::endl;
	usage(argv[0]);
	exit(1);
      }
      break;
    case 's':
      random_seed = strtoull(optarg, &term, 10);
      if (*term) {
	std::cerr << "Argument to -s must be an integer" << std::endl;
	usage(argv[0]);
	exit(1);
      }
      srand(random_seed);
      break;
    case 'i':
      script_infile = optarg;
      break;
    default:
      std::cerr << "Unknown option '" << (char)opt << "'" << std::endl;
      usage(argv[0]);
      exit(1);
    }
  }
  
  FILE *script_input = NULL;
  FILE *script_output = NULL;

  if (mode == NULL || (strcmp(mode, "test") != 0 && strcmp(mode, "benchmark") != 0)) {
    std::cerr << "Must specify a mode of \"test\" or \"benchmark\"" << std::endl;
    usage(argv[0]);
    exit(1);
  }

  if (strcmp(mode, "benchmark") == 0) {
    if (script_infile) {
      std::cerr << "Cannot specify an input script in benchmark mode" << std::endl;
      usage(argv[0]);
      exit(1);
    }
    if (script_outfile) {
      std::cerr << "Cannot specify an output script in benchmark mode" << std::endl;
      usage(argv[0]);
      exit(1);
    }
  }
  
  if (script_infile) {
    script_input = fopen(script_infile, "r");
    if (script_input == NULL) {
      perror("Couldn't open input file");
      exit(1);
    }
  }
  
  if (script_outfile) {
    script_output = fopen(script_outfile, "w");
    if (script_output == NULL) {
      perror("Couldn't open output file");
      exit(1);
    }
  }

  srand(random_seed);

  if (backing_store_dir == NULL) {
    std::cerr << "-d <backing_store_directory> is required" << std::endl;
    usage(argv[0]);
    exit(1);
  }
  
  ////////////////////////////////////////////////////////
  // Construct a betree and run the tests or benchmarks //
  ////////////////////////////////////////////////////////
  
  one_file_per_object_backing_store ofpobs(backing_store_dir);
  swap_space sspace(&ofpobs, cache_size);
  betree<uint64_t, std::string> b(&sspace, max_node_size, min_flush_size);

  if (strcmp(mode, "test") == 0) 
    test(b, nops, number_of_distinct_keys, script_input, script_output);
  else
    benchmark(b, nops, number_of_distinct_keys);
  
  if (script_input)
    fclose(script_input);
  
  if (script_output)
    fclose(script_output);

  return 0;
}

