
/*

Set up: 
------
Basic Object Map_Item has a word and a frequency. 
Take N nodes each with 2 cores. 

Work Schedule 
-------------
Node 0 Ranks
0. Blocking recieve for frequency data 
1. Add this frequency data to an existing map when availible (get operation)

Nodes 1 -> N-1 Ranks
0. Split raw data into words send to rank 1 using asynchornous send
1. Recieve from rank 0 on same node +
use words to map to frequencies and send to main asynchronously 

MPI Primitives
--------------
Allgather for all 
MPI TEST
Isend 


*/


#include "mpi.h"
#include<map> 
#include <vector>
#include <algorithm> 
#include <string>
#include<iostream> 
#include<fstream>
#include <cctype>
#include <queue> 
#include <set>
#include <errno.h>
#include<cstring>
using namespace std;

 
vector<int> batmen; 
vector<int> robin; 
int file_len; 
int my_rank; 
int world_size;

#define INTRANODE_BLOCK_SIZE (50000)
#define INTERNODE_BLOCK_SIZE (100000)
#define INTERNODE_OUTBOX_SIZE 5
#define INTRANODE_OUTBOX_SIZE 5
#define MAP_SWEEP_LENGTH 100000 //Make sure this is at least the intranode block size
#define POLL_FREQUENCY 5 
#define MAX_NUM_SIZE 10
#define MAX_WORD_SIZE 50 
#define INPUT_FILE_PATH "./204178.txt" 
#define OUTPUT_FILE_PATH "./204178.out"
#define PRINT_EXCHANGE_NUMS 1
#define PRINT_BATMEN_REDUCER_MAPS 0
#define PRINT_MASTER_MAP 0
#define PRINT_MASTER_MAP_PERIOD 1
#define MODE_WORD_CHAR 0
#define HEAP_SIZE 10
#define PRINT_HEAP 0
#define PRINT_HEAP_PERIOD 10

// Sending data management 
const int INTRA_BUFF_SIZE = INTRANODE_BLOCK_SIZE*(MAX_WORD_SIZE + MAX_NUM_SIZE); 
const int INTER_BUFF_SIZE = INTERNODE_BLOCK_SIZE*(MAX_WORD_SIZE + MAX_NUM_SIZE); 
char intraNodeOutbox[INTRANODE_OUTBOX_SIZE][INTRA_BUFF_SIZE];
int intraNodeOutBoxLengths[INTRANODE_OUTBOX_SIZE];
char interNodeOutbox[INTERNODE_OUTBOX_SIZE][INTRA_BUFF_SIZE];
int intraNodeOutboxLengths[INTRANODE_OUTBOX_SIZE];
int interNodeOutboxLengths[INTERNODE_OUTBOX_SIZE];
int master_batman; 
int master_robin; 
MPI_Request* intraNodeOutboxRequests[INTRANODE_OUTBOX_SIZE]; 
MPI_Request* interNodeOutboxRequests[INTERNODE_OUTBOX_SIZE]; 
char intraNodeBuffer[INTRA_BUFF_SIZE];
char interNodeBuffer[INTRA_BUFF_SIZE];
int intraNodeBufferSize;
int interNodeBufferSize; 
int current_file_index; 

struct reduceArgs
{
    //[start, stop)
    int start_read_loc; 
    int stop_read_loc;
    int my_partner;
    ifstream* input_file; 
};

//Core Logic
void do_reduce(reduceArgs* input_args); 
void do_reduce_sidekick(reduceArgs* input_args); 
void do_master(); 
void do_master_sidekick();

//Core Text Processing 
void read_in_block_intra(ifstream& fl, int start_index, int stop_index);
void read_in_block_inter(ifstream& fl, int start_index, int stop_index);
void flatten_map(char* output, int& current_index, map<string, int>& input_map, int stop_index);
void unflatten_map(char* input_chars, int& _current_index, map<string, int>& input_map, int stop_index);
void get_words(map<string, int>& output_map, ifstream& fl, int& current_index, int start_index,  int stop_index);
void get_chars(map<string, int>& output_map, ifstream& fl, int& _current_index, int start_index,  int stop_index);

//Helper Funcitons
template <typename PQtype>
void print_pq(PQtype input_pq); 
void print_map_to_file(std::map<std::string, int> map, char* filename); 

static void prep_word(string& cword);
bool is_valid_char(char num);
static void number_as_chars(int num, char *dest, int& output_len); 
void get_most_least_frequent(); 

map<string, int> working_map; 




//Maintaining List of 10 most and 10 least frequent observations 

struct min_comapare_obs_freq{
    bool operator()(const std::pair<string, int>& one, const std::pair<string, int>& two) const { return one.second > two.second;}
};

struct max_comapare_obs_freq{
    bool operator()(const std::pair<string, int>& one, const std::pair<string, int>& two) const { return one.second < two.second;}
};

priority_queue<std::pair<string, int>, std::vector<std::pair<string, int>>, min_comapare_obs_freq>  min_heap; //Most frequent 
priority_queue<std::pair<string, int>, std::vector<std::pair<string, int>>, max_comapare_obs_freq> max_heap; //Least frequent
std::set<int, std::greater<int>> largest_frequencies; 
std::set<int, std::less<int>> smallest_frequencies;  




