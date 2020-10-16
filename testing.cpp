


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

OoO
---

*/



#include "mpi.h"
#include<map> 
#include <vector>
#include <algorithm> 
#include <string>
#include<iostream> 
#include<fstream>
using namespace std;

 
vector<int> batmen; 
vector<int> robin; 
int file_len; 
int my_rank; 
int world_size;

#define INTRANODE_BLOCK_SIZE 50
#define INTERNODE_BLOCK_SIZE 100
#define INTERNODE_OUTBOX_SIZE 50 
#define INTRANODE_OUTBOX_SIZE 50 
#define MAP_SWEEP_LENGTH 1000
#define POLL_FREQUENCY 5 
#define MAX_NUM_SIZE 10
#define MAX_WORD_SIZE 50 
#define INPUT_FILE_PATH "2600.txt"
#define PRINT_EXCHANGE_NUMS 0

//we could combine these for efficiency 
const int INTRA_BUFF_SIZE = INTRANODE_BLOCK_SIZE*(MAX_WORD_SIZE + MAX_NUM_SIZE); 
const int INTER_BUFF_SIZE = INTERNODE_BLOCK_SIZE*(MAX_WORD_SIZE + MAX_NUM_SIZE); 
char intraNodeOutbox[INTRANODE_OUTBOX_SIZE][INTRA_BUFF_SIZE];
int intraNodeOutBoxLengths[INTRANODE_OUTBOX_SIZE];
char interNodeOutbox[INTERNODE_OUTBOX_SIZE][INTRA_BUFF_SIZE];
int intraNodeOutboxLengths[INTRANODE_OUTBOX_SIZE];
int interNodeOutboxLengths[INTERNODE_OUTBOX_SIZE];
MPI_Request* intraNodeOutboxStatuses[INTRANODE_OUTBOX_SIZE]; 
MPI_Request* interNodeOutboxStatuses[INTERNODE_OUTBOX_SIZE]; 

map<string, int> working_map; 

char intraNodeBuffer[INTRA_BUFF_SIZE];
char interNodeBuffer[INTRA_BUFF_SIZE];
int intraNodeBufferSize;
int interNodeBufferSize; 
int current_file_index; 


void get_word(map<string, int>& output_map, ifstream& fl, int& current_index, int start_index,  int stop_index);
void read_in_block_intra(ifstream& fl, int start_index, int stop_index);
void read_in_block_inter(ifstream& fl, int start_index, int stop_index, int can_combine);
void flatten_map(char* output, int& current_index, map<string, int> input_map, int stop_index);
static void prep_word(string& cword);
static void number_as_chars(int num, char *dest, int& output_len); 
void unflatten_map(char* input_chars, int& _current_index, map<string, int>& input_map, int stop_index);
void read_in_block_inter_temp(ifstream& fl, int start_index, int stop_index);



struct reduceArgs
{
    //[start, stop)
    int start_read_loc; 
    int stop_read_loc;
    int my_partner;
    ifstream* input_file; 
    int is_end; 
};



void do_reduce_sidekick(reduceArgs* input_args)
{
    char debug_id[10]; 
    sprintf(debug_id, "%d -> %d", my_rank, input_args->my_partner); 
    int current_index = input_args->start_read_loc; 
    int outboxHead = 0;
    int outboxTail = 0;
    int outboxSize = 0; 
    int bufferSize = 0; 
    int poll_count = 0; 
    int is_last = 0; 
    int send_complete_flag = 0; 
    MPI_Status send_status; 

    current_index = input_args->start_read_loc; 

    int dum_count = 0; 
    //while (current_index < input_args->stop_read_loc)
    while (current_index < input_args->stop_read_loc)
    {
        dum_count++; 
        //read in block from file 
        //if (outboxSize != INTRANODE_OUTBOX_SIZE) read_in_block(workingFD, current_index, 0);  
        if (outboxSize != INTRANODE_OUTBOX_SIZE) read_in_block_intra(*input_args->input_file, input_args->start_read_loc, input_args->stop_read_loc);

        //read block adds data to map 
        
        //check to see if messages have been recieved 
        while (outboxSize > 0)
        {
            //MPI_TEST on intraNodeOutputStatuses[outboxTail]
            MPI_Test(intraNodeOutboxStatuses[outboxTail], &send_complete_flag, 
                &send_status);
            if (send_complete_flag)
            {
                outboxTail = ((outboxTail + 1)%INTRANODE_OUTBOX_SIZE); 
                outboxSize--; 
            }
            else break; 
        }
        //send block
        if (outboxSize != INTRANODE_OUTBOX_SIZE) 
        {
            //Add to current buffer head of outbox 
            intraNodeOutboxLengths[outboxHead] = intraNodeBufferSize; 
            for (int i = 0; i<intraNodeBufferSize; i++)
            {
                intraNodeOutbox[outboxHead][i] = intraNodeBuffer[i]; 
            }
            //send
            //is_last = (current_index < input_args->stop_read_loc) ? 0 : 1; 
            is_last = (current_index < input_args->stop_read_loc) ? 0 : 1; 

            intraNodeOutboxStatuses[outboxHead] = new MPI_Request; 
            if (PRINT_EXCHANGE_NUMS) printf("%s : Count = %d Flag = %d Outbox Size = %d \n", debug_id, dum_count, is_last, outboxSize); 
            MPI_Isend(intraNodeOutbox[outboxHead], intraNodeOutboxLengths[outboxHead], MPI_CHAR, 
                input_args->my_partner, is_last, MPI_COMM_WORLD, intraNodeOutboxStatuses[outboxHead]);
            //incrament pointers 
            outboxSize++; 
            outboxHead = ((outboxHead+1)%INTRANODE_OUTBOX_SIZE); 
            if (PRINT_EXCHANGE_NUMS) if (is_last) printf("exiting sender %d -> %d \n", my_rank, input_args->my_partner); 
            if (is_last) break; 
        }

    }
    //cant leave without waiting for all sends to finish 
    while (outboxSize > 0)
    {
        //MPI_TEST on intraNodeOutputStatuses[outboxTail]
        MPI_Wait(intraNodeOutboxStatuses[outboxTail], &send_status); 
        
        outboxTail = ((outboxTail + 1)%INTRANODE_OUTBOX_SIZE); 
        outboxSize--;

    }

}

void do_dumb_reduce(reduceArgs* input_args)
{
    char debug_id[10]; 
    sprintf(debug_id, "%d <- %d", my_rank, input_args->my_partner); 
    MPI_Request* incoming_request; 
    char incoming_buffer[INTRANODE_BLOCK_SIZE*2*MAX_WORD_SIZE];
    int observed_size; 
    int terminate_flag = 0; 
    MPI_Status check_status; 
    char temp[15]; 
    int total_recieved = 0; 
    int probe_flag = 0; 
    int recieve_tag; 
    while(true)
    {
        

        /*
        MPI_Iprobe(input_args->my_partner, 0, MPI_COMM_WORLD, &probe_flag, &check_status);
        if (!probe_flag)
        {
            MPI_Iprobe(input_args->my_partner, 1, MPI_COMM_WORLD, &terminate_flag, &check_status);
        }
        

        MPI_Irecv(incoming_buffer, observed_size, MPI_CHAR, input_args->my_partner, terminate_flag,  
            MPI_COMM_WORLD, incoming_request);

        */
        MPI_Iprobe(input_args->my_partner, MPI_ANY_TAG, MPI_COMM_WORLD, &probe_flag, &check_status); 
        if (!probe_flag) continue; 
        incoming_request = new MPI_Request; 
        terminate_flag = check_status.MPI_TAG;
        MPI_Get_count(&check_status, MPI_CHAR, &observed_size);
        MPI_Irecv(incoming_buffer, observed_size, MPI_CHAR, input_args->my_partner, terminate_flag,  
            MPI_COMM_WORLD, incoming_request);

        if (PRINT_EXCHANGE_NUMS) printf("%s : Waiting on Count = %d Flag = %d \n", debug_id, total_recieved+1, terminate_flag); 
        MPI_Wait(incoming_request, &check_status); 
        read_in_block_inter_temp(*input_args->input_file, input_args->start_read_loc, input_args->stop_read_loc);
        //terminate_flag = check_status.MPI_TAG;
        
        total_recieved++; 
        if (PRINT_EXCHANGE_NUMS) printf("%s : Count = %d Flag = %d \n", debug_id, total_recieved, terminate_flag); 
        if (terminate_flag) printf("Recieved last packet!! \n\n");
        if (terminate_flag) break; 

        //PRINT MAP 
        printf("%s Map Contents", debug_id); 
        for (auto entry : working_map)
        {
            //printf("[ %s , %d] ", entry.first, entry.second);
            cout<<" [ "<<entry.first<<", "<<entry.second<<"] "; 
        }
        printf("\n"); 
    }

    //NOTE: DO I NEED TO CLEAN UP, CHECK FOR ANY REMAINING MESSAGES??
}    

void do_dumb_master()
{
    vector<int> done_yet((world_size-2)/2, 0); 
    while (accumulate(0, done_yet.begin(), done_yet.end(), 0) != ((world_size-2)/2))
    {
        MPI_Iprobe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &probe_flag, &check_status); 
        if (!probe_flag) continue; 
    }
    MPI_Iprobe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &probe_flag, &check_status); 
    if (!probe_flag) continue; 

}


int main(int argc, char **argv)
{
    MPI_Comm parentcomm;

    MPI_Init(&argc, &argv);
    MPI_Comm_get_parent(&parentcomm); 
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);
    MPI_Comm_rank(MPI_COMM_WORLD, &my_rank); 
    char* processor_names = new char[world_size*MPI_MAX_PROCESSOR_NAME]; 
    char processor_name[MPI_MAX_PROCESSOR_NAME];
    int processor_name_result_len; 
    MPI_Get_processor_name(processor_name, &processor_name_result_len);
    for (int i = processor_name_result_len; i<MPI_MAX_PROCESSOR_NAME; i++)
    {
        processor_name[i] = '\0';
    }
    MPI_Allgather(processor_name, MPI_MAX_PROCESSOR_NAME, MPI_CHAR, processor_names, 
        MPI_MAX_PROCESSOR_NAME, MPI_CHAR, MPI_COMM_WORLD); 
    //declare who are the batmen and who are robins
    int found_match_flag = 0; 
    for (int i = 0; i<world_size; i++)
    {
        for (int j = i+1; j<world_size; j++)
        {
            //see if im the first occurance 
            for (int k = 0; k<MPI_MAX_PROCESSOR_NAME; k++)
            {
                if (processor_names[(i*MPI_MAX_PROCESSOR_NAME) + k] != processor_names[(j*MPI_MAX_PROCESSOR_NAME) + k])
                    break; 
                if (k==(MPI_MAX_PROCESSOR_NAME-1))
                {
                    //This is a first occurance 
                    batmen.push_back(i); 
                    robin.push_back(j); 
                    found_match_flag =1;
                }
            }
            if (found_match_flag == 1)
            {
                found_match_flag = 0; 
                break; 
            }

        }
        if (batmen.size() == (world_size/2))
            break; 
    }
    
    reduceArgs* input_args = new reduceArgs; 
    for (int i = 0; i<batmen.size(); i++)
    {
        if (my_rank == batmen[i])
        {
            input_args->my_partner = robin[i]; 
            break; 
        }
        if (my_rank == robin[i])
        {
            input_args->my_partner = batmen[i]; 
        }
    }

    int num_recievers = world_size ; //soon this will be world size -2 !!!!
    ifstream fl(INPUT_FILE_PATH, ios::in); 
    fl.seekg(0, ios::end); 
    size_t len = fl.tellg();
	file_len = (int)len;
    fl.seekg(0, ios::beg);
    int my_block_size = file_len/num_recievers; 
    int my_start_index = my_block_size*my_rank; //(my_block_size*(my_rank-2)) !!!
    int my_stop_index = my_start_index+my_block_size; 
    input_args->start_read_loc = my_start_index; 
    input_args->stop_read_loc = my_stop_index; 
    input_args->input_file = &fl; 
    if (my_rank == (world_size-1)) my_stop_index = file_len; 



    if (std::find(batmen.begin(), batmen.end(), my_rank) != batmen.end())
    {
        printf("I am a batman with rank # %d and processor name %s \n", my_rank, processor_name); 
        do_dumb_reduce(input_args); 
    }
    else 
    {
        printf("I am a robin with rank # %d and processor name %s \n", my_rank, processor_name); 
        do_reduce_sidekick(input_args); 
    }

    MPI_Finalize();
    return 0;
}

void read_in_block_intra(ifstream& fl, int start_index, int stop_index)
{
    int working_size = 0; 
    while (working_size < ( INTRA_BUFF_SIZE - MAX_WORD_SIZE - MAX_WORD_SIZE))
    {
        get_word(working_map, fl, current_file_index, start_index, min(current_file_index + MAP_SWEEP_LENGTH, stop_index)); 
        flatten_map(intraNodeBuffer, working_size, working_map, INTRA_BUFF_SIZE); 
    }
    return; 
}
void read_in_block_inter_temp(ifstream& fl, int start_index, int stop_index)
{
    int working_size = 0; 
    int unflatten_current_index = 0; 
    if (1) unflatten_map(intraNodeBuffer, unflatten_current_index, working_map, intraNodeBufferSize);
    while (working_size < (INTER_BUFF_SIZE - MAX_WORD_SIZE - MAX_WORD_SIZE))
    {

        get_word(working_map, fl, current_file_index, start_index, min(current_file_index + MAP_SWEEP_LENGTH, stop_index)); 
        flatten_map(intraNodeBuffer, working_size, working_map, INTER_BUFF_SIZE); 
    }
    return; 
}

void read_in_block_inter(ifstream& fl, int start_index, int stop_index, int can_combine)
{
    int working_size = 0; 
    int unflatten_current_index = 0; 
    if (can_combine) unflatten_map(intraNodeBuffer, unflatten_current_index , working_map, intraNodeBufferSize);
    while (working_size < (INTER_BUFF_SIZE - MAX_WORD_SIZE - MAX_WORD_SIZE))
    {

        get_word(working_map, fl, current_file_index, start_index, min(current_file_index + MAP_SWEEP_LENGTH, stop_index)); 
        flatten_map(intraNodeBuffer, working_size, working_map, INTER_BUFF_SIZE); 
    }
    return; 
}


static void number_as_chars(int num, char *dest, int& output_len) 
{
    int i = 0, aux, div = 1;
    aux = num;
    while (aux > 9) {
        div *= 10;
        aux /= 10;
    }
    aux = num;
    while (div >= 1) {
        dest[i] = (aux / div) % 10 + '0';
        i++;
        div /= 10;
    }
    //dest[i] = '\0';
    output_len = i+1; 
}

static void prep_word(string& cword)
{
    transform(cword.begin(), cword.end(), cword.begin(), ::tolower); 
}

void get_word(map<string, int>& output_map, ifstream& fl, int& _current_index, int start_index,  int stop_index)
{

    //Takes words in range of file index and puts into map 

    //ifstream fl = *fl_in; 
    string first_output_word = ""; 
    
    if (my_rank!=0) //soon this will be !=2 !!
    {
        if (start_index == _current_index)
        {
            string compare_word; 
            fl >> first_output_word; 
            int temp_index = fl.tellg();
            int back_track = _current_index - temp_index; 
            back_track--; 

            fl.seekg(back_track , ios::cur); 
            fl >> compare_word; 

            if (first_output_word.compare(compare_word))
            {
                prep_word(first_output_word); 
                if (output_map.count(first_output_word)) output_map[first_output_word]++; 
                else output_map.insert(std::pair<string, int>(first_output_word, 1)); 
            }
            //otherwise rank -1 will have gotten it 
            _current_index = fl.tellg(); 
        }
    }
    
    while(fl.tellg()<(stop_index-1)) //?
    {

        string cword; 
        fl >> cword; 
        prep_word(cword); 
        if (output_map.count(cword)) output_map[cword]++; 
        else output_map.insert(std::pair<string, int>(cword, 1)); 
    }
    _current_index = fl.tellg(); 
    return; 
}
void unflatten_map(char* input_chars, int& _current_index, map<string, int>& input_map, int stop_index)
{
    char word_buffer[MAX_WORD_SIZE]; 
    char num_buffer[MAX_NUM_SIZE];
    int adding_number = 0; 
    while (_current_index < stop_index)
    {
        //all ordered word, num, word, num etc each with null terminator 
        //word 
        string adding_word = input_chars+_current_index;
        while(input_chars[_current_index++] != '/0') {}
        //number
        adding_number = atoi(input_chars+_current_index); 
        while(input_chars[_current_index++] != '/0') {}
        if (input_map.count(adding_word)) input_map[adding_word]+=adding_number; 
        else input_map.insert(std::pair<string, int>(adding_word, adding_number)); 

    }

}
void flatten_map(char* output, int& _current_index, map<string, int> input_map, int stop_index)
{
    char num_buffer[MAX_NUM_SIZE];
    int num_len; 
    for (map<string, int>::iterator it = input_map.begin(); it != input_map.end();)
    {
        //do something 
        for (string::const_iterator  letter = it->first.begin(); letter!=it->first.end(); ++letter) 
            output[_current_index++] = *letter; 
        output[_current_index++] = '\0'; //null terminate at end of every word

        number_as_chars(it->second, num_buffer, num_len); 
        for (int i = 0; i<num_len; i++)
        {
            output[_current_index] = num_buffer[i]; 
            _current_index++; 
        }
        input_map.erase(it++); 
        if ((stop_index - _current_index) < MAX_NUM_SIZE + MAX_WORD_SIZE) return; 

    }
    return; 
}




