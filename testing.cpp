


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
#include <cctype>
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
#define PRINT_BATMEN_REDUCER_MAPS 0
#define PRINT_MASTER_MAP 1

//we could combine these for efficiency 
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
    current_file_index = input_args->start_read_loc; 
    int outboxHead = 0;
    int outboxTail = 0;
    int outboxSize = 0; 
    int bufferSize = 0; 
    int poll_count = 0; 
    int is_last = 0; 
    int send_complete_flag = 0; 
    MPI_Status send_status; 


    int dum_count = 0; 
    //while (current_index < input_args->stop_read_loc)
    while (current_file_index < input_args->stop_read_loc)
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
            MPI_Test(intraNodeOutboxRequests[outboxTail], &send_complete_flag, 
                &send_status);
            if (send_complete_flag)
            {
                outboxTail = ((outboxTail + 1)%INTRANODE_OUTBOX_SIZE); 
                outboxSize--; 
            }
            else break; 
        }
        //send block
        if ((outboxSize != INTRANODE_OUTBOX_SIZE) && (intraNodeBufferSize != 0))
        {
            intraNodeOutboxLengths[outboxHead] = intraNodeBufferSize; //Add to current buffer head of outbox 
            if (intraNodeBufferSize > INTRA_BUFF_SIZE) cout<<endl<<endl<<"*********overflow*********"<<endl<<endl;
            intraNodeBufferSize = 0; 
            for (int i = 0; i<intraNodeOutboxLengths[outboxHead]; i++) intraNodeOutbox[outboxHead][i] = intraNodeBuffer[i]; 
            //If its last message set Tag = 1
            is_last = (current_file_index < input_args->stop_read_loc) ? 0 : 1; 
            //Send message
            intraNodeOutboxRequests[outboxHead] = new MPI_Request; 
            if (PRINT_EXCHANGE_NUMS) printf("%s : Count = %d Flag = %d Outbox Size = %d \n", debug_id, dum_count, is_last, outboxSize); 
            MPI_Isend(intraNodeOutbox[outboxHead], intraNodeOutboxLengths[outboxHead], MPI_CHAR, 
                input_args->my_partner, is_last, MPI_COMM_WORLD, intraNodeOutboxRequests[outboxHead]);
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
        MPI_Wait(intraNodeOutboxRequests[outboxTail], &send_status); 
        
        outboxTail = ((outboxTail + 1)%INTRANODE_OUTBOX_SIZE); 
        outboxSize--;

    }

}

void do_dumb_reduce(reduceArgs* input_args)
{
    char debug_id_intra[10]; 
    char debug_id_inter[10]; 
    sprintf(debug_id_intra, "%d <- %d", my_rank, input_args->my_partner); 
    sprintf(debug_id_inter, "%d -> M", my_rank); 
    MPI_Request* incoming_request; 
    char incoming_buffer[INTRANODE_BLOCK_SIZE*2*MAX_WORD_SIZE];
    int observed_size; 
    int robin_terminate_flag = 0; 
    MPI_Status incoming_status; 
    int total_recieved = 0; 
    int probe_flag = 0; 
    int recieve_tag; 
    int outboxHead = 0;
    int outboxTail = 0;
    int outboxSize = 0; 
    MPI_Status send_status; 
    int send_complete_flag; 
    int is_last = 0; 
    //TODO: INDBOX (INTRA NODE OUTBOX)
    while(current_file_index<input_args->stop_read_loc || (!robin_terminate_flag))
    {
        //PROBE for recieved message from Robin, if a message is coming: How big? What Tag? 
        MPI_Iprobe(input_args->my_partner, MPI_ANY_TAG, MPI_COMM_WORLD, &probe_flag, &incoming_status); 
        if (probe_flag)
        {
            incoming_request = new MPI_Request; 
            robin_terminate_flag = incoming_status.MPI_TAG;
            MPI_Get_count(&incoming_status, MPI_CHAR, &intraNodeBufferSize);
            //Non blocking recieve 
            MPI_Irecv(intraNodeBuffer, intraNodeBufferSize, MPI_CHAR, input_args->my_partner, robin_terminate_flag,  
                MPI_COMM_WORLD, incoming_request);
            //Print what were waiting ot recieve 
            if (PRINT_EXCHANGE_NUMS) printf("%s : Waiting on Count = %d Flag = %d \n", debug_id_intra, total_recieved+1, robin_terminate_flag); 
            //Blocking wait (change to non_blocking later)
            MPI_Wait(incoming_request, &incoming_status); 

            //Printing
            total_recieved++; 
            if (PRINT_EXCHANGE_NUMS) printf("%s : Count = %d Flag = %d \n", debug_id_intra, total_recieved, robin_terminate_flag); 
            if (robin_terminate_flag) printf("Recieved last packet!! \n\n");
        }
        else intraNodeBufferSize = 0;  //nothing to recieve 
        //Populate inter buffer if space in outbox 
        if (outboxSize != INTERNODE_OUTBOX_SIZE) read_in_block_inter_temp(*input_args->input_file, input_args->start_read_loc, input_args->stop_read_loc);
        //Remove recieved items from Inter-Outbox 
        while (outboxSize > 0) 
        {
            MPI_Test(interNodeOutboxRequests[outboxTail], &send_complete_flag, 
                &send_status);
            if (send_complete_flag)
            {
                outboxTail = ((outboxTail + 1)%INTERNODE_OUTBOX_SIZE); 
                outboxSize--; 
            }
            else break; 
        }
        //Add comptuted messages to Inter-Outbox
        if ((outboxSize != INTERNODE_OUTBOX_SIZE) && (interNodeBufferSize != 0)) 
        {
            interNodeOutboxLengths[outboxHead] = interNodeBufferSize; //Add to current buffer head of outbox 
            interNodeBufferSize = 0; //Reset to 0 so we can write new things to this 
            //Copy Inter buffer to Inter Outbox head
            for (int i = 0; i<interNodeOutboxLengths[outboxHead]; i++) interNodeOutbox[outboxHead][i] = interNodeBuffer[i]; 
            //If it is the last message we set the tag = 1 so Reciever knows to fuck off
            is_last = ((current_file_index >= input_args->stop_read_loc) && robin_terminate_flag) ? 1 : 0; 
            //Non-blocking send of head
            interNodeOutboxRequests[outboxHead] = new MPI_Request; 
            MPI_Isend(interNodeOutbox[outboxHead], interNodeOutboxLengths[outboxHead], MPI_CHAR, 
                master_robin, is_last, MPI_COMM_WORLD, interNodeOutboxRequests[outboxHead]);
            //incrament pointers 
            outboxSize++; 
            outboxHead = ((outboxHead+1)%INTERNODE_OUTBOX_SIZE); 
            if (PRINT_EXCHANGE_NUMS) printf("%s : Count = %d Flag = %d \n", debug_id_inter, total_recieved, is_last); 
            if (is_last) break; 
        }
        

        //PRINT MAP 
        if (PRINT_BATMEN_REDUCER_MAPS)
        {
            printf("%s Map Contents", debug_id_intra); 
            for (auto entry : working_map)
            {
                //printf("[ %s , %d] ", entry.first, entry.second);
                cout<<" [ "<<entry.first<<", "<<entry.second<<"] "; 
            }
            printf("\n"); 
        }

    }
    if (outboxSize > 0)
    {
        //NOTE: cant leave without emptying outbox 
        while (outboxSize > 0)
        {
        //MPI_TEST on intraNodeOutputStatuses[outboxTail]
        MPI_Wait(interNodeOutboxRequests[outboxTail], &send_status); 
        
        outboxTail = ((outboxTail + 1)%INTERNODE_OUTBOX_SIZE); 
        outboxSize--;

        }
    }
    
}    

void do_master_sidekick()
{
    vector<int> done_yet((world_size-2)/2, 0); 
    int incoming_source; 
    int incoming_tag; 
    int incoming_size; 
    MPI_Status incoming_status; 
    MPI_Request* incoming_request; 
    char incoming_buffer[INTER_BUFF_SIZE]; 
    int unflatten_current_index = 0; 
    int probe_flag;
    while (accumulate(done_yet.begin(), done_yet.end(), 0) != ((world_size-2)/2))
    {
        //Get Incoming message info 
        MPI_Iprobe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &probe_flag, &incoming_status); 
        if (!probe_flag) continue; //no messages to recieve 
        incoming_request = new MPI_Request; 
        incoming_source = incoming_status.MPI_SOURCE; 
        incoming_tag = incoming_status.MPI_TAG; 
        MPI_Get_count(&incoming_status, MPI_CHAR, &incoming_size);
        done_yet[incoming_source] = incoming_tag; 
        //Recvieve incoming message 
        MPI_Irecv(incoming_buffer, incoming_size, MPI_CHAR, incoming_source, incoming_tag,  
            MPI_COMM_WORLD, incoming_request);
        MPI_Wait(incoming_request, &incoming_status);
        //unflatten and add to map 
        unflatten_current_index = 0;

        unflatten_map(incoming_buffer, unflatten_current_index, working_map, incoming_size);
        if (PRINT_EXCHANGE_NUMS) printf("Master Robin: Source =  %d Flag = %d \n", incoming_source, incoming_tag); 
        
        //PRINT MAP
        if (PRINT_MASTER_MAP)
        {
            printf("\n"); 
            for (auto entry : working_map)
            {
                cout<<"["<<entry.first<<", "<<entry.second<<"] ";
            }
            printf("\n"); 
        }
        

    }


}


//********************************************************************
//********************************************************************
//main
//********************************************************************
//********************************************************************
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
    master_batman = 0; 
    master_robin = robin[0];
    int num_recievers = world_size-2; //soon this will be world size -2 !!!!



    if (my_rank == 0)
    {
        //not yet built 
    }
    else if (my_rank == robin[0])
    {
        printf("I am master robin with rank # %d and processor name %s \n", my_rank, processor_name); 
        do_master_sidekick(); 
    }
    else 
    {

        
        ifstream fl(INPUT_FILE_PATH, ios::in); 
        fl.seekg(0, ios::end); 
        size_t len = fl.tellg();
        file_len = (int)len;
        fl.seekg(0, ios::beg);
        int my_block_size = file_len/num_recievers; 
        int my_start_index = my_block_size*(my_rank-2); //(my_block_size*(my_rank-2)) !!!
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
    }

    MPI_Finalize();
    return 0;
}
//********************************************************************
//********************************************************************
//Processing 
//********************************************************************
//********************************************************************
void read_in_block_intra(ifstream& fl, int start_index, int stop_index)
{

    get_word(working_map, fl, current_file_index, start_index, min(current_file_index + MAP_SWEEP_LENGTH, stop_index)); 
    flatten_map(intraNodeBuffer, intraNodeBufferSize, working_map, INTRA_BUFF_SIZE); 
    return; 
}
void read_in_block_inter_temp(ifstream& fl, int start_index, int stop_index)
{
    int unflatten_current_index = 0; 
    //unflatten interbufffer (recieved from robin)
    unflatten_map(intraNodeBuffer, unflatten_current_index, working_map, intraNodeBufferSize); 
    get_word(working_map, fl, current_file_index, start_index, min(current_file_index + MAP_SWEEP_LENGTH, stop_index)); 
    flatten_map(interNodeBuffer, interNodeBufferSize, working_map, INTER_BUFF_SIZE); 
    return; 
}

void read_in_block_inter(ifstream& fl, int start_index, int stop_index, int can_combine)
{
    int unflatten_current_index = 0; 
    //unflatten interbufffer (recieved from robin)
    if (can_combine) unflatten_map(intraNodeBuffer, unflatten_current_index , working_map, intraNodeBufferSize);
    while (interNodeBufferSize < ((INTER_BUFF_SIZE - MAX_WORD_SIZE) - MAX_NUM_SIZE))
    {

        get_word(working_map, fl, current_file_index, start_index, min(current_file_index + MAP_SWEEP_LENGTH, stop_index)); 
        flatten_map(interNodeBuffer, interNodeBufferSize, working_map, INTER_BUFF_SIZE); 
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
    dest[i] = '\0';
    output_len = i+1; 
}

static void prep_word(string& cword)
{
    cword.erase (std::remove_if (cword.begin (), cword.end (), ::ispunct), cword.end ());
    transform(cword.begin(), cword.end(), cword.begin(), ::tolower); 
    //cword.erase(std::remove_if (cword.begin(), cword.end(), ::isspace), cword.end()); 
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
            prep_word(compare_word); 
            prep_word(first_output_word); 


            if ((!first_output_word.compare(compare_word)) && compare_word.size())
            {
                if (output_map.count(first_output_word)) output_map[first_output_word]++; 
                else output_map.insert(std::pair<string, int>(first_output_word, 1)); 
            }
            //otherwise rank -1 will have gotten it 
            _current_index = fl.tellg(); 
        }
    }
    
    while(fl.tellg()<(stop_index-1)) 
    {

        string cword; 
        fl >> cword; 
        prep_word(cword); 
        if (!cword.size()) continue; 
        if (output_map.count(cword)) output_map[cword]++; 
        else output_map.insert(std::pair<string, int>(cword, 1)); 
    }
    _current_index = fl.tellg(); 
    return; 
}

void flatten_map(char* output, int& _current_index, map<string, int> input_map, int stop_index)
{
    char num_buffer[MAX_NUM_SIZE];
    int num_len; 
    for (map<string, int>::iterator it = input_map.begin(); it != input_map.end();)
    {

        if (!it->first.size()) input_map.erase(it++); 
        //add word
        for (string::const_iterator  letter = it->first.begin(); letter!=it->first.end(); ++letter) 
            output[_current_index++] = *letter; 
        output[_current_index++] = '\0'; //null terminate at end of every word

        //corresponding number 
        
        number_as_chars(it->second, num_buffer, num_len); 
        for (int i = 0; i<num_len; i++)
        {
            output[_current_index] = num_buffer[i]; 
            _current_index++; 
        }
        if ((num_len <= 1) || (it->first.size() == 0) || (it->second == 0))
        {
            printf("\n FLATTEN: invalid number added strin = %s number = %d rank = %d \n", it->first.c_str(), it->second, my_rank);
        }
        input_map.erase(it++);
        
        if ((stop_index - _current_index) < (MAX_NUM_SIZE + MAX_WORD_SIZE)) return; 
    }
    return; 
}
void unflatten_map(char* input_chars, int& _current_index, map<string, int>& input_map, int stop_index)
{
    char word_buffer[MAX_WORD_SIZE]; 
    char num_buffer[MAX_NUM_SIZE];
    int adding_number = 0; 
    char debug_neighbors[2]; 
    while (_current_index < stop_index)
    {
        //all ordered word, num, word, num etc each with null terminator 
        //word 
        string adding_word = input_chars+_current_index;
        while(input_chars[_current_index++] != '\0') {}
        if (_current_index >= stop_index) //this should never happen 
        {
            printf("\n UNFLATTEN: VIOLATED STRING | NUMBER CONVENTION, String = %s has no cooresonding frequency rank = %d\n", adding_word.c_str(), my_rank); 
            break; 
        }
        //number
        debug_neighbors[0] = input_chars[_current_index -1]; 
        adding_number = atoi(input_chars+_current_index); 
        debug_neighbors[1] = input_chars[_current_index +1]; 
        while(input_chars[_current_index++] != '\0') {}
        //debug 
        if (adding_number <= 0)
        {
            printf("\n UNFLATTEN: FOUND INVALID NUMBER = %d, cooresponding word is %s, neighbors are [%c, %c] rank = %d\n", 
            adding_number, adding_word.c_str(), debug_neighbors[0], debug_neighbors[1], my_rank);
        }
        if (input_map.count(adding_word)) input_map[adding_word]+=adding_number; 
        else input_map.insert(std::pair<string, int>(adding_word, adding_number)); 

    }
    return; 
}



