
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



#include <"mpi.h">
#include<map> 


#define INTRANODE_BLOCK_SIZE 50 
#define INTRA_INTER_MULTIPLE 1 //wait no they should be the same size this is stupid 
#define INTERNODE_BLOCK_SIZE (INTRANODE_BLOCK_SIZE*INTA_INTER_MULTIPLE)
#define INTERNODE_OUTBOX_SIZE 50 
#define INTRANODE_OUTBOX_SIZE 50 
#define POLL_FREQUENCY 5 
#define MAX_WORD_SIZE 50 

//we could combine these for efficiency 
char intraNodeOutbox[INTRANODE_OUTBOX_SIZE][INTRANODE_BLOCK_SIZE*MAX_WORD_SIZE*2];
int intraNodeOutBoxLengths[INTRANODE_OUTBOX_SIZE];
char interNodeOutbox[INTERNODE_OUTBOX_SIZE][INTERNODE_BLOCK_SIZE*MAX_WORD_SIZE*2];
int intraNodeOutboxLengths[INTRANODE_OUTBOX_SIZE];
int interNodeOutboxLengths[INTERNODE_OUTBOX_SIZE];
MPI_Request intraNodeOutboxStatuses[INTRANODE_OUTBOX_SIZE]; 
MPI_Request interNodeOutboxStatuses[INTERNODE_OUTBOX_SIZE]; 

char intraNodeBuffer[2*MAX_WORD_SIZE*INTRANODE_BLOCK_SIZE];
char interNodeBuffer[2*MAX_WORD_SIZE*INTERNODE_BLOCK_SIZE];
map<string, int> output_map; 
int intraNodeBufferSize;
int interNodeBufferSize; 
int world_size; 

void read_in_block(filedescriptor, int, int); 
void combine_buffers(); 
void combine_all_buffers(char*); 
void add_buffer_to_map; 
int* batmen; 



struct reduceArgs
{
    //[start, stop)
    int start_read_loc; 
    int stop_read_loc;
    int my_twin;
    char* input_file_location;
    int is_end; 
};

void do_reduce_sidekick(reduceArgs* input_args)
{

    int current_index = input_args->start_read_loc; 
    int outboxHead = 0;
    int outboxTail = 0;
    int outboxSize = 0; 
    int bufferSize = 0; 
    int poll_count = 0; 
    bool is_last = 0; 
    int send_complete_flag = 0; 
    MPI_Status send_status; 




    while (current_index < input_args->stop_read_loc)
    {
        //read in block from file 
        if (outboxSize != INTRANODE_OUTBOX_SIZE) read_in_block(workingFD, current_index, 0);  

        //read block adds data to map 
        
        //check to see if messages have been recieved 
        while (outboxSize > 0)
        {
            //MPI_TEST on intraNodeOutputStatuses[outboxTail]
            if (MPI_Test(intraNodeOutputStatuses[outboxTail], send_complete_flag, 
                &send_status))
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
            intraNodeOutboxLengths[head] = intraNodeBufferSize; 
            for (int i = 0; i<intraNodeBufferSize; i++)
            {
                intraNodeOutbox[head][i] = intraNodeBuffer[i]; 
            }
            //send
            is_last = (current_index < input_args->stop_read_loc) ? 0 : 1; 
            MPI_Isend(intraNodeOutbox[outboxHead], intradNodeOutboxLengths[head], MPI_Char, 
                input_args->my_twin, is_last, MPI_COMM_WORLD);
            //incrament pointers 
            outboxSize++; 
            outboxHead = ((outboxHead+1)%INTRANODE_OUTBOX_SIZE); 
        }

    }
}

void do_reduce(reduceArgs* input_args)
{
    MPI_request incoming_request;
    int incoming_tag; 
    int is_last_incoming;
    int is_last_outgoing; 
    int current_index = input_args->start_read_loc; 
    int outboxHead = 0;
    int outboxTail = 0;
    int outboxSize = 0; 
    int bufferSize = 0; 

    while((!is_last_outgoing) &&  (current_index < input_args->stop_read_loc))
    {

        current_index < input_args->stop_read_loc; 
        if (current_index < input_args->stop_read_loc) 
            read_in_block(workingFD, current_index, 1); 
        else interNodeBufferSize = 0
        MPI_irecv(intraNodeBuffer, &intraNodeBufferSize, MPI_Char, input_args->my_twin, is_last_incoming, 
            MPI_COMM_WORLD);
        MPI_Wait(&incoming_request, NULL, input_args->my_twin); 
        combine_buffers(); 
        //check to see if messages have been recieved 
        while (outboxSize > 0)
        {

            if (MPI_Test(interNodeOutputStatuses[outboxTail], send_complete_flag, 
                &send_status))
            {
                outboxTail = ((outboxTail + 1)%INTERNODE_OUTBOX_SIZE); 
                outboxSize--; 
            }
            else break; 
        }

        if (outboxSize != INTERNODE_OUTBOX_SIZE)
        {
            //Add to current buffer head of outbox 
            interNodeOutboxLengths[head] = interaNodeBufferSize; 
            for (int i = 0; i<intraNodeBufferSize; i++)
            {
                interNodeOutbox[head][i] = interNodeBuffer[i]; 
            }
            //send
            is_last_outgoing = (current_index < input_args->stop_read_loc) ? 0 : 1; 
            MPI_Isend(interNodeOutbox[outboxHead], interdNodeOutboxLengths[head], MPI_Char, 
                input_args->my_mapper, is_last_outgoing, MPI_COMM_WORLD);
            //incrament pointers 
            outboxSize++; 
            outboxHead = ((outboxHead+1)%INTRANODE_OUTBOX_SIZE); 
        }  

    }
}

void do_map()
{
    int is_last = 0; 
    char* buffer = new char[num_incoming*2*MAX_WORD_SIZE*INTERNODE_BLOCK_SIZE]; 
    int buffer_size; 
    
    

    while(!is_last)
    {
        MPI_recv(buffer, buffer_size, MPI_CHAR, robin[0], is_last, MPI_WORLD_COMM, MPI_STATUS_IGNORE); 
        add_buffer_to_map(output_map, buffer, buffer_size); 
    }

    write_to_output_file(); 

}

void do_map_sidekick()
{
    int is_last_outgoing = 0; 
    int is_last_incoming = new int[world_size - 2]
    int num_incoming = (world_size/2) -1; 
    MPI_Requests* current_requests = new MPI_Request[world_size];
    char* outgoing_buffer = new char[num_incoming*2*MAX_WORD_SIZE*INTERNODE_BLOCK_SIZE]; 
    int outgoing_buffer_size; 
    //declare incoming buffer
    int incoming_buffer_sizes = new int[num_incoming];
    char** incoming_buffers = new char*[num_incoming]; 
    for (int i = 0; i<num_incoming; i++)
    {
        incoming_buffers[i] = new char[2*MAX_WORD_SIZE*INTERNODE_BLOCK_SIZE]; 
    }
    
    while (!is_last_outgoing)
    {

        for (int i = 0; i<num_incoming; i++)
        {
            if (is_last_incoming[i]) continue; 
            MPI_irecv(incoming_buffers[i], &incoming_buffer_sizes[i], MPI_Char, batmen[i+1], is_last_incoming[i], 
            MPI_COMM_WORLD);
            MPI_Test(current_requests[i], NULL, NULL, NULL);  
        }
        for (int i = 0; i<num_incoming; i++)
        {
            if (is_last_incoming[i] <= 1)
                MPI_Wait(current_requests[i], NULL));
            if (is_last_incoming[i] == 1) is_last_incoming[i]++; 
        }
        combine_all_buffers(incoming_buffers, incoming_buffer_sizes, outgoing_buffer, outgoing_buffer_size);
        for (int i = 0; i<num_incoming; i++)
        {
            if (!is_last_incoming) break; 
            if (i == (num_incoming-1)) is_last_outgoing = 1; 
        }

        MPI_send(outgoing_buffer, outgoing_buffer_size, MPI_CHAR, batman[0], is_last_outgoing, MPI_COMM_WORLD); 
    }
        
    free current_requests; 
    free is_last_incoming; 
    for (int i = 0; i<num_incoming; i++)
    {
        free incoming_buffers[i]; 
    }
    free incoming_buffers; 
    free incoming_buffer_sizes; 

}

int main(int argc, char **argv)
{
    MPI_Comm parentcomm;

    MPI_Init(&argc, &argv);
    MPI_Comm_get_parent(&parentcomm); 
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);
    char* processor_names = new char[world_size*MPI_MAX_PROCESSOR_NAME]; 
    char processor_name[MPI_MAX_PROCESSOR_NAME];
    int processor_name_result_len; 
    MPI_Get_processor_name(processor_name, &processor_name_result_len);
    for (int i = processor_name_result_len; i<MPI_MAX_PROCESSOR_NAME)
    {
        processor_name[i] = '\0';
    }
    MPI_Allgather(processor_name, MPI_MAX_PROCESSOR_NAME, MPI_CHAR, processor_names, 
        world_size*MPI_MAX_PROCESSOR_NAME, MPI_CHAR, MPI_COMM_WORLD); 
    //declare who are the batmen 
    
    if (parentcomm == MPI_COMM_NULL)
        //I am master 
        do_map()
    else
        // I am a reducer 
        // find out what my range is 
            //code here handle boarder words 
            //If my range starts with 0, take until end of word
            //if if my node is not 0, make sure to ommit any beginig half words
            //If my node is at the end of file => make sure not to extend 
        int batman; 
        reduceArgs input_args; 
        //assign values 
        if (batman) do_reduce(input_args);
        else do_reduce_sidekick(input_args); 
    free batmen; 
    MPI_Finalize();
    return 0;
}



// Puts a number in a char *
static void number_as_chars(int num, char *dest) {
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
}
