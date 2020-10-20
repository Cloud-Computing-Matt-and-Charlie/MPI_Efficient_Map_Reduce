
#include "general.h"



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
    int file_read_occurences = 0; 
    MPI_Status send_status; 

    int dum_count = 0; 
    while ((current_file_index < input_args->stop_read_loc) || working_map.size())
    {
        dum_count++; 
        //read in chunk of words from file 
        if (outboxSize != INTRANODE_OUTBOX_SIZE) read_in_block_intra(*input_args->input_file, input_args->start_read_loc, input_args->stop_read_loc);
        if ((!((file_read_occurences++)%PRINT_FILE_READ_PROGRESS_PERIOD)) && PRINT_FILE_READ_PROGRESS)
            printf("%s: is currently at read location %d in [%d, %d) \n", debug_id, current_file_index, input_args->start_read_loc, input_args->stop_read_loc);
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
        //send chunk of words
        if ((outboxSize != INTRANODE_OUTBOX_SIZE) && (intraNodeBufferSize != 0))
        {
            intraNodeOutboxLengths[outboxHead] = intraNodeBufferSize; //Add to current buffer head of outbox 
            if (intraNodeBufferSize > INTRA_BUFF_SIZE) cout<<endl<<endl<<"*********overflow*********"<<endl<<endl;
            intraNodeBufferSize = 0; 
            for (int i = 0; i<intraNodeOutboxLengths[outboxHead]; i++) intraNodeOutbox[outboxHead][i] = intraNodeBuffer[i]; 
            //If its last message set Tag = 1
            is_last = ((current_file_index < input_args->stop_read_loc) || working_map.size()) ? 0 : 1; 
            //Send message
            intraNodeOutboxRequests[outboxHead] = new MPI_Request; 
            if (PRINT_EXCHANGE_NUMS) printf("%s : Count = %d Flag = %d Outbox Size = %d \n", debug_id, dum_count, is_last, outboxSize); 
            MPI_Isend(intraNodeOutbox[outboxHead], intraNodeOutboxLengths[outboxHead], MPI_CHAR, 
                input_args->my_partner, is_last, MPI_COMM_WORLD, intraNodeOutboxRequests[outboxHead]);
            //incrament pointers 
            outboxSize++; 
            outboxHead = ((outboxHead+1)%INTRANODE_OUTBOX_SIZE); 
            if (is_last) printf("exiting sender %d -> %d \n", my_rank, input_args->my_partner); 
        }

    }

    while (outboxSize > 0)
    {
        MPI_Wait(intraNodeOutboxRequests[outboxTail], &send_status); 
        outboxTail = ((outboxTail + 1)%INTRANODE_OUTBOX_SIZE); 
        outboxSize--;
    }

}

void do_reduce(reduceArgs* input_args)
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
    current_file_index = input_args->start_read_loc; 
    MPI_Status send_status; 
    int send_complete_flag; 
    int file_read_occurences = 0; 
    int is_last = 0; 
    //TODO: INDBOX (INTRA NODE OUTBOX)
    while(current_file_index<=(input_args->stop_read_loc-1) || (!robin_terminate_flag) || working_map.size())
    {
        if (!robin_terminate_flag)
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
                //Print what were waiting to recieve 
                if (PRINT_EXCHANGE_NUMS) printf("%s : Waiting on Count = %d Flag = %d \n", debug_id_intra, total_recieved+1, robin_terminate_flag); 
                MPI_Wait(incoming_request, &incoming_status); 

                //Printing
                total_recieved++; 
                if (PRINT_EXCHANGE_NUMS) printf("%s : Count = %d Flag = %d \n", debug_id_intra, total_recieved, robin_terminate_flag); 
                if (robin_terminate_flag) printf("Recieved last packet from %d!! \n\n", input_args->my_partner);
            }
            else intraNodeBufferSize = 0;  //nothing to recieve 

            if ((!probe_flag) && PRINT_EXCHANGE_NUMS)
            {
                //printf("%s: No message to recieve \n", debug_id_intra);
            }
        }
        //Populate inter buffer if space in outbox 
        if (outboxSize != INTERNODE_OUTBOX_SIZE) read_in_block_inter(*input_args->input_file, input_args->start_read_loc, input_args->stop_read_loc);
        if ((!((file_read_occurences++)%PRINT_FILE_READ_PROGRESS_PERIOD)) && PRINT_FILE_READ_PROGRESS)
        {
            printf("%s: is currently at read location %d in [%d, %d) terminate flag = %d intersize = %d intrasize = %d outbox = %d\n", debug_id_inter, current_file_index, input_args->start_read_loc, input_args->stop_read_loc, 
                robin_terminate_flag, interNodeBufferSize, intraNodeBufferSize, outboxSize);
        }
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
            //If it is the last message we set the tag = 1 so reciever @ master processer knows were done 
            is_last = ((current_file_index >= (input_args->stop_read_loc-1)) && robin_terminate_flag && (!working_map.size())) ? 1 : 0; 
            //Non-blocking send of head
            interNodeOutboxRequests[outboxHead] = new MPI_Request; 
            MPI_Isend(interNodeOutbox[outboxHead], interNodeOutboxLengths[outboxHead], MPI_CHAR, 
                master_robin, is_last, MPI_COMM_WORLD, interNodeOutboxRequests[outboxHead]);
            //incrament pointers 
            outboxSize++; 
            outboxHead = ((outboxHead+1)%INTERNODE_OUTBOX_SIZE); 
            if (PRINT_EXCHANGE_NUMS) printf("%s : Count = %d Flag = %d \n", debug_id_inter, total_recieved, is_last); 
            if (is_last) printf("Exiting sender %s \n", debug_id_inter); 
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
    int incoming_tag  = 0;
   //int incoming_size; 
    MPI_Status incoming_status; 
    MPI_Request* incoming_request; 
    //char incoming_buffer[INTER_BUFF_SIZE]; 
    int unflatten_current_index = 0; 
    int probe_flag;
    int period_counter; 
    int outboxHead = 0;
    int outboxTail = 0;
    int outboxSize = 0; 
    int is_last; 
    int send_complete_flag; 
    MPI_Status send_status; 
    while (accumulate(done_yet.begin(), done_yet.end(), 0) != ((world_size-2)/2))
    {
        //mark off recieved messages from outbox
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
        if (outboxSize > INTERNODE_OUTBOX_SIZE) continue; //wait till more space in outbox 
        //Get Incoming message info 
        MPI_Iprobe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &probe_flag, &incoming_status); 
        //if ((!probe_flag) && PRINT_EXCHANGE_NUMS) printf("Master Robin: Nothing to recieve :/ \n");
        if (!probe_flag) continue; //no messages to recieve 
        incoming_request = new MPI_Request; 
        incoming_source = incoming_status.MPI_SOURCE; 
        incoming_tag = incoming_status.MPI_TAG; 
        MPI_Get_count(&incoming_status, MPI_CHAR, &interNodeBufferSize);
        done_yet[std::distance(batmen.begin(), std::find(batmen.begin(), batmen.end(), incoming_source)) - 1] = incoming_tag; 
        //Recvieve incoming message 
        MPI_Irecv(interNodeBuffer, interNodeBufferSize, MPI_CHAR, incoming_source, incoming_tag,  
            MPI_COMM_WORLD, incoming_request);
        MPI_Wait(incoming_request, &incoming_status);
        if (PRINT_EXCHANGE_NUMS) printf("Master Robin: Source =  %d Flag = %d \n", incoming_source, incoming_tag); 
        if (PRINT_EXCHANGE_NUMS) printf("Master Robin: Recieved %d / %d terminations \n", accumulate(done_yet.begin(), done_yet.end(), 0), ((world_size-2)/2));
        if (outboxSize != INTERNODE_OUTBOX_SIZE)
        {
            interNodeOutboxLengths[outboxHead] = interNodeBufferSize; //Add to current buffer head of outbox 
            interNodeBufferSize = 0; //Reset to 0 so we can write new things to this 
            //Copy Inter buffer to Inter Outbox head
            for (int i = 0; i<interNodeOutboxLengths[outboxHead]; i++) interNodeOutbox[outboxHead][i] = interNodeBuffer[i]; 
            //If it is the last message we set the tag = 1 so Reciever knows to fuck off
            is_last = (accumulate(done_yet.begin(), done_yet.end(), 0) == ((world_size-2)/2)) ? 1 : 0; 
            //Non-blocking send of head
            interNodeOutboxRequests[outboxHead] = new MPI_Request; 
            MPI_Isend(interNodeOutbox[outboxHead], interNodeOutboxLengths[outboxHead], MPI_CHAR, 
                master_batman, is_last, MPI_COMM_WORLD, interNodeOutboxRequests[outboxHead]);
            if (PRINT_EXCHANGE_NUMS) printf("Master Robin: Sent to Master, Outbox Size = %d  \n", outboxSize); 

            //incrament pointers 
            outboxSize++; 
            outboxHead = ((outboxHead+1)%INTERNODE_OUTBOX_SIZE); 
            if (is_last) break; 
        }


    }
    //NOTE: cant leave without emptying outbox 
    while (outboxSize > 0)
    {
    //MPI_TEST on intraNodeOutputStatuses[outboxTail]
    MPI_Wait(interNodeOutboxRequests[outboxTail], &send_status); 
    
    outboxTail = ((outboxTail + 1)%INTERNODE_OUTBOX_SIZE); 
    outboxSize--;

    }


}
void do_master()
{
    int incoming_source; 
    int incoming_tag = 0; 
    int incoming_size; 
    MPI_Status incoming_status; 
    MPI_Request* incoming_request; 
    char incoming_buffer[INTER_BUFF_SIZE]; 
    int unflatten_current_index = 0; 
    int probe_flag;
    int period_counter = 0; 

    
    while(!incoming_tag)
    {
        MPI_Iprobe(master_robin, MPI_ANY_TAG, MPI_COMM_WORLD, &probe_flag, &incoming_status); 
        if (!probe_flag) continue; //no messages to recieve 
        //else printf("DELETE Master Batman Waiting: Flag = %d \n", incoming_tag); 
        incoming_request = new MPI_Request; 
        incoming_source = incoming_status.MPI_SOURCE; 
        incoming_tag = incoming_status.MPI_TAG; 
        MPI_Get_count(&incoming_status, MPI_CHAR, &incoming_size);
        MPI_Irecv(incoming_buffer, incoming_size, MPI_CHAR, master_robin, incoming_tag,  
            MPI_COMM_WORLD, incoming_request);
        if (PRINT_EXCHANGE_NUMS) printf("Master Batman Waiting: Flag = %d \n", incoming_tag); 
        MPI_Wait(incoming_request, &incoming_status);
        if (PRINT_EXCHANGE_NUMS) printf("Master Batman: Flag = %d \n", incoming_tag); 
        //unflatten and add to map 
        unflatten_current_index = 0;
        unflatten_map(incoming_buffer, unflatten_current_index, working_map, incoming_size);
        //PRINT MAP
        period_counter++; 
        if (PRINT_MASTER_MAP && !(period_counter++%PRINT_MASTER_MAP_PERIOD))
        {
            printf("\n"); 
            for (auto entry : working_map)
            {
                cout<<"["<<entry.first<<", "<<entry.second<<"] ";
            }
            printf("\n"); 
        }
        if (PRINT_HEAP && !((period_counter)%PRINT_HEAP_PERIOD))
        {
            get_most_least_frequent();
            printf("\n Most Frequently Used Words \n"); 
            print_pq<priority_queue<std::pair<string, int>, std::vector<std::pair<string, int>>, min_comapare_obs_freq>>(min_heap); 
            printf("\n Least Frequently Used Words \n");
            print_pq<priority_queue<std::pair<string, int>, std::vector<std::pair<string, int>>, max_comapare_obs_freq>>(max_heap); 
            printf("\n\n"); 
        }
    }
    printf("Writing %d words to file: %s ", working_map.size(), INPUT_FILE_PATH); 
    get_most_least_frequent();
    print_map_to_file(working_map, OUTPUT_FILE_PATH); 

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

    const double megabyte = 1024 * 1024;
    struct sysinfo si;
    sysinfo (&si);
    int my_ram = si.totalram/megabyte; 
    int* rams = new int[world_size]; 
    MPI_Allgather(&my_ram, 1, MPI_INT, rams, 1, MPI_INT, MPI_COMM_WORLD); 
    int ram_sum = 0; 
    for (int i = 2; i<world_size; i++) ram_sum+=rams[i]; 
    int* portions = new int[world_size-2]; 
    
    double portion = ram_sum / ((world_size-2)*16); 
    for (int i = 0; i<(world_size-2); i++) portions[i] = rams[i+2]/portion; 
    portions[3] = 1; 
    int my_portions = portions[my_rank -2]; 



    if (my_rank == 0)
    {
        printf("I am master batman with rank # %d and processor name %s \n", my_rank, processor_name); 
        do_master();
    }
    else if (my_rank == robin[0])
    {
        printf("I am master robin with rank # %d and processor name %s \n", my_rank, processor_name); 
        do_master_sidekick(); 
    }
    else 
    {

        
        ifstream fl(INPUT_FILE_PATH, ios::in); 
        if (!fl.is_open())
        {
            printf("Error in rank = %d \n Opeing file %s with errno = %s \n", my_rank, INPUT_FILE_PATH, strerror(errno)); 
            printf("\n"); 
            return 0; 

        }
        
        fl.seekg(0, ios::end); 
        size_t len = fl.tellg();
        file_len = (int)len;
        if (my_rank == 2) printf("File length is %d \n", file_len); 
        fl.seekg(0, ios::beg);
    
        int my_block_size = file_len; 
        my_block_size /= ((world_size-2)*16);
        int single_portion = (file_len/((world_size-2)*4));
        my_block_size *= my_portions; 
        
        //int my_start_index = my_block_size*(my_rank-2); //(my_block_size*(my_rank-2)) !!!
        int my_start_index = 0; 
        for (int i = 0; i<(my_rank-2); i++) my_start_index+= portions[i]*single_portion; 
        int my_stop_index = my_start_index+my_block_size; 
        if (my_rank == (world_size-1)) my_stop_index = file_len; 
        input_args->start_read_loc = my_start_index; 
        input_args->stop_read_loc = my_stop_index; 
        input_args->input_file = &fl; 
        
        if (std::find(batmen.begin(), batmen.end(), my_rank) != batmen.end())
        {
            printf("I am a batman with rank # %d and processor name %s\n", my_rank, processor_name); 
            printf("Rank = %d: My ram space is %d, and I read in [%d, %d) \n", my_rank, my_ram, input_args->start_read_loc, input_args->stop_read_loc); 
            do_reduce(input_args); 
        }
        else 
        {
            printf("I am a robin with rank # %d and processor name %s\n", my_rank, processor_name); 
            printf("Rank = %d: My ram space is %d, and I read in [%d, %d) \n", my_rank, my_ram, input_args->start_read_loc, input_args->stop_read_loc); 
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
    if (MODE_WORD_CHAR) get_chars(working_map, fl, current_file_index, start_index, min(current_file_index + MAP_SWEEP_LENGTH, stop_index)); 
    else get_words(working_map, fl, current_file_index, start_index, min(current_file_index + MAP_SWEEP_LENGTH, stop_index)); 
    flatten_map(intraNodeBuffer, intraNodeBufferSize, working_map, INTRA_BUFF_SIZE); 
    return; 
}
void read_in_block_inter(ifstream& fl, int start_index, int stop_index)
{
    int unflatten_current_index = 0; 
    unflatten_map(intraNodeBuffer, unflatten_current_index, working_map, intraNodeBufferSize); 
    intraNodeBufferSize = 0;
    if (MODE_WORD_CHAR) get_chars(working_map, fl, current_file_index, start_index, min(current_file_index + MAP_SWEEP_LENGTH, stop_index));
    else get_words(working_map, fl, current_file_index, start_index, min(current_file_index + MAP_SWEEP_LENGTH, stop_index)); 
    flatten_map(interNodeBuffer, interNodeBufferSize, working_map, INTER_BUFF_SIZE); 
    return; 
}


void get_words(map<string, int>& output_map, ifstream& fl, int& _current_index, int start_index,  int stop_index)
{

    //Takes words in range of file index and puts into map 
    string first_output_word = ""; 
    
    if (my_rank!=2)  //lowest numbered reducer = 2
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
    int last; 
    int old_last = 0; 
    int dead_count = 0; 
    while(fl.tellg()<=(stop_index-1)) 
    {

        string cword; 
        fl >> cword; 
        prep_word(cword); 
        //if (dead_count >= 100) fl.seekg(1, ios::cur);
        
        last = (int)fl.tellg(); 
        //if (old_last == last && old_last) dead_count++; 
        if (old_last == last && old_last) fl.seekg(1, ios::cur); 
        old_last = last; 
        if (fl.peek() == EOF) 
        {
            _current_index = stop_index; 
            printf("\n\n\n TERMINATING EARLY \n\n"); 
            return; 
        }
        //if (dead_count == 100) break;  
        if (!cword.size()) continue; 
        dead_count = 0; 
        if (output_map.count(cword)) output_map[cword]++; 
        else output_map.insert(std::pair<string, int>(cword, 1)); 

    }
    _current_index = fl.tellg(); 
    return; 
}

void get_chars(map<string, int>& output_map, ifstream& fl, int& _current_index, int start_index,  int stop_index)
{
    char temp; 
    string temp_string = "";  
    while(_current_index<stop_index)
    {
        
        fl.get(temp); 
        if (!is_valid_char(temp)) continue; 
        temp_string += temp; 
        if (output_map.count(temp_string)) output_map[temp_string]++; 
        else output_map.insert(std::pair<string, int>(temp_string, 1)); 
        temp_string.pop_back(); 
        _current_index++; 
    }
}

void flatten_map(char* output, int& _current_index, map<string, int>& input_map, int stop_index)
{
    //NOTE: I choose here to erase in reverse alphabetical order...
    //unless main character of book has first name zebra should be good 
    char num_buffer[MAX_NUM_SIZE];
    int num_len; 
    int count = 0; 
    for (auto it = input_map.rbegin(); it != input_map.rend(); it++)
    {
        count++; 
        if (!it->first.size()) continue; 
        //add word
        for (string::const_iterator  letter = it->first.begin(); letter!=it->first.end(); ++letter) 
            output[_current_index++] = *letter; 
        output[_current_index++] = '\0'; //null terminate at end of every word
        //corresponding number 
        number_as_chars(it->second, num_buffer, num_len); 
        for (int i = 0; i<num_len; i++) output[_current_index++] = num_buffer[i]; //already has null terminator 
        if ((stop_index - _current_index) < (MAX_NUM_SIZE + MAX_WORD_SIZE)) break; 
    }
    for (int i = 0; i<count; i++) 
    {
        auto it = input_map.end(); 
        it--; 
        input_map.erase(it); 
    }
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
        while(input_chars[_current_index++] != '\0') {}
        //number
        adding_number = atoi(input_chars+_current_index); 
        while(input_chars[_current_index++] != '\0') {}
        if (input_map.count(adding_word)) input_map[adding_word]+=adding_number; 
        else input_map.insert(std::pair<string, int>(adding_word, adding_number)); 

    }
    return; 
}


void get_most_least_frequent()
{
    largest_frequencies.clear();
    while (min_heap.size()) min_heap.pop(); 
    smallest_frequencies.clear(); 
    while(max_heap.size()) max_heap.pop(); 
    for (auto entry : working_map)
    {
        // Use min heap for most frequent and max heap for least frequent 
        
        //Find most used with min_heap
        if (largest_frequencies.size() < HEAP_SIZE)
        {
            min_heap.push(std::pair<string, int>(entry.first, entry.second));       
            largest_frequencies.insert(entry.second);             
        } 
        else if ((*largest_frequencies.rbegin()) <= entry.second)
        {
            min_heap.push(std::pair<string, int>(entry.first, entry.second));       
            largest_frequencies.insert(entry.second); 
        }
        while (largest_frequencies.size() > HEAP_SIZE)
        {
            int temp = (*largest_frequencies.rbegin()); 
            largest_frequencies.erase(prev(largest_frequencies.end())); 
            while (min_heap.top().second < (*largest_frequencies.rbegin())) min_heap.pop(); 
        }

        //Find least used with max_heap
        if (largest_frequencies.size() < HEAP_SIZE)
        {
            max_heap.push(std::pair<string, int>(entry.first, entry.second));       
            smallest_frequencies.insert(entry.second);             
        } 
        else if ((*smallest_frequencies.rbegin()) >= entry.second)
        {
            max_heap.push(std::pair<string, int>(entry.first, entry.second));       
            smallest_frequencies.insert(entry.second); 
        }
        while (largest_frequencies.size() > HEAP_SIZE)
        {
            int temp = (*smallest_frequencies.rbegin()); 
            smallest_frequencies.erase(prev(smallest_frequencies.end())); 
            while (max_heap.top().second < (*smallest_frequencies.rbegin())) max_heap.pop(); 
        }
    }



}

template <typename PQtype>
void print_pq(PQtype input_pq)
{
    //we dont pass by reference so no need to reconstruct local copy 
    while (input_pq.size())
    {
        printf(" [%s, %d] ", input_pq.top().first.c_str(), input_pq.top().second); 
        input_pq.pop(); 
    }
}

void print_map_to_file(std::map<std::string, int> in_map, char* filename)
{
    std::ofstream given_file; 
    given_file.open(filename); 


    given_file <<"______________________10 Most Frequent Frequencies_______________________\n"; 
    given_file<<"The Largest Word Occurences are \n"; 
    for (auto entry = largest_frequencies.rbegin(); entry != largest_frequencies.rend(); ++entry)
    {
        given_file<<"Words which occur "; 
        given_file<<*entry; 
        given_file<<" times: ["; 
        while ((min_heap.top().second <= *entry) && min_heap.size()) 
        {
            given_file<<min_heap.top().second;  
            min_heap.pop(); 
            if (min_heap.size()) given_file<<", ";
        }
        given_file<<" ] \n"; 
    }
    
    given_file <<"______________________10 Least Frequent Frequencies_______________________\n"; 
    given_file<<"The Smallest Word Occurences are \n"; 
    for (auto entry = smallest_frequencies.rbegin(); entry != smallest_frequencies.rend(); ++entry)
    {
        given_file<<"Words which occur "; 
        given_file<<*entry; 
        given_file<<" times: ["; 
        while ((max_heap.top().second >= *entry) && max_heap.size()) 
        {
            given_file<<max_heap.top().second;  
            max_heap.pop(); 
            if (max_heap.size()) given_file<<", ";
        }
        given_file<<" ] \n"; 
    }
    
    given_file <<"______________________WORD FREQUENCY LIST_______________________\n"; 
    given_file << "(word : freq)\n"; 
    for (auto entry : in_map)
    {
        given_file << entry.first; 
        given_file << ": "; 
        given_file << to_string(entry.second); 
        given_file << "\n"; 
    }
    given_file.close(); 

}
bool is_valid_char(char input)
{
    if ((input<= 'z') && (input >= '!')) return true; 
    return false; 
}

static void prep_word(string& cword)
{
    for (auto letter = cword.cbegin(); letter != cword.cend();)
    {
        if (::ispunct(*letter)) letter = cword.erase(letter); 
        else ++letter; 
    }
    transform(cword.begin(), cword.end(), cword.begin(), ::tolower); 
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
/*
void print_map_to_file(std::map<std::string, int> map, char * filename)
{
    std::ofstream file;
    file.open(filename);
    file << "______________________WORD FREQUENCY LIST______________________\n";
    file << "(word : freq)\n";
    for (auto entry : map)
    {
        file << entry.first; 
        file << ": ";
        file << to_string(entry.second);
        file << "\n"; 
    }
    
    for (const auto& x : map)
    {
        for(auto it = x.first.begin(); it != x.first.end(); ++it) file << *it;
        file << ": ";
        std::string num = std::to_string(x.second);
        for(auto it2 = num.begin(); it2 != num.end(); ++it2) file << *it2;
        file << "\n";
    } 

    file.close();
}
*/


